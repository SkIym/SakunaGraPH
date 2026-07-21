import re
from typing import Any

import httpx
from pyparsing import ParseBaseException
from rdflib.plugins.sparql.parser import parseQuery

from src.config import settings
from src.services.common import ServiceError
from src.services.llm import generate_text_async
from src.services.sparql.policy import analyze_select_query

SPARQL_PREFIXES: tuple[tuple[str, str], ...] = (
    ("", "https://sakuna.ph/"),
    ("org", "https://sakuna.ph/org/"),
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
    ("skos", "http://www.w3.org/2004/02/skos/core#"),
    ("prov", "http://www.w3.org/ns/prov#"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
    ("qudt", "http://qudt.org/schema/qudt/"),
    ("cur", "http://qudt.org/vocab/currency/"),
)
GRAPHDB_ERROR_PREFIXES = (
    "GraphDB returned ",
    "Cannot connect to GraphDB",
    "GraphDB request timed out",
    "GraphDB request failed",
)

_PREFIX_DECLARATION_RE = re.compile(
    r"(?i)\bPREFIX\s+([A-Za-z][\w.-]*)?\s*:\s*<[^>]+>"
)
_PROLOGUE_RE = re.compile(
    r"(?is)\A\s*(?:(?:PREFIX\s+(?:[A-Za-z][\w.-]*)?\s*:\s*<[^>]+>\s*)|"
    r"(?:BASE\s*<[^>]+>\s*))*"
)
_STRING_IRI_OR_COMMENT_RE = re.compile(
    r'"""(?:\\.|(?!""").)*"""'
    r"|'''(?:\\.|(?!''').)*'''"
    r'|"(?:\\.|[^"\\])*"'
    r"|'(?:\\.|[^'\\])*'"
    r"|<(?=[A-Za-z][A-Za-z0-9+.-]*:)[^>\r\n]*>"
    r"|#[^\r\n]*",
    re.DOTALL,
)
_PREFIXED_NAME_RE = re.compile(
    r"(?<![\w?])(?P<prefix>[A-Za-z][\w.-]*)?:(?P<local>[A-Za-z0-9_][\w.-]*)"
)
_BAD_AGGREGATE_RE = re.compile(
    r"\b(?:COUNT|SUM|AVG|MIN|MAX|SAMPLE|GROUP_CONCAT)\s+(?!\()",
    re.IGNORECASE,
)
_FILTER_TRIPLE_PATTERN_RE = re.compile(
    r"\bFILTER\s*\([^{}]*?"
    r"\?[A-Za-z_][\w-]*\s+"
    r"(?P<predicate>(?:[A-Za-z][\w.-]*)?:[A-Za-z0-9_][\w.-]*)\s*"
    r"(?==|!=|<=|>=|<|>)",
    re.IGNORECASE | re.DOTALL,
)

WRITE_PATTERNS = [
    re.compile(r"\bINSERT\b", re.IGNORECASE),
    re.compile(r"\bDELETE\b", re.IGNORECASE),
    re.compile(r"\bCLEAR\b", re.IGNORECASE),
    re.compile(r"\bDROP\b", re.IGNORECASE),
    re.compile(r"\bCREATE\s+GRAPH\b", re.IGNORECASE),
    re.compile(r"\bLOAD\b", re.IGNORECASE),
    re.compile(r"\bCOPY\s+GRAPH\b", re.IGNORECASE),
    re.compile(r"\bMOVE\s+GRAPH\b", re.IGNORECASE),
]


class SparqlCorrectionError(ServiceError):
    """Raised when model-generated SPARQL remains invalid after repair attempts."""

    def __init__(self, sparql: str, reason: str, attempts: int) -> None:
        self.sparql = sparql
        self.reason = reason
        self.attempts = attempts
        super().__init__(
            502,
            f"Could not generate executable SPARQL after {attempts} attempts. "
            f"Final error: {reason}",
        )


def is_write_operation(query: str) -> bool:
    sanitized = _STRING_IRI_OR_COMMENT_RE.sub("", query)
    return any(pattern.search(sanitized) for pattern in WRITE_PATTERNS)


def is_graphdb_error(error: str) -> bool:
    return error.startswith(GRAPHDB_ERROR_PREFIXES)


def ensure_sparql_prefixes(query: str) -> str:
    """Add the project's known prefixes without duplicating model declarations."""
    stripped = query.strip()
    if not stripped:
        return stripped

    declared = {
        match.group(1) or ""
        for match in _PREFIX_DECLARATION_RE.finditer(stripped)
    }
    missing = [
        f"PREFIX {prefix}: <{iri}>" if prefix else f"PREFIX : <{iri}>"
        for prefix, iri in SPARQL_PREFIXES
        if prefix not in declared
    ]
    if not missing:
        return stripped
    return "\n".join([*missing, stripped])


def validate_sparql(query: str) -> str | None:
    """Return a useful error for common generated-query syntax defects."""
    if not query or not query.strip():
        return "A non-empty SPARQL query is required."

    body = _PROLOGUE_RE.sub("", query, count=1).lstrip()
    if not re.match(r"(?i)SELECT\b", body):
        return "Only SPARQL SELECT queries are supported."

    sanitized = _STRING_IRI_OR_COMMENT_RE.sub("", query)
    stack: list[str] = []
    closing = {"}": "{", ")": "(", "]": "["}
    for character in sanitized:
        if character in "{([":
            stack.append(character)
        elif character in closing:
            if not stack or stack.pop() != closing[character]:
                return f"Unbalanced delimiter: unexpected '{character}'."
    if stack:
        return f"Unbalanced delimiter: missing a closing delimiter for '{stack[-1]}'."

    declared = {
        match.group(1) or ""
        for match in _PREFIX_DECLARATION_RE.finditer(query)
    }
    used = {
        match.group("prefix") or ""
        for match in _PREFIXED_NAME_RE.finditer(sanitized)
    }
    undefined = sorted(prefix or ":" for prefix in used - declared)
    if undefined:
        return f"Undefined prefix declaration(s): {', '.join(undefined)}."

    bad_aggregate = _BAD_AGGREGATE_RE.search(sanitized)
    if bad_aggregate:
        aggregate = bad_aggregate.group(0).strip().split()[0].upper()
        return f"Aggregate {aggregate} must be followed by a parenthesized expression."

    filter_triple = _FILTER_TRIPLE_PATTERN_RE.search(sanitized)
    if filter_triple:
        predicate = filter_triple.group("predicate")
        return (
            f"FILTER cannot use RDF predicate {predicate} as an operator. "
            "Bind the predicate's object in a WHERE triple and filter that variable, "
            "or use VALUES for known resource IRIs."
        )

    try:
        parseQuery(query)
    except ParseBaseException as exc:
        return (
            f"SPARQL parser rejected the query at line {exc.lineno}, "
            f"column {exc.col}: {exc.msg}"
        )

    try:
        analyze_select_query(
            query,
            max_length=1_000_000,
            max_triples=10_000,
            max_optionals=10_000,
            max_unions=10_000,
            max_subqueries=10_000,
            row_limit=1_000_000,
            require_bounded=False,
        )
    except ValueError as exc:
        return str(exc)

    return None


def _extract_sparql(text: str) -> str:
    match = re.search(r"```(?:sparql)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text.strip()


async def nl_to_sparql(nl_query: str, ontology_context: str) -> str:
    prompt = (
        f"{ontology_context}\n\n"
        "Convert the following natural language question into a valid SPARQL SELECT query "
        "for the SakunaGraPH knowledge graph. "
        "Return ONLY the SPARQL query inside a ```sparql code block, no explanation.\n\n"
        f"Question: {nl_query}"
    )
    generated = await generate_text_async(prompt)
    return ensure_sparql_prefixes(_extract_sparql(generated))


async def execute_sparql(
    query: str,
    *,
    timeout_seconds: float | None = None,
    max_rows: int | None = None,
) -> dict[Any, Any] | str:
    if not query or not query.strip():
        return "A non-empty SPARQL query is required."
    if is_write_operation(query):
        return "Write operations (INSERT, DELETE, CLEAR, DROP, LOAD, etc.) are not permitted."

    query = ensure_sparql_prefixes(query)
    validation_error = validate_sparql(query)
    if validation_error:
        return f"Malformed SPARQL query: {validation_error}"

    username = settings.graphdb_read_only_username
    password = settings.graphdb_read_only_password
    if bool(username) != bool(password):
        return "GraphDB request failed: read-only credentials are incomplete."
    auth = (
        (username, password.get_secret_value())
        if username and password
        else None
    )
    timeout = timeout_seconds or settings.graphdb_query_timeout_seconds

    try:
        async with httpx.AsyncClient(timeout=timeout, auth=auth) as client:
            response = await client.post(
                settings.graphdb_endpoint,
                content=query.encode(),
                params={"timeout": str(max(1, int(timeout)))},
                headers={
                    "Content-Type": "application/sparql-query",
                    "Accept": "application/sparql-results+json",
                },
            )
        if response.status_code != 200:
            return f"GraphDB returned {response.status_code}: {response.text[:500]}"
        payload = response.json()
        if max_rows is not None and isinstance(payload, dict):
            bindings = payload.get("results", {}).get("bindings")
            if isinstance(bindings, list) and len(bindings) > max_rows:
                payload["results"]["bindings"] = bindings[:max_rows]
                payload["_truncated"] = True
        return payload
    except httpx.ConnectError:
        return "Cannot connect to GraphDB"
    except httpx.TimeoutException:
        return "GraphDB request timed out."
    except httpx.HTTPError as exc:
        return f"GraphDB request failed: {exc}"
    except Exception as exc:
        return str(exc)


async def sparql_with_correction(
    nl_query: str,
    ontology_context: str,
    max_retries: int = 2,
    stop_on_graphdb_error: bool = False,
) -> tuple[str, dict[Any, Any]]:
    sparql = await nl_to_sparql(nl_query, ontology_context)
    final_error = "Unknown SPARQL execution error."

    for attempt in range(max_retries + 1):
        result = await execute_sparql(sparql)
        if isinstance(result, dict):
            return sparql, result
        final_error = result

        if stop_on_graphdb_error and is_graphdb_error(result):
            raise SparqlCorrectionError(
                sparql=sparql,
                reason=result,
                attempts=attempt + 1,
            )

        if attempt < max_retries:
            correction_prompt = (
                f"{ontology_context}\n\n"
                f'The SPARQL query below for the question "{nl_query}" produced an error.\n\n'
                f"Query:\n```sparql\n{sparql}\n```\n\n"
                f"Error:\n{result}\n\n"
                "Fix the query and return ONLY the corrected SPARQL inside a ```sparql code block."
            )
            corrected = await generate_text_async(correction_prompt)
            sparql = ensure_sparql_prefixes(_extract_sparql(corrected))

    raise SparqlCorrectionError(
        sparql=sparql,
        reason=final_error,
        attempts=max_retries + 1,
    )

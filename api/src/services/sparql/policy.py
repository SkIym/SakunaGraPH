import re
from collections.abc import Iterator
from typing import Any

from pyparsing import ParseBaseException, ParseResults
from rdflib import Literal, Variable
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parserutils import CompValue

from src.schemas.query_validation import ParsedQuerySummary


KNOWN_PREFIXES: dict[str, str] = {
    "": "https://sakuna.ph/",
    "org": "https://sakuna.ph/org/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "prov": "http://www.w3.org/ns/prov#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "qudt": "http://qudt.org/schema/qudt/",
    "cur": "http://qudt.org/vocab/currency/",
}
_PREFIX_RE = re.compile(
    r"(?i)\bPREFIX\s+([A-Za-z][\w.-]*)?\s*:\s*<([^>]+)>"
)
_BASE_RE = re.compile(r"(?i)\bBASE\s*<")
_ALLOWED_GRAPH_PATTERNS = {
    "GroupGraphPatternSub",
    "TriplesBlock",
    "OptionalGraphPattern",
    "Filter",
    "Bind",
    "InlineData",
    "GroupOrUnionGraphPattern",
    "SubSelect",
}
_FORBIDDEN_COMPONENTS = {
    "ServiceGraphPattern",
    "GraphGraphPattern",
    "MinusGraphPattern",
    "DatasetClause",
}


def _walk(value: Any) -> Iterator[Any]:
    yield value
    if isinstance(value, CompValue):
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, (ParseResults, list, tuple)):
        for child in value:
            yield from _walk(child)
    elif isinstance(value, dict):
        for child in value.values():
            yield from _walk(child)


def _projected_columns(query_node: CompValue) -> list[str]:
    if "projection" not in query_node:
        raise ValueError("SELECT * is not permitted; projected variables must be explicit.")
    columns: list[str] = []
    for projection in query_node["projection"]:
        variable = projection.get("var") if "var" in projection else projection.get("evar")
        if isinstance(variable, Variable):
            columns.append(str(variable))
    return columns


def _top_level_limit(query_node: CompValue) -> int | None:
    if "limitoffset" not in query_node:
        return None
    clauses = query_node["limitoffset"]
    if "limit" not in clauses:
        return None
    value = clauses["limit"]
    if not isinstance(value, Literal):
        raise ValueError("LIMIT must be an integer literal.")
    return int(value)


def _validate_prefixes(query: str) -> None:
    if _BASE_RE.search(query):
        raise ValueError("BASE declarations are not permitted.")
    for match in _PREFIX_RE.finditer(query):
        prefix = match.group(1) or ""
        iri = match.group(2)
        expected = KNOWN_PREFIXES.get(prefix)
        if expected is None:
            raise ValueError(f"Prefix {prefix or ':'!r} is not allowlisted.")
        if iri != expected:
            raise ValueError(
                f"Prefix {prefix or ':'!r} must resolve to the approved namespace {expected}."
            )


def analyze_select_query(
    query: str,
    *,
    max_length: int,
    max_triples: int,
    max_optionals: int,
    max_unions: int,
    max_subqueries: int,
    row_limit: int,
    require_bounded: bool,
) -> ParsedQuerySummary:
    if len(query) > max_length:
        raise ValueError(f"Query exceeds the {max_length}-character limit.")
    _validate_prefixes(query)
    try:
        parsed = parseQuery(query)
    except ParseBaseException as exc:
        raise ValueError(
            f"SPARQL parser rejected the query at line {exc.lineno}, "
            f"column {exc.col}: {exc.msg}"
        ) from exc

    query_node = parsed[1]
    if not isinstance(query_node, CompValue) or query_node.name != "SelectQuery":
        name = getattr(query_node, "name", "unknown")
        raise ValueError(f"Only SELECT queries are permitted; received {name}.")

    components = [node for node in _walk(query_node) if isinstance(node, CompValue)]
    component_names = sorted({node.name for node in components})
    forbidden = sorted(_FORBIDDEN_COMPONENTS.intersection(component_names))
    if forbidden:
        raise ValueError(f"Disallowed SPARQL component(s): {', '.join(forbidden)}.")
    unapproved_patterns = sorted(
        {
            name
            for name in component_names
            if name.endswith("GraphPattern") and name not in _ALLOWED_GRAPH_PATTERNS
        }
    )
    if unapproved_patterns:
        raise ValueError(
            f"Graph pattern component(s) are not allowlisted: {', '.join(unapproved_patterns)}."
        )

    triple_count = 0
    for component in components:
        if component.name != "TriplesBlock" or "triples" not in component:
            continue
        for triple_group in component["triples"]:
            triple_count += max(1, len(triple_group) // 3)
    optional_count = sum(node.name == "OptionalGraphPattern" for node in components)
    union_count = sum(node.name == "GroupOrUnionGraphPattern" for node in components)
    subquery_count = sum(node.name == "SubSelect" for node in components)
    aggregate_count = sum(node.name.startswith("Aggregate_") for node in components)

    budgets = (
        (triple_count, max_triples, "triple patterns"),
        (optional_count, max_optionals, "OPTIONAL blocks"),
        (union_count, max_unions, "UNION groups"),
        (subquery_count, max_subqueries, "subqueries"),
    )
    for actual, maximum, label in budgets:
        if actual > maximum:
            raise ValueError(f"Query has {actual} {label}; the maximum is {maximum}.")

    limit = _top_level_limit(query_node)
    has_top_level_grouping = "groupby" in query_node
    if require_bounded and limit is None and (aggregate_count == 0 or has_top_level_grouping):
        raise ValueError("A bounded top-level LIMIT is required for this SELECT query.")
    if limit is not None and (limit < 1 or limit > row_limit):
        raise ValueError(f"LIMIT must be between 1 and {row_limit}.")

    return ParsedQuerySummary(
        projected_columns=_projected_columns(query_node),
        component_names=component_names,
        triple_pattern_count=triple_count,
        optional_count=optional_count,
        union_count=union_count,
        subquery_count=subquery_count,
        has_aggregate=aggregate_count > 0,
        limit=limit,
    )

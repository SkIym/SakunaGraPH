import re
from typing import Any

import httpx

from src.config import settings
from src.services.gemini import get_gemini_client, get_gemini_model

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


def is_write_operation(query: str) -> bool:
    return any(pattern.search(query) for pattern in WRITE_PATTERNS)


def _extract_sparql(text: str) -> str:
    match = re.search(r"```(?:sparql)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text.strip()


def nl_to_sparql(nl_query: str, ontology_context: str) -> str:
    prompt = (
        f"{ontology_context}\n\n"
        "Convert the following natural language question into a valid SPARQL SELECT query "
        "for the SakunaGraPH knowledge graph. "
        "Return ONLY the SPARQL query inside a ```sparql code block, no explanation.\n\n"
        f"Question: {nl_query}"
    )
    response = get_gemini_client().models.generate_content(
        model=get_gemini_model(),
        contents=prompt,
    )
    return _extract_sparql(response.text)


async def execute_sparql(query: str) -> dict[Any, Any] | str:
    if not query or not query.strip():
        return "A non-empty SPARQL query is required."
    if is_write_operation(query):
        return "Write operations (INSERT, DELETE, CLEAR, DROP, LOAD, etc.) are not permitted."

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                settings.graphdb_endpoint,
                content=query.encode(),
                headers={
                    "Content-Type": "application/sparql-query",
                    "Accept": "application/sparql-results+json",
                },
            )
        if response.status_code != 200:
            return f"GraphDB returned {response.status_code}: {response.text[:500]}"
        return response.json()
    except httpx.ConnectError:
        return "Cannot connect to GraphDB"
    except Exception as exc:
        return str(exc)


async def sparql_with_correction(
    nl_query: str,
    ontology_context: str,
    max_retries: int = 2,
) -> tuple[str, dict[Any, Any]]:
    sparql = nl_to_sparql(nl_query, ontology_context)

    for attempt in range(max_retries + 1):
        result = await execute_sparql(sparql)
        if isinstance(result, dict):
            return sparql, result

        if attempt < max_retries:
            correction_prompt = (
                f"{ontology_context}\n\n"
                f'The SPARQL query below for the question "{nl_query}" produced an error.\n\n'
                f"Query:\n```sparql\n{sparql}\n```\n\n"
                f"Error:\n{result}\n\n"
                "Fix the query and return ONLY the corrected SPARQL inside a ```sparql code block."
            )
            response = get_gemini_client().models.generate_content(
                model=get_gemini_model(),
                contents=correction_prompt,
            )
            sparql = _extract_sparql(response.text)

    return sparql, {}

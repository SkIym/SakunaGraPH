import asyncio
import json
import re
from typing import Any

import google as genai
import httpx

from src.config import settings
from src.services.ontology_context import load_ontology_context

genai.configure(api_key=settings.gemini_api_key)
_model = genai.GenerativeModel(settings.gemini_model)


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
    response = _model.generate_content(prompt)
    return _extract_sparql(response.text)


async def execute_sparql(query: str) -> dict[Any, Any] | str:
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
        return f"Cannot connect to GraphDB"
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
            response = _model.generate_content(correction_prompt)
            sparql = _extract_sparql(response.text)

    return sparql, {}


if __name__ == "__main__":
    async def _test() -> None:
        ctx = load_ontology_context()
        sparql, bindings = await sparql_with_correction(
            "how many people were affected by floods in 2023", ctx
        )
        print("=== Generated SPARQL ===")
        print(sparql)
        print("\n=== Bindings ===")
        print(json.dumps(bindings, indent=2))

    asyncio.run(_test())

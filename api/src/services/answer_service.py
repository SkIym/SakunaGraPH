import json
from typing import Any
from google import genai
from src.config import settings

genai.configure(api_key=settings.gemini_api_key) # type: ignore
_model = genai.GenerativeModel(settings.gemini_model) # type: ignore


def build_grounding_prompt(nl_query: str, sparql_results: dict[Any, Any]) -> str:
    bindings = sparql_results.get("results", {}).get("bindings", [])
    vars_ = sparql_results.get("head", {}).get("vars", [])

    if not bindings:
        results_text = "No results were returned by the query."
    else:
        rows = [
            {v: b[v]["value"] for v in vars_ if v in b}
            for b in bindings[:50]
        ]
        results_text = json.dumps(rows, indent=2)

    return (
        "You are a disaster data analyst for the Philippines. "
        "Answer the user's question using only the SPARQL query results below. "
        "Be concise and factual. If the results are empty, say no data was found.\n\n"
        f"Question: {nl_query}\n\n"
        f"SPARQL Results:\n{results_text}\n\n"
        "Answer:"
    )


async def ground_answer(nl_query: str, sparql_results: dict[Any, Any]) -> str:
    prompt = build_grounding_prompt(nl_query, sparql_results)
    response = await _model.generate_content_async(prompt)
    return response.text

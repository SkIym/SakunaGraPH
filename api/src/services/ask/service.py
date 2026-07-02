import json
from collections.abc import AsyncIterator
from typing import Any

from src.services.ask.answer import build_grounding_prompt, ground_answer
from src.services.ask.context import load_ontology_context
from src.services.llm import stream_text_async
from src.services.sparql import sparql_with_correction
from src.services.sparql.executor import nl_to_sparql

_ontology_context = load_ontology_context()


async def preview_question(query: str) -> dict[str, str]:
    sparql = nl_to_sparql(query, _ontology_context)
    return {"sparql": sparql}


async def ask_question(query: str) -> dict[str, Any]:
    sparql, raw_results = await sparql_with_correction(query, _ontology_context)
    bindings = raw_results.get("results", {}).get("bindings", [])
    answer = await ground_answer(query, raw_results)
    return {"sparql": sparql, "answer": answer, "bindings": bindings}


async def stream_answer_events(query: str) -> AsyncIterator[str]:
    sparql, raw_results = await sparql_with_correction(query, _ontology_context)
    bindings = raw_results.get("results", {}).get("bindings", [])

    yield f"data: {json.dumps({'type': 'meta', 'sparql': sparql, 'bindings': bindings})}\n\n"

    prompt = build_grounding_prompt(query, raw_results)
    async for text in stream_text_async(prompt):
        yield f"data: {json.dumps({'type': 'token', 'text': text})}\n\n"

    yield f"data: {json.dumps({'type': 'done'})}\n\n"

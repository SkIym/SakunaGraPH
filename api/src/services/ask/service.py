import json
from collections.abc import AsyncIterator

from src.schemas.ask import AskPreviewResponse, AskResponse
from src.services.ask.answer import build_grounding_prompt, ground_answer
from src.services.ask.context import load_ontology_context
from src.services.llm import stream_text_async
from src.services.sparql import sparql_with_correction
from src.services.sparql.executor import nl_to_sparql

_ontology_context = load_ontology_context()


def _display_value(term: dict) -> str:
    value = str(term.get("value", ""))
    if term.get("type") == "uri":
        return value.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
    return value


def _result_rows(raw_results: dict) -> list[dict[str, str]]:
    bindings = raw_results.get("results", {}).get("bindings", [])
    vars_ = raw_results.get("head", {}).get("vars", [])
    if not vars_ and bindings:
        vars_ = list(bindings[0].keys())

    rows: list[dict[str, str]] = []
    for binding in bindings:
        rows.append({
            var: _display_value(binding[var])
            for var in vars_
            if var in binding
        })
    return rows


async def preview_question(query: str) -> AskPreviewResponse:
    sparql = nl_to_sparql(query, _ontology_context)
    return AskPreviewResponse(sparql=sparql)


async def ask_question(query: str) -> AskResponse:
    sparql, raw_results = await sparql_with_correction(query, _ontology_context)
    answer = await ground_answer(query, raw_results)
    return AskResponse(sparql=sparql, answer=answer, rows=_result_rows(raw_results))


async def stream_answer_events(query: str) -> AsyncIterator[str]:
    sparql, raw_results = await sparql_with_correction(query, _ontology_context)
    rows = _result_rows(raw_results)

    yield f"data: {json.dumps({'type': 'meta', 'sparql': sparql, 'rows': rows})}\n\n"

    prompt = build_grounding_prompt(query, raw_results)
    async for text in stream_text_async(prompt):
        yield f"data: {json.dumps({'type': 'token', 'text': text})}\n\n"

    yield f"data: {json.dumps({'type': 'done'})}\n\n"

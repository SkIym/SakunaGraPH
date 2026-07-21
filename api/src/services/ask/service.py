import json
from collections.abc import AsyncIterator

from src.schemas.ask import AskPreviewResponse, AskResponse, AskStatus
from src.schemas.entity_resolution import ResolvedAskPlan
from src.services.ask.answer import build_grounding_prompt, ground_answer
from src.services.ask.context import load_ontology_context
from src.services.ask.entity_resolver import resolve_ask_plan
from src.services.ask.planner import plan_question
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


def _result_status(raw_results: dict) -> AskStatus:
    if "boolean" in raw_results:
        return AskStatus.ANSWERED if raw_results["boolean"] else AskStatus.NO_DATA
    bindings = raw_results.get("results", {}).get("bindings", [])
    return AskStatus.ANSWERED if bindings else AskStatus.NO_DATA


def _ambiguity_answer(resolved_plan: ResolvedAskPlan) -> str:
    choices = []
    for ambiguity in resolved_plan.ambiguities:
        labels = ", ".join(
            f"{candidate.label} ({candidate.id})"
            for candidate in ambiguity.candidates
        )
        choices.append(f"{ambiguity.mention}: {labels}")
    return "Please clarify which graph entity you mean: " + "; ".join(choices) + "."


async def preview_question(query: str) -> AskPreviewResponse:
    # Phase 2 validation gate: unrestricted query generation is never reached
    # unless the model first produces a valid, query-language-free AskPlan.
    plan = await plan_question(query)
    resolved_plan = await resolve_ask_plan(query, plan)
    if resolved_plan.ambiguities:
        return AskPreviewResponse(
            status=AskStatus.NEEDS_DISAMBIGUATION,
            sparql="",
            interpretation=resolved_plan,
            warnings=resolved_plan.warnings,
            ambiguities=resolved_plan.ambiguities,
        )
    sparql = await nl_to_sparql(query, _ontology_context)
    return AskPreviewResponse(
        sparql=sparql,
        interpretation=resolved_plan,
        warnings=resolved_plan.warnings,
        ambiguities=resolved_plan.ambiguities,
    )


async def ask_question(query: str) -> AskResponse:
    plan = await plan_question(query)
    resolved_plan = await resolve_ask_plan(query, plan)
    if resolved_plan.ambiguities:
        return AskResponse(
            status=AskStatus.NEEDS_DISAMBIGUATION,
            sparql="",
            answer=_ambiguity_answer(resolved_plan),
            rows=[],
            interpretation=resolved_plan,
            warnings=resolved_plan.warnings,
            ambiguities=resolved_plan.ambiguities,
        )
    sparql, raw_results = await sparql_with_correction(query, _ontology_context)
    answer = await ground_answer(query, raw_results)
    return AskResponse(
        status=_result_status(raw_results),
        sparql=sparql,
        answer=answer,
        rows=_result_rows(raw_results),
        interpretation=resolved_plan,
        warnings=resolved_plan.warnings,
        ambiguities=resolved_plan.ambiguities,
    )


async def stream_answer_events(query: str) -> AsyncIterator[str]:
    plan = await plan_question(query)
    resolved_plan = await resolve_ask_plan(query, plan)
    interpretation = resolved_plan.model_dump(mode="json")
    ambiguities = [
        ambiguity.model_dump(mode="json")
        for ambiguity in resolved_plan.ambiguities
    ]
    if resolved_plan.ambiguities:
        status = AskStatus.NEEDS_DISAMBIGUATION
        meta = {
            "type": "meta",
            "status": status,
            "sparql": "",
            "rows": [],
            "interpretation": interpretation,
            "warnings": resolved_plan.warnings,
            "ambiguities": ambiguities,
        }
        yield f"data: {json.dumps(meta)}\n\n"
        yield f"data: {json.dumps({'type': 'token', 'text': _ambiguity_answer(resolved_plan)})}\n\n"
        yield f"data: {json.dumps({'type': 'done', 'status': status})}\n\n"
        return

    sparql, raw_results = await sparql_with_correction(query, _ontology_context)
    rows = _result_rows(raw_results)
    status = _result_status(raw_results)

    meta = {
        "type": "meta",
        "status": status,
        "sparql": sparql,
        "rows": rows,
        "interpretation": interpretation,
        "warnings": resolved_plan.warnings,
        "ambiguities": ambiguities,
    }
    yield f"data: {json.dumps(meta)}\n\n"

    prompt = build_grounding_prompt(query, raw_results)
    async for text in stream_text_async(prompt):
        yield f"data: {json.dumps({'type': 'token', 'text': text})}\n\n"

    yield f"data: {json.dumps({'type': 'done', 'status': status})}\n\n"

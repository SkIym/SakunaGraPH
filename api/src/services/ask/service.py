import json
from collections.abc import AsyncIterator

from src.config import settings
from src.schemas.ask import AskPreviewResponse, AskResponse, AskStatus
from src.schemas.ask_execution import QueryArtifact
from src.schemas.entity_resolution import ResolvedAskPlan
from src.schemas.query_validation import ResultValidationReport
from src.services.ask.answer import (
    build_answer_context,
    build_grounding_prompt,
    deterministic_answer,
    evidence_citation_suffix,
    ground_answer,
)
from src.services.ask.context import load_ontology_context
from src.services.ask.entity_resolver import resolve_ask_plan
from src.services.ask.planner import plan_question
from src.services.ask.query_compiler import compile_query
from src.services.ask.query_validator import validate_query_artifact
from src.services.ask.result_validator import validate_query_results
from src.services.ask.service_router import (
    execute_service_route,
    select_service_route,
    service_query_artifact,
)
from src.services.common import ServiceError
from src.services.llm import stream_text_async
from src.services.sparql import execute_sparql
from src.services.sparql.executor import nl_to_sparql

_ontology_context = load_ontology_context()
ASK_DETERMINISTIC_EXECUTION_ERROR_CODE = "ask_deterministic_execution"


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


def _raw_results_from_rows(
    rows: list[dict[str, str]],
    expected_columns: list[str],
    *,
    truncated: bool,
    truncation_limit: int | None = None,
) -> dict:
    columns = list(expected_columns)
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    result = {
        "head": {"vars": columns},
        "results": {
            "bindings": [
                {
                    key: {"type": "literal", "value": value}
                    for key, value in row.items()
                }
                for row in rows
            ]
        },
    }
    if truncated:
        result["_truncated"] = True
        result["_truncation_limit"] = truncation_limit or len(rows)
        result["_truncation_reason"] = "requested result limit"
    return result


def _model_fallback_artifact(
    sparql: str,
    resolved_plan: ResolvedAskPlan,
) -> QueryArtifact:
    return QueryArtifact(
        sparql=sparql,
        origin="model_fallback",
        expected_entities=[
            entity.iri
            for entities in (
                resolved_plan.locations,
                resolved_plan.disaster_types,
                resolved_plan.events,
                resolved_plan.organizations,
                resolved_plan.casualty_types,
            )
            for entity in entities
        ],
        expected_metric=resolved_plan.plan.metric,
        expected_group_by=resolved_plan.plan.group_by,
        warnings=list(resolved_plan.warnings),
    )


def _deterministic_artifact(resolved_plan: ResolvedAskPlan) -> QueryArtifact:
    route = select_service_route(resolved_plan)
    if route:
        return service_query_artifact(resolved_plan, route)
    return compile_query(resolved_plan)


async def _preview_artifact(
    query: str,
    resolved_plan: ResolvedAskPlan,
) -> QueryArtifact:
    if resolved_plan.plan.intent == "open_graph_query":
        sparql = await nl_to_sparql(query, _ontology_context)
        artifact = _model_fallback_artifact(sparql, resolved_plan)
    else:
        artifact = _deterministic_artifact(resolved_plan)
    report = await validate_query_artifact(artifact, resolved_plan)
    if not artifact.projected_columns:
        artifact = artifact.model_copy(
            update={
                "projected_columns": report.summary.projected_columns,
                "expected_columns": report.summary.projected_columns,
            }
        )
    return artifact


async def _execute_artifact(
    query: str,
    resolved_plan: ResolvedAskPlan,
) -> tuple[QueryArtifact, dict, ResultValidationReport]:
    artifact = await _preview_artifact(query, resolved_plan)
    if artifact.service_route:
        try:
            service_result = await execute_service_route(resolved_plan, artifact)
        except ServiceError as exc:
            if exc.status_code < 500:
                raise
            raise ServiceError(
                exc.status_code,
                exc.detail,
                code=ASK_DETERMINISTIC_EXECUTION_ERROR_CODE,
            ) from exc
        raw_results = _raw_results_from_rows(
            service_result.rows,
            artifact.expected_columns,
            truncated=service_result.truncated,
            truncation_limit=resolved_plan.plan.limit,
        )
    else:
        raw_results = await execute_sparql(
            artifact.sparql,
            timeout_seconds=settings.graphdb_query_timeout_seconds,
            max_rows=settings.ask_result_row_limit,
        )
        if isinstance(raw_results, str):
            raise ServiceError(
                502,
                f"Validated Ask query failed: {raw_results}",
                code=ASK_DETERMINISTIC_EXECUTION_ERROR_CODE,
            )
    result_report = validate_query_results(raw_results, artifact, resolved_plan)
    return artifact, raw_results, result_report


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
    artifact = await _preview_artifact(query, resolved_plan)
    return AskPreviewResponse(
        sparql=artifact.sparql,
        interpretation=resolved_plan,
        warnings=resolved_plan.warnings,
        ambiguities=resolved_plan.ambiguities,
        query_artifact=artifact,
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
    artifact, raw_results, result_report = await _execute_artifact(query, resolved_plan)
    answer_context = build_answer_context(
        query,
        raw_results,
        resolved_plan,
        artifact,
        result_report,
    )
    answer = deterministic_answer(answer_context)
    if answer is None:
        answer = await ground_answer(answer_context)
    return AskResponse(
        status=_result_status(raw_results),
        sparql=artifact.sparql,
        answer=answer,
        rows=_result_rows(raw_results),
        interpretation=resolved_plan,
        warnings=answer_context.warnings,
        ambiguities=resolved_plan.ambiguities,
        query_artifact=artifact,
        truncated=result_report.truncated,
        approximate=answer_context.approximate,
        evidence=answer_context.evidence,
        answer_context=answer_context,
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
            "truncated": False,
            "approximate": False,
            "evidence": [],
        }
        yield f"data: {json.dumps(meta)}\n\n"
        yield f"data: {json.dumps({'type': 'token', 'text': _ambiguity_answer(resolved_plan)})}\n\n"
        done = {
            "type": "done",
            "status": status,
            "evidence_count": 0,
            "truncated": False,
            "approximate": False,
        }
        yield f"data: {json.dumps(done)}\n\n"
        return

    artifact, raw_results, result_report = await _execute_artifact(query, resolved_plan)
    rows = _result_rows(raw_results)
    status = _result_status(raw_results)
    answer_context = build_answer_context(
        query,
        raw_results,
        resolved_plan,
        artifact,
        result_report,
    )

    meta = {
        "type": "meta",
        "status": status,
        "sparql": artifact.sparql,
        "rows": rows,
        "interpretation": interpretation,
        "warnings": answer_context.warnings,
        "ambiguities": ambiguities,
        "query_artifact": artifact.model_dump(mode="json"),
        "truncated": result_report.truncated,
        "approximate": answer_context.approximate,
        "evidence": [item.model_dump(mode="json") for item in answer_context.evidence],
        "answer_context": answer_context.model_dump(mode="json"),
    }
    yield f"data: {json.dumps(meta)}\n\n"

    results_event = {
        "type": "results",
        "status": status,
        "rows": rows,
        "evidence": [item.model_dump(mode="json") for item in answer_context.evidence],
        "truncated": answer_context.truncated,
        "approximate": answer_context.approximate,
    }
    yield f"data: {json.dumps(results_event)}\n\n"

    for warning in answer_context.warnings:
        yield f"data: {json.dumps({'type': 'warning', 'text': warning})}\n\n"

    fixed_answer = deterministic_answer(answer_context)
    if fixed_answer is not None:
        yield f"data: {json.dumps({'type': 'token', 'text': fixed_answer})}\n\n"
    else:
        prompt = build_grounding_prompt(answer_context)
        streamed_answer = ""
        async for text in stream_text_async(prompt):
            streamed_answer += text
            yield f"data: {json.dumps({'type': 'token', 'text': text})}\n\n"
        citation_suffix = evidence_citation_suffix(answer_context, streamed_answer)
        if citation_suffix:
            yield f"data: {json.dumps({'type': 'token', 'text': citation_suffix})}\n\n"

    done = {
        "type": "done",
        "status": status,
        "evidence_count": len(answer_context.evidence),
        "truncated": answer_context.truncated,
        "approximate": answer_context.approximate,
    }
    yield f"data: {json.dumps(done)}\n\n"

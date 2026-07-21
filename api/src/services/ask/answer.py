import hashlib
import json
from decimal import Decimal, InvalidOperation
from typing import Any

from src.schemas.answer_context import (
    AskAnswerContext,
    AskAnswerRow,
    AskEvidence,
    AskProvenance,
    AskResultTerm,
)
from src.schemas.ask_execution import QueryArtifact
from src.schemas.entity_resolution import ResolvedAskPlan, ResolvedEntity
from src.schemas.query_validation import ResultValidationReport
from src.services.llm import generate_text_async


_URI_COLUMNS = {"event", "group", "source"}
_PERSON_METRICS = {"dead", "injured", "missing", "affected_persons"}
_METRIC_LABELS = {
    "events": "events",
    "dead": "deaths",
    "injured": "injured people",
    "missing": "missing people",
    "affected_persons": "affected people",
    "affected_families": "affected families",
    "damage": "damage",
}


def _display_value(value: str, term_type: str) -> str:
    if term_type == "uri":
        return value.rstrip("/").rsplit("#", 1)[-1].rsplit("/", 1)[-1]
    return value


def _unique(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = value.strip()
        if cleaned and cleaned not in seen:
            result.append(cleaned)
            seen.add(cleaned)
    return result


def _resolved_entities(resolved: ResolvedAskPlan) -> list[ResolvedEntity]:
    return [
        entity
        for entities in (
            resolved.locations,
            resolved.disaster_types,
            resolved.events,
            resolved.organizations,
            resolved.casualty_types,
        )
        for entity in entities
    ]


def _approximation_warnings(resolved: ResolvedAskPlan) -> list[str]:
    return [
        (
            f"{entity.entity_type.replace('_', ' ').title()} mention "
            f"{entity.mention!r} was approximately matched to {entity.label!r} "
            f"with confidence {entity.confidence:.0%}."
        )
        for entity in _resolved_entities(resolved)
        if entity.match_type == "fuzzy"
    ]


def _row_value(binding: dict[str, Any], column: str) -> str:
    term = binding.get(column, {})
    return str(term.get("value", "")) if isinstance(term, dict) else ""


def _quantity_unit(
    column: str,
    binding: dict[str, Any],
    resolved: ResolvedAskPlan,
) -> str | None:
    if column == "total":
        explicit = _row_value(binding, "unit")
        if explicit:
            return _display_value(explicit, "uri" if explicit.startswith("http") else "literal")
        metric = _row_value(binding, "metric") or resolved.plan.metric or ""
        if metric == "events":
            return "events"
        if metric in _PERSON_METRICS:
            return "persons"
        if metric == "affected_families":
            return "families"
        if resolved.plan.intent in {"event_count", "region_ranking"}:
            return "events"
        if resolved.plan.intent in {"disaster_ranking", "victim_trend"}:
            return "persons"
    if column in {"dead", "injured", "missing", "affectedPersons"}:
        return "persons"
    if column == "affectedFamilies":
        return "families"
    if column == "recordCount":
        return "records"
    return None


def _answer_term(
    column: str,
    term: dict[str, Any],
    binding: dict[str, Any],
    resolved: ResolvedAskPlan,
) -> AskResultTerm:
    value = str(term.get("value", ""))
    term_type = str(term.get("type", "literal"))
    if value.startswith(("http://", "https://")) and column in _URI_COLUMNS:
        term_type = "uri"
    return AskResultTerm(
        value=value,
        display=_display_value(value, term_type),
        term_type=term_type,
        datatype=term.get("datatype"),
        language=term.get("xml:lang") or term.get("lang"),
        unit=_quantity_unit(column, binding, resolved),
    )


def _json_objects(value: str) -> list[dict[str, Any]]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []
    if isinstance(parsed, dict):
        return [parsed]
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    return []


def _row_provenance(
    artifact: QueryArtifact,
    binding: dict[str, Any],
) -> AskProvenance:
    source_iris: list[str] = []
    source_labels: list[str] = []
    source_record_iris: list[str] = []
    report_links: list[str] = []

    source_value = _row_value(binding, "source")
    source_label = _row_value(binding, "sourceLabel")
    if source_value.startswith(("http://", "https://")):
        source_iris.append(source_value)
    elif source_value:
        source_labels.extend(part.strip() for part in source_value.split(","))
    if source_label:
        source_labels.append(source_label)

    for item in _json_objects(_row_value(binding, "sources")):
        if uri := item.get("uri"):
            source_record_iris.append(str(uri))
        if label := item.get("reportName") or item.get("label"):
            source_labels.append(str(label))
        if link := item.get("reportLink"):
            report_links.append(str(link))
        for organization in item.get("attributedTo", []):
            if isinstance(organization, dict):
                if uri := organization.get("uri"):
                    source_iris.append(str(uri))
                if label := organization.get("label"):
                    source_labels.append(str(label))

    for item in _json_objects(_row_value(binding, "records")):
        if uri := item.get("uri"):
            source_record_iris.append(str(uri))
        if label := item.get("label"):
            source_labels.append(str(label))

    return AskProvenance(
        query_origin=artifact.origin,
        query_hash=hashlib.sha256(artifact.sparql.encode("utf-8")).hexdigest(),
        service_route=artifact.service_route,
        source_iris=_unique(source_iris),
        source_labels=_unique(source_labels),
        source_record_iris=_unique(source_record_iris),
        report_links=_unique(report_links),
    )


def _evidence_kind(binding: dict[str, Any]) -> str:
    if _row_value(binding, "event"):
        return "event"
    if _row_value(binding, "source"):
        return "source"
    if _row_value(binding, "total"):
        return "aggregate"
    return "result"


def _evidence_label(binding: dict[str, Any], index: int) -> str:
    for column in ("eventName", "sourceLabel", "groupLabel", "metric"):
        if value := _row_value(binding, column):
            return value
    for column in ("event", "source", "group"):
        if value := _row_value(binding, column):
            return _display_value(value, "uri" if value.startswith("http") else "literal")
    return f"Validated query result row {index + 1}"


def _evidence_uri(binding: dict[str, Any]) -> str | None:
    for column in ("event", "source", "group"):
        value = _row_value(binding, column)
        if value.startswith(("http://", "https://")):
            return value
    return None


def _empty_evidence(artifact: QueryArtifact) -> AskEvidence:
    return AskEvidence(
        id="E1",
        kind="result_set",
        label="Validated query returned zero rows",
        values={"rowCount": "0"},
        unit="rows",
        provenance=_row_provenance(artifact, {}),
    )


def build_answer_context(
    question: str,
    raw_results: dict[str, Any],
    resolved: ResolvedAskPlan,
    artifact: QueryArtifact,
    result_report: ResultValidationReport,
) -> AskAnswerContext:
    columns = [str(value) for value in raw_results.get("head", {}).get("vars", [])]
    bindings = raw_results.get("results", {}).get("bindings", [])
    rows: list[AskAnswerRow] = []
    evidence: list[AskEvidence] = []

    for index, binding in enumerate(bindings):
        evidence_id = f"E{index + 1}"
        values = {
            column: _answer_term(column, binding[column], binding, resolved)
            for column in columns
            if column in binding and isinstance(binding[column], dict)
        }
        rows.append(
            AskAnswerRow(index=index, values=values, evidence_ids=[evidence_id])
        )
        total_term = values.get("total")
        evidence.append(
            AskEvidence(
                id=evidence_id,
                kind=_evidence_kind(binding),
                label=_evidence_label(binding, index),
                row_index=index,
                uri=_evidence_uri(binding),
                values={column: term.value for column, term in values.items()},
                unit=total_term.unit if total_term else None,
                provenance=_row_provenance(artifact, binding),
            )
        )

    if not evidence:
        evidence.append(_empty_evidence(artifact))

    approximation_warnings = _approximation_warnings(resolved)
    warnings = _unique(
        [*resolved.warnings, *result_report.warnings, *approximation_warnings]
    )
    return AskAnswerContext(
        question=question,
        interpretation=resolved,
        query=artifact,
        columns=columns,
        rows=rows,
        row_count=result_report.row_count,
        truncated=result_report.truncated,
        approximate=bool(approximation_warnings),
        warnings=warnings,
        evidence=evidence,
    )


def _format_number(value: str) -> str:
    try:
        number = Decimal(value)
    except (InvalidOperation, ValueError):
        return value
    if not number.is_finite():
        return value
    if number == number.to_integral_value():
        return f"{int(number):,}"
    return f"{number.normalize():,f}"


def _truncation_sentence(context: AskAnswerContext) -> str:
    if not context.truncated:
        return ""
    return f" Only the first {context.row_count} results are shown."


def _deterministic_summary(context: AskAnswerContext) -> str:
    parts: list[str] = []
    for row, evidence in zip(context.rows, context.evidence, strict=True):
        total = row.values.get("total")
        if total is None:
            continue
        metric = row.values.get("metric")
        metric_name = metric.value if metric else context.interpretation.plan.metric
        label = _METRIC_LABELS.get(metric_name or "", metric_name or "total")
        unit = total.unit or "units"
        parts.append(f"{label}: {_format_number(total.value)} {unit} [{evidence.id}]")
    if not parts:
        return ""
    return "Validated totals — " + "; ".join(parts) + "."


def deterministic_answer(context: AskAnswerContext) -> str | None:
    plan = context.interpretation.plan
    if context.row_count == 0:
        return "No matching data was found in the validated query results [E1]."

    if plan.intent == "event_count" and plan.group_by is None:
        total = context.rows[0].values.get("total")
        if total:
            unit = total.unit or "events"
            return f"The validated count is {_format_number(total.value)} {unit} [E1]."

    if plan.intent == "impact_summary" and plan.group_by is None:
        summary = _deterministic_summary(context)
        return summary + _truncation_sentence(context) if summary else None

    if plan.intent in {"list_events", "source_lookup"}:
        names = []
        for row, evidence in zip(context.rows, context.evidence, strict=True):
            columns = (
                ("eventName", "event")
                if plan.intent == "list_events"
                else ("sourceLabel", "source")
            )
            term = next(
                (
                    row.values.get(column)
                    for column in columns
                    if row.values.get(column)
                ),
                None,
            )
            if term:
                names.append(f"{term.display} [{evidence.id}]")
        if names:
            noun = "matching events" if plan.intent == "list_events" else "sources"
            return (
                f"Found {len(names)} {noun}: "
                + "; ".join(names)
                + "."
                + _truncation_sentence(context)
            )
    return None


def _structured_payload(context: AskAnswerContext) -> dict[str, Any]:
    return {
        "question": context.question,
        "interpretation": context.interpretation.model_dump(mode="json"),
        "query": {
            "status": context.query_status,
            "origin": context.query.origin,
            "service_route": context.query.service_route,
        },
        "results": [row.model_dump(mode="json") for row in context.rows],
        "row_count": context.row_count,
        "truncated": context.truncated,
        "approximate": context.approximate,
        "warnings": context.warnings,
        "evidence": [item.model_dump(mode="json") for item in context.evidence],
    }


def _legacy_payload(nl_query: str, sparql_results: dict[Any, Any]) -> dict[str, Any]:
    bindings = sparql_results.get("results", {}).get("bindings", [])
    columns = sparql_results.get("head", {}).get("vars", [])
    rows = [
        {
            column: binding[column].get("value", "")
            for column in columns
            if column in binding and isinstance(binding[column], dict)
        }
        for binding in bindings[:50]
    ]
    return {
        "question": nl_query,
        "query": {"status": "validated"},
        "results": rows,
        "row_count": len(bindings),
        "truncated": len(bindings) > 50,
    }


def build_grounding_prompt(
    context: AskAnswerContext | str,
    sparql_results: dict[Any, Any] | None = None,
) -> str:
    if isinstance(context, AskAnswerContext):
        payload = _structured_payload(context)
    else:
        payload = _legacy_payload(context, sparql_results or {})
    return (
        "You are a disaster data analyst for the Philippines. Answer using only "
        "the validated structured context below. Cite every factual statement "
        "with its evidence ID, such as [E1]. Preserve the supplied units and never "
        "combine values with different units. State truncation and approximate-match "
        "warnings. Do not invent facts, identifiers, provenance, or evidence. If the "
        "validated result set is empty, say that no matching data was found.\n\n"
        f"Structured answer context:\n{json.dumps(payload, ensure_ascii=False)}\n\n"
        "Answer:"
    )


def evidence_citation_suffix(context: AskAnswerContext, answer: str) -> str:
    missing = [item.id for item in context.evidence if f"[{item.id}]" not in answer]
    if not missing:
        return ""
    return "\n\nEvidence: " + ", ".join(f"[{item}]" for item in missing) + "."


async def ground_answer(
    context: AskAnswerContext | str,
    sparql_results: dict[Any, Any] | None = None,
) -> str:
    prompt = build_grounding_prompt(context, sparql_results)
    answer = await generate_text_async(prompt)
    if isinstance(context, AskAnswerContext):
        answer += evidence_citation_suffix(context, answer)
    return answer

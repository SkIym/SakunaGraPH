from decimal import Decimal, InvalidOperation
from typing import Any

from src.config import settings
from src.schemas.ask_execution import QueryArtifact
from src.schemas.entity_resolution import ResolvedAskPlan
from src.schemas.query_validation import ResultValidationReport
from src.services.common import ServiceError


ASK_RESULT_VALIDATION_ERROR_CODE = "ask_result_validation"


def _result_error(detail: str) -> ServiceError:
    return ServiceError(502, detail, code=ASK_RESULT_VALIDATION_ERROR_CODE)


def _numeric_value(binding: dict[str, Any], column: str) -> None:
    if column not in binding:
        return
    term = binding[column]
    if not isinstance(term, dict) or "value" not in term:
        raise _result_error(f"Result column {column!r} is not an RDF term.")
    try:
        value = Decimal(str(term["value"]))
    except (InvalidOperation, ValueError):
        raise _result_error(f"Result column {column!r} must be numeric.") from None
    if not value.is_finite():
        raise _result_error(f"Result column {column!r} must be finite.")


def validate_query_results(
    raw_results: dict[str, Any],
    artifact: QueryArtifact,
    resolved: ResolvedAskPlan,
) -> ResultValidationReport:
    if "boolean" in raw_results:
        raise _result_error("Ask execution expected SELECT results, not a boolean result.")
    results = raw_results.get("results")
    head = raw_results.get("head")
    if not isinstance(results, dict) or not isinstance(head, dict):
        raise _result_error("GraphDB response is missing SELECT head/results objects.")
    bindings = results.get("bindings")
    columns = head.get("vars")
    if not isinstance(bindings, list) or not isinstance(columns, list):
        raise _result_error("GraphDB SELECT response has an invalid result shape.")
    if any(not isinstance(column, str) for column in columns):
        raise _result_error("GraphDB projection variables must be strings.")

    missing = [column for column in artifact.expected_columns if column not in columns]
    if missing:
        raise _result_error(f"GraphDB response is missing expected columns: {missing}.")
    if len(bindings) > settings.ask_result_row_limit:
        raise _result_error(
            f"GraphDB returned more than {settings.ask_result_row_limit} allowed rows."
        )

    intent = resolved.plan.intent
    numeric_columns: set[str] = set()
    if intent in {"event_count", "impact_summary", "region_ranking", "disaster_ranking"}:
        numeric_columns.add("total")
    if intent == "victim_trend":
        numeric_columns.update(("total", "dead", "injured", "missing"))
    for binding in bindings:
        if not isinstance(binding, dict):
            raise _result_error("GraphDB bindings must be objects.")
        for column in numeric_columns:
            _numeric_value(binding, column)

    if (
        bindings
        and resolved.plan.group_by
        and intent
        in {
            "event_count",
            "impact_summary",
            "victim_trend",
            "region_ranking",
            "disaster_ranking",
        }
        and not ({"group", "year"}.intersection(columns))
    ):
        raise _result_error(
            f"Grouped result is missing {resolved.plan.group_by!r} grouping values."
        )

    truncated = bool(raw_results.get("_truncated", False))
    warnings = []
    if truncated:
        warnings.append(
            f"Results were truncated to {settings.ask_result_row_limit} rows by the API."
        )
    return ResultValidationReport(
        row_count=len(bindings),
        truncated=truncated,
        warnings=warnings,
    )

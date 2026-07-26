import json
import re
from datetime import date
from typing import Any

from pydantic import ValidationError

from src.schemas.ask_plan import AskPlan
from src.services.ask.normalization import normalize_plan_payload
from src.services.ask.prompts import build_planner_prompt, build_planner_repair_prompt
from src.services.common import ServiceError
from src.services.llm import generate_text_async


PLANNER_MAX_OUTPUT_LENGTH = 12_000
PLANNER_ATTEMPTS = 2
PLANNER_VALIDATION_ERROR_CODE = "ask_plan_validation"
_JSON_FENCE_RE = re.compile(r"\A```(?:json)?\s*(.*?)\s*```\Z", re.DOTALL | re.IGNORECASE)


def _planner_validation_error(reason: str) -> ServiceError:
    return ServiceError(
        502,
        f"Could not produce a valid structured Ask plan after {PLANNER_ATTEMPTS} "
        f"attempts. Final validation error: {reason}",
        code=PLANNER_VALIDATION_ERROR_CODE,
    )


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Non-standard JSON constant {value!r} is not allowed.")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON field {key!r} is not allowed.")
        result[key] = value
    return result


def _json_document(text: str) -> str:
    stripped = text.strip()
    fence = _JSON_FENCE_RE.fullmatch(stripped)
    if fence:
        stripped = fence.group(1).strip()
    if not stripped:
        raise ValueError("Planner output was empty.")
    if len(stripped) > PLANNER_MAX_OUTPUT_LENGTH:
        raise ValueError(
            f"Planner output exceeded {PLANNER_MAX_OUTPUT_LENGTH} characters."
        )
    return stripped


def parse_plan_output(
    question: str,
    output: str,
    *,
    today: date | None = None,
) -> AskPlan:
    payload = json.loads(
        _json_document(output),
        parse_constant=_reject_json_constant,
        object_pairs_hook=_unique_object,
    )
    if not isinstance(payload, dict):
        raise ValueError("Planner output must be one JSON object.")
    normalized = normalize_plan_payload(
        question,
        payload,
        today=today or date.today(),
    )
    return AskPlan.model_validate(normalized)


def _validation_reason(exc: ValueError) -> str:
    if isinstance(exc, ValidationError):
        messages = [
            f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
            for error in exc.errors(include_url=False, include_input=False)
        ]
        return "; ".join(messages)
    if isinstance(exc, json.JSONDecodeError):
        return (
            f"Invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        )
    return str(exc) or exc.__class__.__name__


async def plan_question(question: str, *, today: date | None = None) -> AskPlan:
    """Create an AskPlan with exactly one bounded repair attempt."""
    planning_date = today or date.today()
    initial_output = await generate_text_async(
        build_planner_prompt(question, today=planning_date)
    )
    try:
        return parse_plan_output(question, initial_output, today=planning_date)
    except (ValidationError, ValueError) as initial_error:
        repaired_output = await generate_text_async(
            build_planner_repair_prompt(
                question,
                initial_output,
                _validation_reason(initial_error),
                today=planning_date,
            )
        )

    try:
        return parse_plan_output(question, repaired_output, today=planning_date)
    except (ValidationError, ValueError) as repair_error:
        raise _planner_validation_error(_validation_reason(repair_error)) from repair_error

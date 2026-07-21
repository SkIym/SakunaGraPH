#!/usr/bin/env python3
"""Evaluate the current Ask pipeline against the golden question fixtures.

Run from the API directory:

    .venv/bin/python scripts/evaluate_ask.py

The runner is intentionally sequential by default so it does not overwhelm the
local model server or GraphDB. It does not mutate GraphDB. Reports are written
under ``api/evaluation/reports`` unless ``--output`` is supplied.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from src.config import settings  # noqa: E402
from src.services.ask.answer import ground_answer  # noqa: E402
from src.services.ask.context import load_ontology_context  # noqa: E402
from src.services.common import ServiceError  # noqa: E402
from src.services.llm import active_model_info  # noqa: E402
from src.services.sparql import execute_sparql, sparql_with_correction  # noqa: E402

DEFAULT_FIXTURES = API_ROOT / "tests" / "fixtures" / "ask_golden_questions.json"
DEFAULT_REPORT_DIR = API_ROOT / "evaluation" / "reports"
NO_DATA_PATTERNS = (
    "no data",
    "no results",
    "nothing was found",
    "did not return any",
    "there are no",
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _duration_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 2)


def _error_text(exc: Exception) -> str:
    if isinstance(exc, ServiceError):
        return exc.detail
    return str(exc)


def load_fixture_document(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Fixture file does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Fixture file is not valid JSON: {exc}") from exc

    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("Fixture document must contain a non-empty 'cases' array")

    required_fields = {
        "id",
        "category",
        "question",
        "expected_plan",
        "expected_entities",
        "required_query_terms",
        "forbidden_query_terms",
        "expected_result_shape",
        "tags",
    }
    seen_ids: set[str] = set()
    for index, case in enumerate(cases):
        if not isinstance(case, dict):
            raise ValueError(f"Fixture case at index {index} must be an object")
        missing = sorted(required_fields - case.keys())
        if missing:
            raise ValueError(
                f"Fixture case at index {index} is missing: {', '.join(missing)}"
            )
        case_id = case["id"]
        if not isinstance(case_id, str) or not case_id:
            raise ValueError(f"Fixture case at index {index} has an invalid id")
        if case_id in seen_ids:
            raise ValueError(f"Duplicate fixture case id: {case_id}")
        seen_ids.add(case_id)
        if not isinstance(case["question"], str) or not case["question"].strip():
            raise ValueError(f"Fixture case '{case_id}' has an empty question")
        for field in ("required_query_terms", "forbidden_query_terms", "tags"):
            if not isinstance(case[field], list) or not all(
                isinstance(value, str) for value in case[field]
            ):
                raise ValueError(f"Fixture case '{case_id}' has an invalid {field}")
    return payload


def select_cases(
    cases: list[dict[str, Any]],
    *,
    case_ids: set[str] | None = None,
    categories: set[str] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    selected = [
        case
        for case in cases
        if (not case_ids or case["id"] in case_ids)
        and (not categories or case["category"] in categories)
    ]
    if case_ids:
        found = {case["id"] for case in selected}
        missing = sorted(case_ids - found)
        if missing:
            raise ValueError(f"Unknown case id(s): {', '.join(missing)}")
    return selected[:limit] if limit is not None else selected


def score_query(case: dict[str, Any], sparql: str) -> dict[str, Any]:
    required = case.get("required_query_terms", [])
    forbidden = case.get("forbidden_query_terms", [])
    missing_required = [term for term in required if not _query_contains(sparql, term)]
    present_forbidden = [term for term in forbidden if _query_contains(sparql, term)]
    return {
        "required_term_count": len(required),
        "matched_required_term_count": len(required) - len(missing_required),
        "missing_required_terms": missing_required,
        "forbidden_term_count": len(forbidden),
        "present_forbidden_terms": present_forbidden,
        "required_terms_passed": not missing_required,
        "forbidden_terms_passed": not present_forbidden,
        "semantic_terms_passed": not missing_required and not present_forbidden,
    }


def _query_contains(sparql: str, term: str) -> bool:
    """Match expected terms without treating one prefixed name as another.

    Plain phrases remain substring checks so fixtures can assert fragments such
    as ``GROUP BY``. Prefixed local names use a trailing identifier boundary,
    preventing ``:PHP`` from matching ``:PHP_millions``.
    """
    if re.fullmatch(r"(?:[A-Za-z][\w-]*)?:[A-Za-z0-9_~-]+", term):
        return re.search(
            re.escape(term) + r"(?![A-Za-z0-9_~-])",
            sparql,
            flags=re.IGNORECASE,
        ) is not None
    return term.casefold() in sparql.casefold()


def result_row_count(results: dict[str, Any]) -> int:
    bindings = results.get("results", {}).get("bindings", [])
    return len(bindings) if isinstance(bindings, list) else 0


def result_is_empty(results: dict[str, Any]) -> bool:
    if "boolean" in results:
        return not bool(results["boolean"])
    return result_row_count(results) == 0


def answer_claims_no_data(answer: str | None) -> bool:
    normalized = (answer or "").casefold()
    return any(pattern in normalized for pattern in NO_DATA_PATTERNS)


def infer_actual_status(execution_success: bool) -> str:
    return "answered" if execution_success else "execution_failed"


async def evaluate_case(
    case: dict[str, Any],
    *,
    ontology_context: str,
    generate_answer: bool,
) -> dict[str, Any]:
    started = time.perf_counter()
    sparql = ""
    raw_results: dict[str, Any] = {}
    generation_error: str | None = None
    execution_error: str | None = None
    answer: str | None = None
    answer_error: str | None = None
    generation_ms = 0.0
    verification_ms = 0.0
    answer_ms = 0.0

    generation_started = time.perf_counter()
    try:
        sparql, raw_results = await sparql_with_correction(
            case["question"], ontology_context
        )
    except Exception as exc:  # Keep the baseline run alive across service failures.
        generation_error = _error_text(exc)
    generation_ms = _duration_ms(generation_started)

    # The current production helper returns an empty dict after exhausting its
    # repair attempts. Re-run only that final query to recover the masked
    # GraphDB error and distinguish it from a successful empty result set.
    execution_success = bool(raw_results)
    if sparql and not raw_results and generation_error is None:
        verification_started = time.perf_counter()
        verification = await execute_sparql(sparql)
        verification_ms = _duration_ms(verification_started)
        if isinstance(verification, dict):
            raw_results = verification
            execution_success = True
        else:
            execution_error = verification
    elif generation_error is not None:
        execution_error = "Query generation did not complete."

    if generate_answer and generation_error is None:
        answer_started = time.perf_counter()
        try:
            # This deliberately mirrors current behavior, including grounding
            # on {} after a masked query failure, so false no-data answers are
            # visible in the baseline.
            answer = await ground_answer(case["question"], raw_results)
        except Exception as exc:
            answer_error = _error_text(exc)
        answer_ms = _duration_ms(answer_started)

    query_score = score_query(case, sparql)
    empty_result = execution_success and result_is_empty(raw_results)
    expected_status = case.get("expected_status", "answered")
    actual_status = infer_actual_status(execution_success)
    no_data_claim = answer_claims_no_data(answer)
    masked_failure_no_data = not execution_success and no_data_claim
    unexpected_empty_candidate = (
        empty_result
        and expected_status == "answered"
        and case["expected_result_shape"] != "valid_empty_event_list"
    )

    return {
        "id": case["id"],
        "category": case["category"],
        "source": case.get("source"),
        "question": case["question"],
        "tags": case["tags"],
        "expectations": {
            "plan": case["expected_plan"],
            "entities": case["expected_entities"],
            "result_shape": case["expected_result_shape"],
            "status": expected_status,
        },
        "generated_sparql": sparql,
        "query_origin": "current_text_to_sparql",
        "query_score": query_score,
        "generation": {
            "succeeded": bool(sparql) and generation_error is None,
            "error": generation_error,
            "duration_ms": generation_ms,
            "attempt_count": None,
        },
        "execution": {
            "succeeded": execution_success,
            "error": execution_error,
            "row_count": result_row_count(raw_results) if execution_success else 0,
            "empty": empty_result,
            "verification_duration_ms": verification_ms,
        },
        "answer": {
            "requested": generate_answer,
            "succeeded": answer is not None and answer_error is None,
            "text": answer,
            "error": answer_error,
            "claims_no_data": no_data_claim,
            "duration_ms": answer_ms,
        },
        "actual_status": actual_status,
        "status_matches_expected": actual_status == expected_status,
        "masked_failure_no_data": masked_failure_no_data,
        "unexpected_empty_candidate": unexpected_empty_candidate,
        "total_duration_ms": _duration_ms(started),
    }


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return round(ordered[lower], 2)
    fraction = rank - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 2)


def summarize_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(results)
    generated = sum(item["generation"]["succeeded"] for item in results)
    executed = sum(item["execution"]["succeeded"] for item in results)
    semantic_passes = sum(
        item["query_score"]["semantic_terms_passed"] for item in results
    )
    answer_requested = sum(item["answer"]["requested"] for item in results)
    answers_generated = sum(item["answer"]["succeeded"] for item in results)
    expected_unsafe = [
        item for item in results if item["expectations"]["status"] == "unsupported"
    ]
    unsafe_rejected = sum(item["actual_status"] != "answered" for item in expected_unsafe)
    durations = [float(item["total_duration_ms"]) for item in results]

    category_totals: Counter[str] = Counter(item["category"] for item in results)
    category_passes: defaultdict[str, int] = defaultdict(int)
    category_executed: defaultdict[str, int] = defaultdict(int)
    for item in results:
        category_passes[item["category"]] += int(
            item["query_score"]["semantic_terms_passed"]
        )
        category_executed[item["category"]] += int(item["execution"]["succeeded"])

    by_category = {
        category: {
            "total": count,
            "executable": category_executed[category],
            "semantic_term_passes": category_passes[category],
            "executable_rate": round(category_executed[category] / count, 4),
            "semantic_term_pass_rate": round(category_passes[category] / count, 4),
        }
        for category, count in sorted(category_totals.items())
    }

    def rate(value: int, denominator: int = total) -> float:
        return round(value / denominator, 4) if denominator else 0.0

    return {
        "total_cases": total,
        "generated_queries": generated,
        "query_generation_rate": rate(generated),
        "executable_queries": executed,
        "executable_query_rate": rate(executed),
        "semantic_term_passes": semantic_passes,
        "semantic_term_pass_rate": rate(semantic_passes),
        "status_matches": sum(item["status_matches_expected"] for item in results),
        "status_match_rate": rate(
            sum(item["status_matches_expected"] for item in results)
        ),
        "expected_unsafe_or_unsupported_cases": len(expected_unsafe),
        "unsafe_or_unsupported_cases_rejected": unsafe_rejected,
        "unsafe_rejection_rate": rate(unsafe_rejected, len(expected_unsafe)),
        "answers_requested": answer_requested,
        "answers_generated": answers_generated,
        "answer_generation_rate": rate(answers_generated, answer_requested),
        "valid_empty_results": sum(
            item["execution"]["empty"]
            and item["expectations"]["result_shape"] == "valid_empty_event_list"
            for item in results
        ),
        "unexpected_empty_candidates": sum(
            item["unexpected_empty_candidate"] for item in results
        ),
        "masked_failure_no_data_answers": sum(
            item["masked_failure_no_data"] for item in results
        ),
        "latency_ms": {
            "mean": round(statistics.fmean(durations), 2) if durations else 0.0,
            "median": round(statistics.median(durations), 2) if durations else 0.0,
            "p95": _percentile(durations, 0.95),
            "maximum": round(max(durations), 2) if durations else 0.0,
        },
        "by_category": by_category,
    }


def build_report(
    *,
    fixture_path: Path,
    fixture_document: dict[str, Any],
    selected_cases: list[dict[str, Any]],
    results: list[dict[str, Any]],
    started_at: str,
    finished_at: str,
    generate_answer: bool,
) -> dict[str, Any]:
    fixture_bytes = fixture_path.read_bytes()
    return {
        "report_version": "1.0.0",
        "pipeline": "current_text_to_sparql_baseline",
        "started_at": started_at,
        "finished_at": finished_at,
        "model": active_model_info(),
        "graphdb": {"endpoint": settings.graphdb_endpoint},
        "fixture": {
            "path": str(fixture_path),
            "fixture_version": fixture_document.get("fixture_version"),
            "sha256": hashlib.sha256(fixture_bytes).hexdigest(),
            "available_cases": len(fixture_document["cases"]),
            "selected_cases": len(selected_cases),
        },
        "configuration": {
            "generate_answers": generate_answer,
            "execution_order": "sequential",
            "semantic_scoring": "case-insensitive required/forbidden terms with prefixed-name boundaries",
            "notes": [
                "Planner-field accuracy is not scored because the current pipeline does not emit AskPlan.",
                "Unexpected empty results are candidates for review, not automatically classified as false negatives.",
                "A final query is re-executed only when the current correction helper masks failure as an empty dict.",
            ],
        },
        "summary": summarize_results(results),
        "cases": results,
    }


def default_output_path() -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_REPORT_DIR / f"baseline-{timestamp}.json"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate the current Ask pipeline against golden questions."
    )
    parser.add_argument(
        "--fixtures",
        type=Path,
        default=DEFAULT_FIXTURES,
        help=f"Golden fixture JSON (default: {DEFAULT_FIXTURES})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Report path (default: api/evaluation/reports/baseline-<timestamp>.json)",
    )
    parser.add_argument(
        "--case",
        action="append",
        dest="case_ids",
        help="Run one case id; repeat to select multiple cases.",
    )
    parser.add_argument(
        "--category",
        action="append",
        dest="categories",
        help="Run one category; repeat to select multiple categories.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Run only the first N selected cases.",
    )
    parser.add_argument(
        "--no-answer",
        action="store_true",
        help="Skip answer generation and evaluate query generation/execution only.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and list selected fixtures without calling the model or GraphDB.",
    )
    return parser.parse_args(argv)


async def async_main(args: argparse.Namespace) -> int:
    if args.limit is not None and args.limit < 1:
        raise ValueError("--limit must be at least 1")

    fixture_path = args.fixtures.expanduser().resolve()
    fixture_document = load_fixture_document(fixture_path)
    selected = select_cases(
        fixture_document["cases"],
        case_ids=set(args.case_ids or []),
        categories=set(args.categories or []),
        limit=args.limit,
    )
    if not selected:
        raise ValueError("No fixture cases matched the selection")

    if args.dry_run:
        print(
            json.dumps(
                {
                    "fixture": str(fixture_path),
                    "available_cases": len(fixture_document["cases"]),
                    "selected_cases": len(selected),
                    "cases": [
                        {
                            "id": case["id"],
                            "category": case["category"],
                            "question": case["question"],
                        }
                        for case in selected
                    ],
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        return 0

    started_at = _now_iso()
    ontology_context = load_ontology_context()
    results: list[dict[str, Any]] = []
    for index, case in enumerate(selected, start=1):
        print(
            f"[{index}/{len(selected)}] {case['id']}",
            file=sys.stderr,
            flush=True,
        )
        results.append(
            await evaluate_case(
                case,
                ontology_context=ontology_context,
                generate_answer=not args.no_answer,
            )
        )

    finished_at = _now_iso()
    output_path = (args.output or default_output_path()).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = build_report(
        fixture_path=fixture_path,
        fixture_document=fixture_document,
        selected_cases=selected,
        results=results,
        started_at=started_at,
        finished_at=finished_at,
        generate_answer=not args.no_answer,
    )
    output_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report["summary"], indent=2, ensure_ascii=False))
    print(f"Report written to {output_path}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return asyncio.run(async_main(parse_args(argv)))
    except (ValueError, ServiceError) as exc:
        print(f"error: {_error_text(exc)}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Evaluation interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

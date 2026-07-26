#!/usr/bin/env python3
"""Measure structured Ask planner field accuracy on the golden fixture set.

Run from the API directory:

    .venv/bin/python scripts/evaluate_ask_planner.py

The command calls only the configured local language model. It does not issue
SPARQL or access GraphDB.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from scripts.evaluate_ask import load_fixture_document, select_cases  # noqa: E402
from src.services.ask.planner import plan_question  # noqa: E402
from src.services.common import ServiceError  # noqa: E402
from src.services.llm import active_model_info  # noqa: E402

DEFAULT_FIXTURES = API_ROOT / "tests" / "fixtures" / "ask_golden_questions.json"
DEFAULT_REPORT_DIR = API_ROOT / "evaluation" / "reports"
DEFAULT_FIELD_ACCURACY_THRESHOLD = 0.80


def score_plan(expected: dict[str, Any], actual: dict[str, Any]) -> dict[str, Any]:
    fields = sorted(expected)
    matches = {
        field: field in actual and actual[field] == expected[field]
        for field in fields
    }
    matched = sum(matches.values())
    return {
        "field_count": len(fields),
        "matched_field_count": matched,
        "field_matches": matches,
        "mismatches": {
            field: {"expected": expected[field], "actual": actual.get(field)}
            for field in fields
            if not matches[field]
        },
        "exact_match": bool(fields) and matched == len(fields),
    }


def summarize_plan_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    total_cases = len(results)
    valid_cases = sum(result["valid"] for result in results)
    total_fields = sum(result["score"]["field_count"] for result in results)
    matched_fields = sum(
        result["score"]["matched_field_count"] for result in results
    )
    exact_cases = sum(result["score"]["exact_match"] for result in results)
    return {
        "total_cases": total_cases,
        "valid_plan_cases": valid_cases,
        "valid_plan_rate": round(valid_cases / total_cases, 4) if total_cases else 0.0,
        "matched_fields": matched_fields,
        "total_expected_fields": total_fields,
        "field_accuracy": round(matched_fields / total_fields, 4) if total_fields else 0.0,
        "exact_plan_cases": exact_cases,
        "exact_plan_rate": round(exact_cases / total_cases, 4) if total_cases else 0.0,
    }


async def evaluate_plan_case(case: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    actual: dict[str, Any] = {}
    error: str | None = None
    try:
        plan = await plan_question(case["question"])
        actual = plan.model_dump(mode="json")
    except ServiceError as exc:
        error = exc.detail

    score = score_plan(case["expected_plan"], actual)
    return {
        "id": case["id"],
        "category": case["category"],
        "question": case["question"],
        "expected_plan": case["expected_plan"],
        "actual_plan": actual or None,
        "valid": error is None and bool(actual),
        "error": error,
        "score": score,
        "duration_ms": round((time.perf_counter() - started) * 1000, 2),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURES)
    parser.add_argument("--case", action="append", dest="case_ids")
    parser.add_argument("--category", action="append", dest="categories")
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_FIELD_ACCURACY_THRESHOLD,
        help="Required field accuracy (default: 0.80)",
    )
    parser.add_argument("--output", type=Path)
    return parser


async def _run(args: argparse.Namespace) -> int:
    if not 0 <= args.threshold <= 1:
        raise ValueError("--threshold must be between 0 and 1")
    fixture = load_fixture_document(args.fixtures)
    cases = select_cases(
        fixture["cases"],
        case_ids=set(args.case_ids or []),
        categories=set(args.categories or []),
        limit=args.limit,
    )
    if not cases:
        raise ValueError("No fixture cases matched the selected filters")

    # Sequential execution keeps local-model load and result ordering stable.
    results = [await evaluate_plan_case(case) for case in cases]
    summary = summarize_plan_results(results)
    summary["required_field_accuracy"] = args.threshold
    summary["threshold_passed"] = summary["field_accuracy"] >= args.threshold

    generated_at = datetime.now(UTC)
    output = args.output or (
        DEFAULT_REPORT_DIR / f"planner-{generated_at.strftime('%Y%m%dT%H%M%SZ')}.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "report_version": "1.0.0",
        "generated_at": generated_at.isoformat(),
        "fixtures": str(args.fixtures),
        "model": active_model_info(),
        "summary": summary,
        "results": results,
    }
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Report: {output}")
    return 0 if summary["threshold_passed"] else 1


def main() -> int:
    args = _parser().parse_args()
    try:
        return asyncio.run(_run(args))
    except (ValueError, ServiceError) as exc:
        detail = exc.detail if isinstance(exc, ServiceError) else str(exc)
        print(f"ERROR: {detail}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

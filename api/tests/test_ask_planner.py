import json
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

from pydantic import ValidationError

from scripts.evaluate_ask_planner import score_plan, summarize_plan_results
from src.schemas.ask_plan import AskPlan
from src.services.ask.normalization import (
    normalize_date_range,
    normalize_metric,
    normalize_plan_payload,
)
from src.services.ask.planner import (
    PLANNER_VALIDATION_ERROR_CODE,
    parse_plan_output,
    plan_question,
)
from src.services.ask.prompts import build_planner_prompt
from src.services.ask.service import ask_question
from src.services.common import ServiceError


def _plan_json(**overrides: object) -> str:
    payload: dict[str, object] = {
        "intent": "list_events",
        "event_type": "all",
        "location_mentions": [],
        "disaster_type_mentions": [],
        "event_mentions": [],
        "organization_mentions": [],
        "start_date": None,
        "end_date": None,
        "metric": None,
        "group_by": None,
        "sort_direction": "desc",
        "limit": 25,
    }
    payload.update(overrides)
    return json.dumps(payload)


class AskPlanSchemaTests(unittest.TestCase):
    def test_defaults_and_normalizes_entity_mentions(self) -> None:
        plan = AskPlan(
            intent="list_events",
            location_mentions=[" Cebu ", "cebu", "Cebu Province"],
        )

        self.assertEqual(plan.event_type, "all")
        self.assertEqual(plan.limit, 25)
        self.assertEqual(plan.location_mentions, ["Cebu", "Cebu Province"])

    def test_rejects_extra_query_field_and_reversed_dates(self) -> None:
        with self.assertRaises(ValidationError):
            AskPlan.model_validate({"intent": "list_events", "sparql": "SELECT * {}"})
        with self.assertRaises(ValidationError):
            AskPlan(
                intent="list_events",
                start_date="2024-03-31",
                end_date="2024-01-01",
            )

    def test_schema_has_no_sparql_output(self) -> None:
        self.assertNotIn("sparql", AskPlan.model_json_schema()["properties"])


class PlannerNormalizationTests(unittest.TestCase):
    def test_golden_date_and_metric_expectations_survive_normalization(self) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "ask_golden_questions.json"
        cases = json.loads(fixture_path.read_text(encoding="utf-8"))["cases"]

        for case in cases:
            expected = case["expected_plan"]
            normalized = normalize_plan_payload(
                case["question"],
                expected,
                today=date(2026, 7, 21),
            )
            with self.subTest(case=case["id"]):
                for field in ("start_date", "end_date"):
                    if field in expected:
                        actual = normalized[field]
                        self.assertEqual(
                            actual.isoformat() if actual else None,
                            expected[field],
                        )
                if "metric" in expected:
                    self.assertEqual(normalized["metric"], expected["metric"])

    def test_normalizes_calendar_year_from_question(self) -> None:
        start, end = normalize_date_range(
            "How many events were recorded in 2022?",
            None,
            None,
            today=date(2026, 7, 21),
        )

        self.assertEqual(start, date(2022, 1, 1))
        self.assertEqual(end, date(2022, 12, 31))

    def test_normalizes_named_month_range(self) -> None:
        start, end = normalize_date_range(
            "List events from January through March 2024.",
            None,
            None,
            today=date(2026, 7, 21),
        )

        self.assertEqual(start, date(2024, 1, 1))
        self.assertEqual(end, date(2024, 3, 31))

    def test_relative_dates_use_injected_clock(self) -> None:
        start, end = normalize_date_range(
            "Show events from last year.",
            None,
            None,
            today=date(2026, 7, 21),
        )

        self.assertEqual((start, end), (date(2025, 1, 1), date(2025, 12, 31)))

    def test_population_reference_is_not_mistaken_for_event_date(self) -> None:
        start, end = normalize_date_range(
            "List locations and their 2024 population.",
            None,
            None,
            today=date(2026, 7, 21),
        )

        self.assertEqual((start, end), (None, None))

    def test_normalizes_metric_synonyms_and_event_counts(self) -> None:
        self.assertEqual(
            normalize_metric("How many fatalities were reported?", None, intent="impact_summary"),
            "dead",
        )
        self.assertEqual(
            normalize_metric("How many disasters occurred?", None, intent="event_count"),
            "events",
        )
        self.assertEqual(
            normalize_metric(
                "What displaced-family and displaced-person totals were recorded?",
                None,
                intent="impact_summary",
            ),
            "affected_persons",
        )


class PlannerParsingTests(unittest.TestCase):
    def test_parses_fenced_json_and_applies_deterministic_normalization(self) -> None:
        output = "```json\n" + _plan_json(
            intent="impact_summary",
            metric="fatalities",
            start_date="2022",
            end_date="2022",
        ) + "\n```"

        plan = parse_plan_output(
            "How many fatalities were recorded during 2022?",
            output,
            today=date(2026, 7, 21),
        )

        self.assertEqual(plan.metric, "dead")
        self.assertEqual(plan.start_date, date(2022, 1, 1))
        self.assertEqual(plan.end_date, date(2022, 12, 31))

    def test_rejects_duplicate_fields_and_non_object_json(self) -> None:
        for output in ('{"intent":"list_events","intent":"event_count"}', "[]"):
            with self.subTest(output=output):
                with self.assertRaises(ValueError):
                    parse_plan_output("List events", output)


class PlannerRepairTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_exactly_one_repair_attempt(self) -> None:
        valid = _plan_json(intent="event_count", metric="event count")
        with patch(
            "src.services.ask.planner.generate_text_async",
            new=AsyncMock(side_effect=["not json", valid]),
        ) as model:
            plan = await plan_question(
                "How many events occurred?",
                today=date(2026, 7, 21),
            )

        self.assertEqual(plan.intent, "event_count")
        self.assertEqual(plan.metric, "events")
        self.assertEqual(model.await_count, 2)

    async def test_stops_after_invalid_initial_and_repair_outputs(self) -> None:
        with patch(
            "src.services.ask.planner.generate_text_async",
            new=AsyncMock(side_effect=["not json", '{"intent":"not_supported"}']),
        ) as model:
            with self.assertRaises(ServiceError) as raised:
                await plan_question("Do something", today=date(2026, 7, 21))

        self.assertEqual(raised.exception.code, PLANNER_VALIDATION_ERROR_CODE)
        self.assertEqual(model.await_count, 2)

    async def test_invalid_plan_blocks_query_generation_and_graphdb(self) -> None:
        error = ServiceError(
            502,
            "Could not produce a valid structured Ask plan.",
            code=PLANNER_VALIDATION_ERROR_CODE,
        )
        with (
            patch(
                "src.services.ask.service.plan_question",
                new=AsyncMock(side_effect=error),
            ),
            patch(
                "src.services.ask.service.execute_sparql",
                new=AsyncMock(),
            ) as execute,
        ):
            with self.assertRaises(ServiceError):
                await ask_question("List events")

        execute.assert_not_awaited()


class PlannerPromptTests(unittest.TestCase):
    def test_prompt_requires_json_and_forbids_sparql(self) -> None:
        prompt = build_planner_prompt("List events", today=date(2026, 7, 21))

        self.assertIn("Return exactly one JSON object", prompt)
        self.assertIn("Never output SPARQL", prompt)
        self.assertIn('"intent"', prompt)


class PlannerEvaluationTests(unittest.TestCase):
    def test_scores_only_expected_golden_fields(self) -> None:
        score = score_plan(
            {"intent": "event_count", "metric": "events", "group_by": None},
            {
                "intent": "event_count",
                "metric": "events",
                "group_by": "region",
                "limit": 25,
            },
        )

        self.assertEqual(score["matched_field_count"], 2)
        self.assertEqual(
            score["mismatches"],
            {"group_by": {"expected": None, "actual": "region"}},
        )

    def test_missing_null_field_does_not_count_as_a_match(self) -> None:
        score = score_plan({"metric": None, "group_by": None}, {})

        self.assertEqual(score["matched_field_count"], 0)
        self.assertFalse(score["exact_match"])

    def test_summarizes_field_and_exact_plan_accuracy(self) -> None:
        results = [
            {
                "valid": True,
                "score": {
                    "field_count": 3,
                    "matched_field_count": 3,
                    "exact_match": True,
                },
            },
            {
                "valid": False,
                "score": {
                    "field_count": 3,
                    "matched_field_count": 1,
                    "exact_match": False,
                },
            },
        ]

        summary = summarize_plan_results(results)

        self.assertEqual(summary["valid_plan_rate"], 0.5)
        self.assertEqual(summary["field_accuracy"], 0.6667)
        self.assertEqual(summary["exact_plan_rate"], 0.5)


if __name__ == "__main__":
    unittest.main()

import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from src.main import app
from src.schemas.ask import AskResponse, AskStatus
from src.schemas.ask_execution import QueryArtifact
from src.schemas.ask_plan import AskPlan
from src.schemas.entity_resolution import ResolvedAskPlan, ResolvedEntity
from src.schemas.query_validation import ResultValidationReport
from src.services.ask.answer import (
    build_answer_context,
    build_grounding_prompt,
    deterministic_answer,
    ground_answer,
)
from src.services.ask.service import ask_question, stream_answer_events
from src.services.common import ServiceError


def _artifact(columns: list[str], *, origin: str = "compiler") -> QueryArtifact:
    return QueryArtifact(
        sparql="SELECT ?value WHERE { ?value ?predicate ?object } LIMIT 10",
        origin=origin,
        projected_columns=columns,
        expected_columns=columns,
    )


def _raw_results(
    columns: list[str],
    rows: list[dict[str, str]],
    *,
    truncated: bool = False,
) -> dict:
    result = {
        "head": {"vars": columns},
        "results": {
            "bindings": [
                {
                    key: {
                        "type": (
                            "uri"
                            if value.startswith(("http://", "https://"))
                            else "literal"
                        ),
                        "value": value,
                    }
                    for key, value in row.items()
                }
                for row in rows
            ]
        },
    }
    if truncated:
        result.update(
            {
                "_truncated": True,
                "_truncation_limit": len(rows),
                "_truncation_reason": "requested result limit",
            }
        )
    return result


def _context(
    plan: AskPlan,
    columns: list[str],
    rows: list[dict[str, str]],
    *,
    resolved: ResolvedAskPlan | None = None,
    truncated: bool = False,
):
    resolved_plan = resolved or ResolvedAskPlan(plan=plan)
    raw = _raw_results(columns, rows, truncated=truncated)
    warnings = (
        [f"Results were truncated to {len(rows)} rows by the requested result limit."]
        if truncated
        else []
    )
    report = ResultValidationReport(
        row_count=len(rows),
        truncated=truncated,
        warnings=warnings,
    )
    return build_answer_context(
        "A grounded question",
        raw,
        resolved_plan,
        _artifact(columns),
        report,
    )


class StructuredAnswerContextTests(unittest.TestCase):
    def test_preserves_rdf_terms_and_exposes_event_source_provenance(self) -> None:
        columns = ["event", "eventName", "startDate", "source"]
        raw = {
            "head": {"vars": columns},
            "results": {
                "bindings": [
                    {
                        "event": {
                            "type": "uri",
                            "value": "https://sakuna.ph/events/ndrrmc/one",
                        },
                        "eventName": {
                            "type": "literal",
                            "xml:lang": "en",
                            "value": "Typhoon One",
                        },
                        "startDate": {
                            "type": "literal",
                            "datatype": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "value": "2024-01-02T00:00:00",
                        },
                        "source": {"type": "literal", "value": "NDRRMC"},
                    }
                ]
            },
        }
        plan = AskPlan(intent="list_events")
        context = build_answer_context(
            "List events",
            raw,
            ResolvedAskPlan(plan=plan),
            _artifact(columns, origin="service"),
            ResultValidationReport(row_count=1),
        )

        event = context.rows[0].values["event"]
        self.assertEqual(event.value, "https://sakuna.ph/events/ndrrmc/one")
        self.assertEqual(event.term_type, "uri")
        self.assertEqual(context.rows[0].values["eventName"].language, "en")
        self.assertEqual(
            context.rows[0].values["startDate"].datatype,
            "http://www.w3.org/2001/XMLSchema#dateTime",
        )
        self.assertEqual(context.evidence[0].uri, event.value)
        self.assertEqual(context.evidence[0].provenance.source_labels, ["NDRRMC"])
        self.assertEqual(len(context.evidence[0].provenance.query_hash), 64)

    def test_fuzzy_entity_matches_are_explicitly_approximate(self) -> None:
        location = ResolvedEntity(
            iri="https://sakuna.ph/0706000000",
            id="0706000000",
            label="Province of Cebu",
            entity_type="location",
            mention="Cebuu",
            match_type="fuzzy",
            confidence=0.91,
        )
        plan = AskPlan(intent="event_count", metric="events")
        resolved = ResolvedAskPlan(plan=plan, locations=[location])
        context = _context(
            plan,
            ["total"],
            [{"total": "2"}],
            resolved=resolved,
        )

        self.assertTrue(context.approximate)
        self.assertIn("approximately matched", context.warnings[0])
        self.assertIn("91%", context.warnings[0])

    def test_ranking_totals_receive_semantic_units(self) -> None:
        cases = (
            (AskPlan(intent="region_ranking"), "events"),
            (AskPlan(intent="disaster_ranking"), "persons"),
            (
                AskPlan(intent="victim_trend", metric="dead", group_by="year"),
                "persons",
            ),
        )
        for plan, expected_unit in cases:
            with self.subTest(intent=plan.intent):
                context = _context(
                    plan,
                    ["group", "groupLabel", "total"],
                    [{"group": "one", "groupLabel": "One", "total": "4"}],
                )
                self.assertEqual(context.rows[0].values["total"].unit, expected_unit)


class DeterministicGroundingGoldenTests(unittest.TestCase):
    def test_simple_answer_faithfulness_meets_phase_six_target(self) -> None:
        fixture = json.loads(
            (
                Path(__file__).parent
                / "fixtures"
                / "ask_phase6_grounded_answers.json"
            ).read_text(encoding="utf-8")
        )
        passed = 0
        for case in fixture["cases"]:
            plan = AskPlan(
                intent=case["intent"],
                metric=case["metric"],
            )
            context = _context(plan, case["columns"], case["rows"])
            answer = deterministic_answer(context)
            with self.subTest(case=case["id"]):
                self.assertEqual(answer, case["expected_answer"])
                for evidence in context.evidence:
                    self.assertIn(f"[{evidence.id}]", answer)
                for row in context.rows:
                    if total := row.values.get("total"):
                        self.assertIsNotNone(total.unit)
            passed += answer == case["expected_answer"]

        accuracy = passed / len(fixture["cases"])
        self.assertGreaterEqual(accuracy, fixture["threshold"])


class GroundedAnswerGenerationTests(unittest.IsolatedAsyncioTestCase):
    async def test_model_receives_structured_context_and_missing_citations_are_added(
        self,
    ) -> None:
        plan = AskPlan(intent="victim_trend", metric="dead", group_by="year")
        context = _context(
            plan,
            ["group", "groupLabel", "total"],
            [{"group": "2024", "groupLabel": "2024", "total": "5"}],
        )
        prompt = build_grounding_prompt(context)

        self.assertIn("Structured answer context", prompt)
        self.assertIn('"unit": "persons"', prompt)
        self.assertIn('"id": "E1"', prompt)
        self.assertNotIn(context.query.sparql, prompt)

        with patch(
            "src.services.ask.answer.generate_text_async",
            new=AsyncMock(return_value="Five deaths were recorded in 2024."),
        ):
            answer = await ground_answer(context)

        self.assertIn("Evidence: [E1].", answer)

    async def test_simple_count_bypasses_answer_model(self) -> None:
        plan = AskPlan(intent="event_count", metric="events")
        resolved = ResolvedAskPlan(plan=plan)
        artifact = _artifact(["total"], origin="service")
        raw = _raw_results(["total"], [{"total": "7"}])
        report = ResultValidationReport(row_count=1)
        with (
            patch(
                "src.services.ask.service.plan_question",
                new=AsyncMock(return_value=plan),
            ),
            patch(
                "src.services.ask.service.resolve_ask_plan",
                new=AsyncMock(return_value=resolved),
            ),
            patch(
                "src.services.ask.service._execute_artifact",
                new=AsyncMock(return_value=(artifact, raw, report)),
            ),
            patch(
                "src.services.ask.service.ground_answer",
                new=AsyncMock(),
            ) as model,
        ):
            response = await ask_question("How many events?")

        model.assert_not_awaited()
        self.assertEqual(response.answer, "The validated count is 7 events [E1].")
        self.assertEqual(response.evidence[0].values["total"], "7")
        self.assertEqual(response.answer_context.row_count, 1)


class AskStreamingPhaseSixTests(unittest.IsolatedAsyncioTestCase):
    async def test_richer_events_preserve_legacy_meta_token_done_order(self) -> None:
        plan = AskPlan(intent="event_count", metric="events")
        resolved = ResolvedAskPlan(plan=plan)
        artifact = _artifact(["total"], origin="service")
        raw = _raw_results(["total"], [{"total": "5"}], truncated=True)
        report = ResultValidationReport(
            row_count=1,
            truncated=True,
            warnings=[
                "Results were truncated to 1 rows by the requested result limit."
            ],
        )
        with (
            patch(
                "src.services.ask.service.plan_question",
                new=AsyncMock(return_value=plan),
            ),
            patch(
                "src.services.ask.service.resolve_ask_plan",
                new=AsyncMock(return_value=resolved),
            ),
            patch(
                "src.services.ask.service._execute_artifact",
                new=AsyncMock(return_value=(artifact, raw, report)),
            ),
            patch(
                "src.services.ask.service.stream_text_async",
                new=AsyncMock(),
            ) as model_stream,
        ):
            events = [event async for event in stream_answer_events("Count events")]

        payloads = [json.loads(event.removeprefix("data: ")) for event in events]
        event_types = [payload["type"] for payload in payloads]
        legacy_types = [kind for kind in event_types if kind in {"meta", "token", "done"}]
        self.assertEqual(legacy_types, ["meta", "token", "done"])
        self.assertEqual(event_types, ["meta", "results", "warning", "token", "done"])
        self.assertIn("answer_context", payloads[0])
        self.assertEqual(payloads[1]["evidence"][0]["id"], "E1")
        self.assertTrue(payloads[-1]["truncated"])
        model_stream.assert_not_awaited()


class CurrentFrontendContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_success_empty_and_ambiguous_keep_required_frontend_fields(self) -> None:
        responses = (
            AskResponse(
                status=AskStatus.ANSWERED,
                sparql="SELECT",
                answer="One result [E1].",
                rows=[{"event": "one"}],
            ),
            AskResponse(
                status=AskStatus.NO_DATA,
                sparql="SELECT",
                answer="No matching data [E1].",
                rows=[],
            ),
            AskResponse(
                status=AskStatus.NEEDS_DISAMBIGUATION,
                sparql="",
                answer="Please clarify.",
                rows=[],
            ),
        )
        for expected in responses:
            with (
                self.subTest(status=expected.status),
                patch(
                    "src.routers.ask.ask_question",
                    new=AsyncMock(return_value=expected),
                ),
            ):
                response = self.client.post("/api/ask", json={"query": "Question"})
                payload = response.json()

                self.assertEqual(response.status_code, 200)
                self.assertTrue({"answer", "sparql", "rows"}.issubset(payload))
                self.assertIsInstance(payload["answer"], str)
                self.assertIsInstance(payload["rows"], list)

    def test_failed_response_keeps_detail_contract_without_false_answer(self) -> None:
        with patch(
            "src.routers.ask.ask_question",
            new=AsyncMock(
                side_effect=ServiceError(502, "GraphDB unavailable", code="failure")
            ),
        ):
            response = self.client.post("/api/ask", json={"query": "Question"})

        payload = response.json()
        self.assertEqual(response.status_code, 502)
        self.assertEqual(payload["detail"], "GraphDB unavailable")
        self.assertNotIn("answer", payload)


if __name__ == "__main__":
    unittest.main()

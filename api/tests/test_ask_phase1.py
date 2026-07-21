import json
import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from pydantic import ValidationError

from src.main import app
from src.schemas.ask import ASK_QUERY_MAX_LENGTH, AskRequest, AskStatus
from src.schemas.ask_plan import AskPlan
from src.services.ask import PLANNER_VALIDATION_ERROR_CODE
from src.services.ask.context import load_ontology_context
from src.services.common import ServiceError
from src.services.ask.service import ask_question, preview_question
from src.services.sparql.executor import SparqlCorrectionError, nl_to_sparql


EMPTY_RESULTS = {"head": {"vars": ["event"]}, "results": {"bindings": []}}
ONE_RESULT = {
    "head": {"vars": ["event"]},
    "results": {
        "bindings": [
            {"event": {"type": "uri", "value": "https://sakuna.ph/event/one"}}
        ]
    },
}
OPEN_PLAN = AskPlan(intent="open_graph_query")


class AskRequestValidationTests(unittest.TestCase):
    def test_trims_and_accepts_query_at_length_limit(self) -> None:
        request = AskRequest(query="  List events  ")
        maximum = AskRequest(query="x" * ASK_QUERY_MAX_LENGTH)

        self.assertEqual(request.query, "List events")
        self.assertEqual(len(maximum.query), ASK_QUERY_MAX_LENGTH)

    def test_rejects_blank_and_oversized_queries(self) -> None:
        for query in ("   ", "x" * (ASK_QUERY_MAX_LENGTH + 1)):
            with self.subTest(length=len(query)):
                with self.assertRaises(ValidationError):
                    AskRequest(query=query)


class AskContextTests(unittest.TestCase):
    def test_uses_controlled_casualty_iris_instead_of_string_literals(self) -> None:
        context = load_ontology_context()

        self.assertIn(":isOfCasualtyType :Dead", context)
        self.assertIn(":Dead, :Injured, and :Missing", context)
        self.assertNotIn(':isOfCasualtyType ["dead"', context)


class AskAsyncGenerationTests(unittest.IsolatedAsyncioTestCase):
    async def test_nl_to_sparql_awaits_async_model_client(self) -> None:
        generated = "SELECT ?event WHERE { ?event a :DisasterEvent . }"
        with patch(
            "src.services.sparql.executor.generate_text_async",
            new=AsyncMock(return_value=generated),
        ) as model:
            sparql = await nl_to_sparql("List events", "context")

        model.assert_awaited_once()
        self.assertIn("SELECT ?event", sparql)

    async def test_preview_awaits_query_generation(self) -> None:
        with (
            patch(
                "src.services.ask.service.plan_question",
                new=AsyncMock(return_value=OPEN_PLAN),
            ) as planner,
            patch(
                "src.services.ask.service.nl_to_sparql",
                new=AsyncMock(return_value="SELECT * WHERE {}"),
            ) as generate,
        ):
            response = await preview_question("List events")

        planner.assert_awaited_once_with("List events")
        generate.assert_awaited_once()
        self.assertEqual(response.status, AskStatus.QUERY_READY)
        self.assertEqual(response.sparql, "SELECT * WHERE {}")


class AskFailureSemanticsTests(unittest.IsolatedAsyncioTestCase):
    async def test_successful_results_have_answered_status(self) -> None:
        with (
            patch(
                "src.services.ask.service.plan_question",
                new=AsyncMock(return_value=OPEN_PLAN),
            ),
            patch(
                "src.services.ask.service.sparql_with_correction",
                new=AsyncMock(return_value=("SELECT", ONE_RESULT)),
            ),
            patch(
                "src.services.ask.service.ground_answer",
                new=AsyncMock(return_value="One event was found."),
            ) as answer,
        ):
            response = await ask_question("List events")

        answer.assert_awaited_once()
        self.assertEqual(response.status, AskStatus.ANSWERED)
        self.assertEqual(response.answer, "One event was found.")
        self.assertEqual(response.rows, [{"event": "one"}])

    async def test_valid_empty_results_have_no_data_status(self) -> None:
        with (
            patch(
                "src.services.ask.service.plan_question",
                new=AsyncMock(return_value=OPEN_PLAN),
            ),
            patch(
                "src.services.ask.service.sparql_with_correction",
                new=AsyncMock(return_value=("SELECT", EMPTY_RESULTS)),
            ),
            patch(
                "src.services.ask.service.ground_answer",
                new=AsyncMock(return_value="No data was found."),
            ),
        ):
            response = await ask_question("List events in 2099")

        self.assertEqual(response.status, AskStatus.NO_DATA)
        self.assertEqual(response.rows, [])
        self.assertEqual(response.answer, "No data was found.")

    async def test_query_failure_never_reaches_answer_generation(self) -> None:
        correction_error = SparqlCorrectionError(
            sparql="SELECT COUNT ?event WHERE {}",
            reason="Malformed SPARQL query: bad aggregate",
            attempts=3,
        )
        with (
            patch(
                "src.services.ask.service.plan_question",
                new=AsyncMock(return_value=OPEN_PLAN),
            ),
            patch(
                "src.services.ask.service.sparql_with_correction",
                new=AsyncMock(side_effect=correction_error),
            ),
            patch(
                "src.services.ask.service.ground_answer",
                new=AsyncMock(),
            ) as answer,
        ):
            with self.assertRaises(SparqlCorrectionError):
                await ask_question("Count events")

        answer.assert_not_awaited()


class AskApiCompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_success_response_keeps_existing_fields_and_adds_status(self) -> None:
        response_payload = {
            "status": AskStatus.ANSWERED,
            "sparql": "SELECT",
            "answer": "One event was found.",
            "rows": [{"event": "one"}],
        }
        with patch(
            "src.routers.ask.ask_question",
            new=AsyncMock(return_value=response_payload),
        ):
            response = self.client.post("/api/ask", json={"query": "List events"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), response_payload)

    def test_validation_failure_preserves_detail_and_adds_status(self) -> None:
        error = SparqlCorrectionError(
            sparql="SELECT COUNT ?event WHERE {}",
            reason="Malformed SPARQL query: bad aggregate",
            attempts=3,
        )
        with patch(
            "src.routers.ask.ask_question",
            new=AsyncMock(side_effect=error),
        ):
            response = self.client.post("/api/ask", json={"query": "Count events"})

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["status"], "validation_failed")
        self.assertIn("Could not generate executable SPARQL", response.json()["detail"])

    def test_planner_validation_failure_uses_validation_status(self) -> None:
        error = ServiceError(
            502,
            "Could not produce a valid structured Ask plan.",
            code=PLANNER_VALIDATION_ERROR_CODE,
        )
        with patch(
            "src.routers.ask.ask_question",
            new=AsyncMock(side_effect=error),
        ):
            response = self.client.post("/api/ask", json={"query": "List events"})

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["status"], "validation_failed")
        self.assertIn("valid structured Ask plan", response.json()["detail"])

    def test_execution_failure_is_not_returned_as_no_data(self) -> None:
        error = SparqlCorrectionError(
            sparql="SELECT ?event WHERE {}",
            reason="Cannot connect to GraphDB",
            attempts=1,
        )
        with patch(
            "src.routers.ask.ask_question",
            new=AsyncMock(side_effect=error),
        ):
            response = self.client.post("/api/ask", json={"query": "List events"})

        payload = response.json()
        self.assertEqual(response.status_code, 502)
        self.assertEqual(payload["status"], "execution_failed")
        self.assertIn("Final error: Cannot connect to GraphDB", payload["detail"])
        self.assertNotIn("answer", payload)

    def test_stream_error_keeps_legacy_fields_and_adds_ask_status(self) -> None:
        error = SparqlCorrectionError(
            sparql="SELECT ?event WHERE {}",
            reason="Cannot connect to GraphDB",
            attempts=1,
        )
        with patch(
            "src.routers.ask.stream_answer_events",
            side_effect=error,
        ):
            response = self.client.post(
                "/api/ask/stream",
                json={"query": "List events"},
            )

        event = json.loads(response.text.removeprefix("data: ").strip())
        self.assertEqual(event["type"], "error")
        self.assertEqual(event["status"], 502)
        self.assertEqual(event["ask_status"], "execution_failed")
        self.assertIn("detail", event)


if __name__ == "__main__":
    unittest.main()

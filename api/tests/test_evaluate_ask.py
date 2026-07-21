import json
import tempfile
import unittest
from pathlib import Path

from scripts.evaluate_ask import (
    answer_claims_no_data,
    load_fixture_document,
    result_is_empty,
    result_row_count,
    score_query,
    select_cases,
    summarize_results,
)


def _case(case_id: str = "case-1", category: str = "events") -> dict:
    return {
        "id": case_id,
        "category": category,
        "source": "test",
        "question": "How many flood events occurred?",
        "expected_plan": {"intent": "event_count"},
        "expected_entities": {"disaster_types": ["Flood"]},
        "required_query_terms": ["COUNT", ":Flood"],
        "forbidden_query_terms": [":numberOfEvents"],
        "expected_result_shape": "single_count",
        "tags": ["test"],
    }


class FixtureTests(unittest.TestCase):
    def test_loads_and_validates_fixture_document(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixtures.json"
            path.write_text(json.dumps({"cases": [_case()]}), encoding="utf-8")

            payload = load_fixture_document(path)

        self.assertEqual(payload["cases"][0]["id"], "case-1")

    def test_rejects_duplicate_case_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixtures.json"
            path.write_text(
                json.dumps({"cases": [_case(), _case()]}), encoding="utf-8"
            )

            with self.assertRaisesRegex(ValueError, "Duplicate fixture case id"):
                load_fixture_document(path)

    def test_selects_cases_by_id_category_and_limit(self) -> None:
        cases = [_case("one", "events"), _case("two", "safety"), _case("three", "events")]

        selected = select_cases(cases, categories={"events"}, limit=1)

        self.assertEqual([item["id"] for item in selected], ["one"])
        with self.assertRaisesRegex(ValueError, "Unknown case id"):
            select_cases(cases, case_ids={"missing"})


class ScoringTests(unittest.TestCase):
    def test_scores_required_and_forbidden_terms_case_insensitively(self) -> None:
        passing = score_query(
            _case(),
            "SELECT (count(?event) AS ?total) WHERE { ?event :hasDisasterType :Flood }",
        )
        failing = score_query(
            _case(),
            "SELECT ?numberOfEvents WHERE { ?event :numberOfEvents ?numberOfEvents }",
        )

        self.assertTrue(passing["semantic_terms_passed"])
        self.assertEqual(failing["missing_required_terms"], ["COUNT", ":Flood"])
        self.assertEqual(failing["present_forbidden_terms"], [":numberOfEvents"])

    def test_prefixed_name_matching_respects_local_name_boundary(self) -> None:
        case = _case()
        case["required_query_terms"] = [":PHP_millions"]
        case["forbidden_query_terms"] = [":PHP"]

        score = score_query(case, "SELECT * WHERE { ?value qudt:unit :PHP_millions }")

        self.assertTrue(score["semantic_terms_passed"])

    def test_reads_select_and_boolean_result_shapes(self) -> None:
        select_result = {
            "head": {"vars": ["event"]},
            "results": {"bindings": [{"event": {"value": "one"}}]},
        }

        self.assertEqual(result_row_count(select_result), 1)
        self.assertFalse(result_is_empty(select_result))
        self.assertTrue(result_is_empty({"head": {"vars": []}, "results": {"bindings": []}}))
        self.assertTrue(result_is_empty({"boolean": False}))
        self.assertFalse(result_is_empty({"boolean": True}))

    def test_detects_common_no_data_claims(self) -> None:
        self.assertTrue(answer_claims_no_data("No data was found for that period."))
        self.assertFalse(answer_claims_no_data("Three events were found."))

    def test_summarizes_case_results(self) -> None:
        result = {
            "category": "events",
            "generation": {"succeeded": True},
            "execution": {"succeeded": True, "empty": False},
            "query_score": {"semantic_terms_passed": True},
            "answer": {"requested": True, "succeeded": True},
            "expectations": {"status": "answered", "result_shape": "single_count"},
            "actual_status": "answered",
            "status_matches_expected": True,
            "unexpected_empty_candidate": False,
            "masked_failure_no_data": False,
            "total_duration_ms": 100.0,
        }

        summary = summarize_results([result])

        self.assertEqual(summary["total_cases"], 1)
        self.assertEqual(summary["executable_query_rate"], 1.0)
        self.assertEqual(summary["semantic_term_pass_rate"], 1.0)
        self.assertEqual(summary["latency_ms"]["p95"], 100.0)


if __name__ == "__main__":
    unittest.main()

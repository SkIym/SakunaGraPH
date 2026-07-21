import unittest
from unittest.mock import AsyncMock, patch

from src.services.sparql.executor import (
    SparqlCorrectionError,
    ensure_sparql_prefixes,
    execute_sparql,
    sparql_with_correction,
    validate_sparql,
)


class SparqlNormalizationTests(unittest.TestCase):
    def test_adds_known_prefixes_to_model_query(self) -> None:
        query = "SELECT ?event WHERE { ?event a :DisasterEvent . }"

        normalized = ensure_sparql_prefixes(query)

        self.assertIn("PREFIX : <https://sakuna.ph/>", normalized)
        self.assertIn("PREFIX rdf:", normalized)
        self.assertEqual(normalized.count("PREFIX :"), 1)
        self.assertIsNone(validate_sparql(normalized))

    def test_does_not_duplicate_an_existing_prefix(self) -> None:
        query = """PREFIX : <https://example.test/>
SELECT ?event WHERE { ?event a :DisasterEvent . }"""

        normalized = ensure_sparql_prefixes(query)

        self.assertEqual(normalized.count("PREFIX :"), 1)
        self.assertIn("PREFIX : <https://example.test/>", normalized)

    def test_detects_multiple_prefix_declarations_on_one_line(self) -> None:
        query = (
            "PREFIX : <https://example.test/> "
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
            "SELECT ?event WHERE { ?event a :DisasterEvent . }"
        )

        normalized = ensure_sparql_prefixes(query)

        self.assertEqual(normalized.count("PREFIX :"), 1)
        self.assertEqual(normalized.count("PREFIX xsd:"), 1)
        self.assertIsNone(validate_sparql(normalized))

    def test_rejects_undefined_custom_prefix(self) -> None:
        query = ensure_sparql_prefixes(
            "SELECT ?event WHERE { ?event custom:property ?value . }"
        )

        self.assertEqual(
            validate_sparql(query),
            "Undefined prefix declaration(s): custom.",
        )

    def test_rejects_aggregate_without_parentheses(self) -> None:
        query = ensure_sparql_prefixes(
            "SELECT COUNT ?event WHERE { ?event a :DisasterEvent . }"
        )

        self.assertEqual(
            validate_sparql(query),
            "Aggregate COUNT must be followed by a parenthesized expression.",
        )

    def test_rejects_rdf_predicate_used_as_filter_operator(self) -> None:
        query = ensure_sparql_prefixes(
            """SELECT ?event WHERE {
  ?event :hasDisasterType ?dtype .
  FILTER (?dtype skos:prefLabel = "Flash Flood")
}"""
        )

        self.assertEqual(
            validate_sparql(query),
            "FILTER cannot use RDF predicate skos:prefLabel as an operator. "
            "Bind the predicate's object in a WHERE triple and filter that variable, "
            "or use VALUES for known resource IRIs.",
        )

    def test_accepts_values_for_known_disaster_type_iris(self) -> None:
        query = ensure_sparql_prefixes(
            """SELECT ?event WHERE {
  VALUES ?dtype { :FlashFlood :RiverineFlood }
  ?event a :DisasterEvent ; :hasDisasterType ?dtype .
}"""
        )

        self.assertIsNone(validate_sparql(query))


class SparqlExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_validation_prevents_graphdb_request(self) -> None:
        with patch("src.services.sparql.executor.httpx.AsyncClient") as client:
            result = await execute_sparql(
                "SELECT COUNT ?event WHERE { ?event a :DisasterEvent . }"
            )

        self.assertIn("Malformed SPARQL query", result)
        client.assert_not_called()

    async def test_correction_failure_preserves_query_and_error(self) -> None:
        generated = "SELECT COUNT ?event WHERE { ?event a :DisasterEvent . }"
        with (
            patch(
                "src.services.sparql.executor.generate_text_async",
                new=AsyncMock(side_effect=[generated, generated]),
            ),
            patch(
                "src.services.sparql.executor.execute_sparql",
                new=AsyncMock(return_value="Malformed SPARQL query: bad aggregate"),
            ),
        ):
            with self.assertRaises(SparqlCorrectionError) as raised:
                await sparql_with_correction("Count events", "context", max_retries=1)

        error = raised.exception
        self.assertIn("PREFIX : <https://sakuna.ph/>", error.sparql)
        self.assertEqual(error.reason, "Malformed SPARQL query: bad aggregate")
        self.assertEqual(error.attempts, 2)

    async def test_graphdb_fail_fast_does_not_wait_for_correction(self) -> None:
        generated = "SELECT ?event WHERE { ?event a :DisasterEvent . }"
        with (
            patch(
                "src.services.sparql.executor.generate_text_async",
                new=AsyncMock(return_value=generated),
            ) as generate,
            patch(
                "src.services.sparql.executor.execute_sparql",
                new=AsyncMock(return_value="GraphDB returned 400: MALFORMED QUERY"),
            ),
        ):
            with self.assertRaises(SparqlCorrectionError) as raised:
                await sparql_with_correction(
                    "List events",
                    "context",
                    stop_on_graphdb_error=True,
                )

        self.assertEqual(raised.exception.attempts, 1)
        self.assertEqual(
            raised.exception.reason,
            "GraphDB returned 400: MALFORMED QUERY",
        )
        generate.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()

import time
import unittest
from unittest.mock import AsyncMock, patch

from src.config import settings
from src.schemas.ask_execution import QueryArtifact
from src.schemas.ask_plan import AskPlan
from src.schemas.entity_resolution import ResolvedAskPlan, ResolvedEntity
from src.schemas.query_validation import SchemaCatalog, SchemaTerm
from src.services.ask.query_compiler import compile_query
from src.services.ask.query_validator import validate_query_artifact
from src.services.ask.result_validator import validate_query_results
from src.services.ask.schema_catalog import (
    clear_schema_catalog_cache,
    get_schema_catalog,
)
from src.services.ask.service import ask_question
from src.services.common import ServiceError
from src.services.sparql.policy import analyze_select_query


def _term(
    local: str,
    kind: str,
    *,
    domains: list[str] | None = None,
    ranges: list[str] | None = None,
    parents: list[str] | None = None,
    types: list[str] | None = None,
) -> SchemaTerm:
    return SchemaTerm(
        iri=f"https://sakuna.ph/{local}",
        local_name=local,
        label=local,
        kind=kind,
        domains=domains or [],
        ranges=ranges or [],
        parents=parents or [],
        types=types or [],
    )


def _catalog() -> SchemaCatalog:
    disaster_event = "https://sakuna.ph/DisasterEvent"
    casualties = "https://sakuna.ph/Casualties"
    affected = "https://sakuna.ph/AffectedPopulation"
    terms = [
        _term("DisasterEvent", "class"),
        _term("MajorEvent", "class", parents=[disaster_event]),
        _term("Incident", "class", parents=[disaster_event]),
        _term("Region", "class"),
        _term("Casualties", "class"),
        _term("AffectedPopulation", "class"),
        _term("startDate", "datatype_property", domains=[disaster_event]),
        _term("endDate", "datatype_property", domains=[disaster_event]),
        _term("eventName", "datatype_property", domains=[disaster_event]),
        _term("incidentDescription", "datatype_property", domains=[disaster_event]),
        _term("hasLocation", "object_property", domains=[disaster_event]),
        _term("isPartOf", "object_property"),
        _term("hasDisasterType", "object_property", domains=[disaster_event]),
        _term("hasDisasterSubtype", "object_property", domains=[disaster_event]),
        _term("hasCasualties", "object_property", domains=[disaster_event]),
        _term("isOfCasualtyType", "object_property", domains=[casualties]),
        _term("casualtyCount", "datatype_property", domains=[casualties]),
        _term("hasAffectedPopulation", "object_property", domains=[disaster_event]),
        _term("affectedPersons", "datatype_property", domains=[affected]),
        _term("affectedFamilies", "datatype_property", domains=[affected]),
    ]
    for local in (
        "hasAgricultureDamage",
        "hasDamageGeneral",
        "hasHousingDamage",
        "hasInfrastructureDamage",
        "agriDamageAmount",
        "commercialDamageAmount",
        "crossSectoralDamageAmount",
        "generalDamageAmount",
        "housingDamageAmount",
        "infraDamageAmount",
        "socialDamageAmount",
    ):
        terms.append(_term(local, "object_property"))
    for local in ("Dead", "Injured", "Missing"):
        terms.append(
            _term(
                local,
                "individual",
                types=["https://sakuna.ph/CasualtyType"],
            )
        )
    return SchemaCatalog(
        terms={term.iri: term for term in terms},
        loaded_at_monotonic=time.monotonic(),
    )


def _resolved_entity(entity_type: str, identifier: str) -> ResolvedEntity:
    return ResolvedEntity(
        iri=f"https://sakuna.ph/{identifier}",
        id=identifier,
        label=identifier,
        entity_type=entity_type,
        mention=identifier,
        match_type="exact",
        confidence=1.0,
    )


def _policy(query: str):
    return analyze_select_query(
        query,
        max_length=settings.ask_sparql_max_length,
        max_triples=settings.ask_sparql_max_triples,
        max_optionals=settings.ask_sparql_max_optionals,
        max_unions=settings.ask_sparql_max_unions,
        max_subqueries=settings.ask_sparql_max_subqueries,
        row_limit=settings.ask_result_row_limit,
        require_bounded=True,
    )


class ParsedQueryPolicyTests(unittest.TestCase):
    def test_accepts_bounded_select_and_unbounded_scalar_aggregate(self) -> None:
        listing = "SELECT ?event WHERE { ?event a <https://sakuna.ph/DisasterEvent> } LIMIT 10"
        aggregate = (
            "SELECT (COUNT(?event) AS ?total) "
            "WHERE { ?event a <https://sakuna.ph/DisasterEvent> }"
        )

        self.assertEqual(_policy(listing).limit, 10)
        self.assertTrue(_policy(aggregate).has_aggregate)

    def test_rejects_non_select_federation_and_unbounded_listing(self) -> None:
        cases = (
            "ASK { ?s ?p ?o }",
            "SELECT ?s WHERE { SERVICE <https://evil.example/sparql> { ?s ?p ?o } } LIMIT 10",
            "SELECT ?s WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 10",
            "SELECT * WHERE { ?s ?p ?o } LIMIT 10",
            "SELECT ?s WHERE { ?s ?p ?o }",
            "SELECT ?s WHERE { ?s ?p ?o } LIMIT 101",
        )

        for query in cases:
            with self.subTest(query=query):
                with self.assertRaises(ValueError):
                    _policy(query)

    def test_rejects_redefined_or_unknown_prefixes(self) -> None:
        for declaration in (
            "PREFIX : <https://evil.example/>",
            "PREFIX custom: <https://sakuna.ph/>",
        ):
            query = f"{declaration}\nSELECT ?s WHERE {{ ?s a :DisasterEvent }} LIMIT 5"
            with self.subTest(declaration=declaration):
                with self.assertRaises(ValueError):
                    _policy(query)

    def test_enforces_complexity_budget(self) -> None:
        triples = "\n".join(f"?s <https://sakuna.ph/p{i}> ?o{i} ." for i in range(81))
        query = f"SELECT ?s WHERE {{ {triples} }} LIMIT 5"

        with self.assertRaisesRegex(ValueError, "triple patterns"):
            _policy(query)


class SchemaAndPlanValidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_valid_compiled_query_passes_schema_and_plan_validation(self) -> None:
        location = _resolved_entity("location", "0706000000")
        resolved = ResolvedAskPlan(
            plan=AskPlan(
                intent="event_count",
                location_mentions=["Cebu"],
                start_date="2024-01-01",
                end_date="2024-12-31",
                metric="events",
                group_by="year",
            ),
            locations=[location],
        )
        artifact = compile_query(resolved)
        with patch(
            "src.services.ask.query_validator.get_schema_catalog",
            new=AsyncMock(return_value=_catalog()),
        ):
            report = await validate_query_artifact(artifact, resolved)

        self.assertTrue(report.summary.has_aggregate)
        self.assertIn("https://sakuna.ph/hasLocation", report.validated_terms)

    async def test_hallucinated_vocabulary_is_rejected_before_execution(self) -> None:
        resolved = ResolvedAskPlan(plan=AskPlan(intent="open_graph_query"))
        artifact = QueryArtifact(
            sparql="""PREFIX : <https://sakuna.ph/>
SELECT ?event WHERE {
  ?event a :DisasterEvent ; :numberOfDeaths ?count .
}
LIMIT 10
""",
            origin="model_fallback",
        )
        with patch(
            "src.services.ask.query_validator.get_schema_catalog",
            new=AsyncMock(return_value=_catalog()),
        ):
            with self.assertRaises(ServiceError) as raised:
                await validate_query_artifact(artifact, resolved)

        self.assertIn("numberOfDeaths", raised.exception.detail)

    async def test_known_domain_mismatch_is_rejected(self) -> None:
        resolved = ResolvedAskPlan(plan=AskPlan(intent="open_graph_query"))
        artifact = QueryArtifact(
            sparql="""PREFIX : <https://sakuna.ph/>
SELECT ?count WHERE {
  ?event a :DisasterEvent .
  ?event :casualtyCount ?count .
}
LIMIT 10
""",
            origin="model_fallback",
        )
        with patch(
            "src.services.ask.query_validator.get_schema_catalog",
            new=AsyncMock(return_value=_catalog()),
        ):
            with self.assertRaises(ServiceError) as raised:
                await validate_query_artifact(artifact, resolved)

        self.assertIn("incompatible", raised.exception.detail)

    async def test_missing_resolved_entity_and_date_are_rejected(self) -> None:
        location = _resolved_entity("location", "0706000000")
        resolved = ResolvedAskPlan(
            plan=AskPlan(
                intent="list_events",
                location_mentions=["Cebu"],
                start_date="2024-01-01",
            ),
            locations=[location],
        )
        artifact = QueryArtifact(
            sparql="""PREFIX : <https://sakuna.ph/>
SELECT ?event WHERE { ?event a :DisasterEvent . }
LIMIT 10
""",
            origin="model_fallback",
            projected_columns=["event"],
            expected_columns=["event"],
        )
        with patch(
            "src.services.ask.query_validator.get_schema_catalog",
            new=AsyncMock(return_value=_catalog()),
        ):
            with self.assertRaises(ServiceError):
                await validate_query_artifact(artifact, resolved)

    async def test_schema_catalog_is_graphdb_backed_and_cached(self) -> None:
        clear_schema_catalog_cache()
        result = {
            "results": {
                "bindings": [
                    {
                        "term": {"type": "uri", "value": "https://sakuna.ph/DisasterEvent"},
                        "kind": {
                            "type": "uri",
                            "value": "http://www.w3.org/2002/07/owl#Class",
                        },
                        "label": {"type": "literal", "value": "Disaster event"},
                    }
                ]
            }
        }
        with patch(
            "src.services.ask.schema_catalog.execute_sparql",
            new=AsyncMock(return_value=result),
        ) as graphdb:
            first = await get_schema_catalog()
            second = await get_schema_catalog()

        self.assertEqual(graphdb.await_count, 1)
        self.assertIs(first, second)
        self.assertIn("https://sakuna.ph/DisasterEvent", first.terms)
        clear_schema_catalog_cache()


class ResultValidationTests(unittest.TestCase):
    def test_valid_empty_result_is_not_a_failure(self) -> None:
        resolved = ResolvedAskPlan(plan=AskPlan(intent="list_events"))
        artifact = QueryArtifact(
            sparql="SELECT ?event WHERE { ?event ?p ?o } LIMIT 10",
            origin="compiler",
            expected_columns=["event"],
        )
        raw = {"head": {"vars": ["event"]}, "results": {"bindings": []}}

        report = validate_query_results(raw, artifact, resolved)

        self.assertEqual(report.row_count, 0)
        self.assertFalse(report.truncated)

    def test_invalid_shape_and_non_numeric_aggregate_are_rejected(self) -> None:
        resolved = ResolvedAskPlan(
            plan=AskPlan(intent="event_count", metric="events")
        )
        artifact = QueryArtifact(
            sparql="SELECT (COUNT(?event) AS ?total) WHERE { ?event ?p ?o }",
            origin="compiler",
            expected_columns=["total"],
        )
        cases = (
            {"head": {"vars": []}, "results": {"bindings": []}},
            {
                "head": {"vars": ["total"]},
                "results": {
                    "bindings": [
                        {"total": {"type": "literal", "value": "not-a-number"}}
                    ]
                },
            },
        )

        for raw in cases:
            with self.subTest(raw=raw):
                with self.assertRaises(ServiceError):
                    validate_query_results(raw, artifact, resolved)

    def test_truncation_is_explicit(self) -> None:
        resolved = ResolvedAskPlan(plan=AskPlan(intent="list_events"))
        artifact = QueryArtifact(
            sparql="SELECT ?event WHERE { ?event ?p ?o } LIMIT 10",
            origin="compiler",
            expected_columns=["event"],
        )
        raw = {
            "head": {"vars": ["event"]},
            "results": {"bindings": []},
            "_truncated": True,
        }

        report = validate_query_results(raw, artifact, resolved)

        self.assertTrue(report.truncated)
        self.assertIn("truncated", report.warnings[0])


class ValidationGateIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_hallucinated_open_query_never_reaches_graphdb(self) -> None:
        plan = AskPlan(intent="open_graph_query")
        resolved = ResolvedAskPlan(plan=plan)
        generated = """PREFIX : <https://sakuna.ph/>
SELECT ?event WHERE { ?event :madeUpProperty ?value . }
LIMIT 10
"""
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
                "src.services.ask.service.nl_to_sparql",
                new=AsyncMock(return_value=generated),
            ),
            patch(
                "src.services.ask.query_validator.get_schema_catalog",
                new=AsyncMock(return_value=_catalog()),
            ),
            patch(
                "src.services.ask.service.execute_sparql",
                new=AsyncMock(),
            ) as graphdb,
        ):
            with self.assertRaises(ServiceError):
                await ask_question("Use made up data")

        graphdb.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

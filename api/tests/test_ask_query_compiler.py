import inspect
import json
import re
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src.schemas.analysis import AnalysisEventsResponse, AnalysisSummaryResponse
from src.schemas.ask import AskStatus
from src.schemas.ask_execution import DeterministicAskResult
from src.schemas.ask_plan import AskPlan
from src.schemas.entity_resolution import ResolvedAskPlan, ResolvedEntity
from src.services.ask import query_compiler
from src.services.ask.query_compiler import compile_query
from src.services.ask.service import ask_question, preview_question
from src.services.ask.service_router import (
    execute_service_route,
    select_service_route,
    service_query_artifact,
)
from src.services.common import ServiceError
from src.services.sparql.executor import is_write_operation, validate_sparql


def _entity(
    entity_type: str,
    identifier: str,
    *,
    iri: str | None = None,
    label: str | None = None,
) -> ResolvedEntity:
    return ResolvedEntity(
        iri=iri or f"https://sakuna.ph/{identifier}",
        id=identifier,
        label=label or identifier,
        entity_type=entity_type,
        mention=label or identifier,
        match_type="exact",
        confidence=1.0,
    )


CEBU = _entity("location", "0706000000", label="Cebu")
FLOOD = _entity("disaster_type", "Flood")
DEAD = _entity("casualty_type", "Dead")
EVENT = _entity(
    "event",
    "event-yolanda",
    iri="https://sakuna.ph/events/ndrrmc/event-yolanda",
    label="Super Typhoon Yolanda",
)


class QueryCompilerSnapshotTests(unittest.TestCase):
    def test_event_count_snapshot(self) -> None:
        resolved = ResolvedAskPlan(
            plan=AskPlan(
                intent="event_count",
                event_type="major",
                location_mentions=["Cebu Province"],
                disaster_type_mentions=["floods"],
                start_date="2024-01-01",
                end_date="2024-12-31",
                metric="events",
                group_by="year",
                sort_direction="desc",
                limit=10,
            ),
            locations=[CEBU],
            disaster_types=[FLOOD],
        )
        expected = (
            Path(__file__).parent / "snapshots" / "ask_event_count_by_year.sparql"
        ).read_text(encoding="utf-8")

        artifact = compile_query(resolved)

        self.assertEqual(artifact.sparql, expected)
        self.assertEqual(artifact.origin, "compiler")
        self.assertEqual(artifact.expected_columns, ["group", "groupLabel", "total"])
        self.assertEqual(
            artifact.expected_entities,
            ["https://sakuna.ph/0706000000", "https://sakuna.ph/Flood"],
        )


class QueryCompilerBehaviorTests(unittest.TestCase):
    def test_listing_reuses_filters_hierarchies_and_deduplication(self) -> None:
        resolved = ResolvedAskPlan(
            plan=AskPlan(
                intent="list_events",
                location_mentions=["Cebu"],
                disaster_type_mentions=["Flood"],
                start_date="2023-01-01",
                end_date="2023-12-31",
                limit=15,
            ),
            locations=[CEBU],
            disaster_types=[FLOOD],
        )

        query = compile_query(resolved).sparql

        self.assertIn("VALUES ?eventClass { :MajorEvent :Incident }", query)
        self.assertIn(":isPartOf* ?selectedLocation", query)
        self.assertIn("skos:broader* ?selectedDisasterType", query)
        self.assertIn("FILTER NOT EXISTS", query)
        self.assertIn("prov:alternateOf", query)
        self.assertIn('>= "2023-01-01"', query)
        self.assertIn('<= "2023-12-31"', query)
        self.assertTrue(query.rstrip().endswith("LIMIT 15"))

    def test_casualty_and_damage_queries_use_controlled_graph_shapes(self) -> None:
        casualty = compile_query(
            ResolvedAskPlan(
                plan=AskPlan(intent="impact_summary", metric="dead"),
                casualty_types=[DEAD],
            )
        ).sparql
        damage = compile_query(
            ResolvedAskPlan(
                plan=AskPlan(
                    intent="impact_summary",
                    metric="damage",
                    group_by="location",
                )
            )
        ).sparql

        self.assertIn(":isOfCasualtyType <https://sakuna.ph/Dead>", casualty)
        self.assertIn(":casualtyCount ?rawValue", casualty)
        self.assertNotIn('"dead"', casualty)
        self.assertIn("qudt:numericValue ?rawValue", damage)
        self.assertIn("qudt:unit ?unit", damage)
        self.assertIn(":infraDamageAmount", damage)
        self.assertIn("GROUP BY ?group ?groupLabel ?unit", damage)

    def test_supported_query_variants_parse_and_are_read_only(self) -> None:
        plans = [
            ResolvedAskPlan(plan=AskPlan(intent="list_events", group_by="region")),
            ResolvedAskPlan(plan=AskPlan(intent="event_count", group_by="source")),
            ResolvedAskPlan(plan=AskPlan(intent="impact_summary")),
            ResolvedAskPlan(plan=AskPlan(intent="victim_trend")),
            ResolvedAskPlan(plan=AskPlan(intent="region_ranking", metric="events")),
            ResolvedAskPlan(
                plan=AskPlan(intent="disaster_ranking", metric="dead"),
                casualty_types=[DEAD],
            ),
            ResolvedAskPlan(
                plan=AskPlan(intent="event_details", event_mentions=["Yolanda"]),
                events=[EVENT],
            ),
            ResolvedAskPlan(
                plan=AskPlan(intent="source_lookup", event_mentions=["Yolanda"]),
                events=[EVENT],
            ),
        ]

        for resolved in plans:
            with self.subTest(intent=resolved.plan.intent):
                query = compile_query(resolved).sparql
                self.assertIsNone(validate_sparql(query))
                self.assertFalse(is_write_operation(query))

    def test_compiled_local_names_are_known_vocabulary_or_resolved_values(self) -> None:
        resolved = ResolvedAskPlan(
            plan=AskPlan(
                intent="impact_summary",
                location_mentions=["Cebu"],
                disaster_type_mentions=["Flood"],
                metric="damage",
                group_by="region",
            ),
            locations=[CEBU],
            disaster_types=[FLOOD],
        )
        query = compile_query(resolved).sparql
        local_names = set(re.findall(r"(?<![\w?]):([A-Za-z0-9][A-Za-z0-9._~-]*)", query))
        approved = {
            "0706000000",
            "Flood",
            "MajorEvent",
            "Incident",
            "Region",
            "startDate",
            "eventName",
            "incidentDescription",
            "endDate",
            "hasLocation",
            "isPartOf",
            "hasDisasterType",
            "hasDisasterSubtype",
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
        }
        self.assertLessEqual(local_names, approved)

    def test_unresolved_entities_unsafe_iris_and_open_queries_are_rejected(self) -> None:
        invalid = _entity(
            "event",
            "bad",
            iri="https://sakuna.ph/good> SERVICE <https://evil.example/",
        )
        cases = [
            ResolvedAskPlan(
                plan=AskPlan(intent="list_events", location_mentions=["Unknown"]),
                warnings=["No location entity matched 'Unknown'."],
            ),
            ResolvedAskPlan(
                plan=AskPlan(intent="event_details", event_mentions=["bad"]),
                events=[invalid],
            ),
            ResolvedAskPlan(plan=AskPlan(intent="open_graph_query")),
        ]

        for resolved in cases:
            with self.subTest(intent=resolved.plan.intent):
                with self.assertRaises(ServiceError):
                    compile_query(resolved)

    def test_compiler_has_no_etl_dependency(self) -> None:
        source = inspect.getsource(query_compiler)
        self.assertNotIn("from etl", source)
        self.assertNotIn("import etl", source)


class ServiceRouterTests(unittest.IsolatedAsyncioTestCase):
    def test_selects_existing_services_for_compatible_common_intents(self) -> None:
        cases = [
            (ResolvedAskPlan(plan=AskPlan(intent="list_events")), "analysis_events"),
            (
                ResolvedAskPlan(plan=AskPlan(intent="event_count", metric="events")),
                "analysis_event_count",
            ),
            (
                ResolvedAskPlan(
                    plan=AskPlan(intent="impact_summary", metric="dead"),
                    casualty_types=[DEAD],
                ),
                "analysis_summary",
            ),
            (
                ResolvedAskPlan(plan=AskPlan(intent="victim_trend")),
                "analysis_victim_trends",
            ),
            (
                ResolvedAskPlan(plan=AskPlan(intent="region_ranking", metric="events")),
                "analysis_region_rankings",
            ),
            (
                ResolvedAskPlan(
                    plan=AskPlan(intent="disaster_ranking", metric="dead"),
                    casualty_types=[DEAD],
                ),
                "analysis_disaster_rankings",
            ),
            (
                ResolvedAskPlan(
                    plan=AskPlan(intent="event_details", event_mentions=["Yolanda"]),
                    events=[EVENT],
                ),
                "event_details",
            ),
            (
                ResolvedAskPlan(
                    plan=AskPlan(intent="source_lookup", event_mentions=["Yolanda"]),
                    events=[EVENT],
                ),
                "event_sources",
            ),
        ]

        for resolved, expected in cases:
            with self.subTest(intent=resolved.plan.intent):
                self.assertEqual(select_service_route(resolved), expected)

    async def test_service_results_match_phase_four_golden_cases(self) -> None:
        fixture = json.loads(
            (
                Path(__file__).parent
                / "fixtures"
                / "ask_phase4_golden_results.json"
            ).read_text(encoding="utf-8")
        )
        for case in fixture["cases"]:
            plan = AskPlan(intent=case["intent"], metric=case["metric"])
            resolved = ResolvedAskPlan(
                plan=plan,
                casualty_types=[DEAD] if case["metric"] == "dead" else [],
            )
            route = select_service_route(resolved)
            self.assertIsNotNone(route)
            artifact = service_query_artifact(resolved, route)
            if route == "analysis_event_count":
                response = AnalysisEventsResponse(
                    items=[],
                    page=1,
                    page_size=1,
                    total=case["service_payload"]["record_count"],
                    sort_by="startDate",
                    sort_dir="desc",
                )
                target = "src.services.ask.service_router.get_analysis_events"
            else:
                response = AnalysisSummaryResponse.model_validate(case["service_payload"])
                target = "src.services.ask.service_router.get_summary"
            with patch(target, new=AsyncMock(return_value=response)):
                result = await execute_service_route(resolved, artifact)

            with self.subTest(intent=case["intent"], metric=case["metric"]):
                self.assertEqual(result.rows, case["expected_rows"])


class DeterministicAskIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_common_service_route_never_generates_model_sparql(self) -> None:
        plan = AskPlan(intent="event_count", metric="events")
        resolved = ResolvedAskPlan(plan=plan)

        async def service_result(_resolved, artifact):
            return DeterministicAskResult(query=artifact, rows=[{"total": "3"}])

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
                "src.services.ask.service.execute_service_route",
                new=AsyncMock(side_effect=service_result),
            ),
            patch(
                "src.services.ask.service.sparql_with_correction",
                new=AsyncMock(),
            ) as model_query,
            patch(
                "src.services.ask.service.nl_to_sparql",
                new=AsyncMock(),
            ) as preview_model,
            patch(
                "src.services.ask.service.execute_sparql",
                new=AsyncMock(),
            ) as direct_graphdb,
            patch(
                "src.services.ask.service.ground_answer",
                new=AsyncMock(return_value="Three events were found."),
            ),
        ):
            response = await ask_question("How many events occurred?")

        self.assertEqual(response.status, AskStatus.ANSWERED)
        self.assertEqual(response.rows, [{"total": "3"}])
        self.assertEqual(response.query_artifact.origin, "service")
        self.assertEqual(response.query_artifact.service_route, "analysis_event_count")
        model_query.assert_not_awaited()
        preview_model.assert_not_awaited()
        direct_graphdb.assert_not_awaited()

    async def test_grouped_question_executes_compiled_query_without_model(self) -> None:
        plan = AskPlan(intent="event_count", metric="events", group_by="year")
        resolved = ResolvedAskPlan(plan=plan)
        raw = {
            "head": {"vars": ["group", "groupLabel", "total"]},
            "results": {
                "bindings": [
                    {
                        "group": {"type": "literal", "value": "2024"},
                        "groupLabel": {"type": "literal", "value": "2024"},
                        "total": {"type": "literal", "value": "5"},
                    }
                ]
            },
        }
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
                "src.services.ask.service.execute_sparql",
                new=AsyncMock(return_value=raw),
            ) as graphdb,
            patch(
                "src.services.ask.service.sparql_with_correction",
                new=AsyncMock(),
            ) as model_query,
            patch(
                "src.services.ask.service.ground_answer",
                new=AsyncMock(return_value="Five events occurred in 2024."),
            ),
        ):
            response = await ask_question("Count events by year")

        self.assertEqual(response.query_artifact.origin, "compiler")
        self.assertEqual(response.rows[0]["total"], "5")
        graphdb.assert_awaited_once()
        model_query.assert_not_awaited()

    async def test_common_preview_is_deterministic(self) -> None:
        plan = AskPlan(intent="list_events", limit=5)
        resolved = ResolvedAskPlan(plan=plan)
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
                new=AsyncMock(),
            ) as model_query,
        ):
            response = await preview_question("List five events")

        self.assertEqual(response.status, AskStatus.QUERY_READY)
        self.assertEqual(response.query_artifact.origin, "service")
        self.assertIn("LIMIT 5", response.sparql)
        model_query.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

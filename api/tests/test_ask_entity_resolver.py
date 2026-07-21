import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src.schemas.ask import AskStatus
from src.schemas.ask_plan import AskPlan
from src.schemas.entity_resolution import (
    EntityAmbiguity,
    EntityCatalogEntry,
    ResolvedAskPlan,
    ResolvedEntity,
)
from src.services.ask import entity_resolver
from src.services.ask.entity_resolver import (
    clear_entity_resolution_caches,
    resolve_ask_plan,
    resolve_mentions,
)
from src.services.ask.service import (
    ask_question,
    preview_question,
    stream_answer_events,
)


def _entry(
    entity_type: str,
    identifier: str,
    label: str,
    *,
    aliases: list[str] | None = None,
    hierarchy_aliases: list[str] | None = None,
    level: str | None = None,
) -> EntityCatalogEntry:
    return EntityCatalogEntry(
        iri=f"https://catalog.example/{entity_type}/{identifier}",
        id=identifier,
        label=label,
        entity_type=entity_type,
        aliases=aliases or [],
        hierarchy_aliases=hierarchy_aliases or [],
        level=level,
    )


LOCATION_CATALOG = [
    _entry("location", "1300000000", "National Capital Region", aliases=["NCR"]),
    _entry("location", "0100000000", "Ilocos Region", level="Region"),
    _entry(
        "location",
        "1401000000",
        "Abra",
        hierarchy_aliases=["Abra Province", "Province of Abra"],
        level="Province",
    ),
    _entry(
        "location",
        "0706000000",
        "Cebu",
        hierarchy_aliases=["Cebu Province", "Province of Cebu"],
        level="Province",
    ),
    _entry(
        "location",
        "0730600000",
        "City of Cebu",
        hierarchy_aliases=["Cebu", "Cebu City"],
        level="City",
    ),
    _entry(
        "location",
        "1102400000",
        "Davao del Sur",
        hierarchy_aliases=["Davao del Sur Province"],
        level="Province",
    ),
    _entry("location", "1130700000", "Davao City", level="City"),
    _entry("location", "1430300000", "Baguio City", level="City"),
]

DISASTER_CATALOG = [
    _entry("disaster_type", "Flood", "Flood", aliases=["Floods"]),
    _entry(
        "disaster_type",
        "TropicalCyclone",
        "Tropical Cyclone",
        aliases=["Tropical Cyclones"],
    ),
    _entry("disaster_type", "Earthquake", "Earthquake"),
    _entry("disaster_type", "Drought", "Drought"),
]

EVENT_CATALOG = [
    _entry(
        "event",
        "event-yolanda",
        "Super Typhoon Yolanda",
        aliases=["Haiyan"],
    )
]

ORGANIZATION_CATALOG = [
    _entry(
        "organization",
        "NDRRMC",
        "NDRRMC",
        aliases=["National Disaster Risk Reduction and Management Council"],
    )
]

CASUALTY_CATALOG = [
    _entry("casualty_type", "Dead", "Dead"),
    _entry("casualty_type", "Injured", "Injured"),
    _entry("casualty_type", "Missing", "Missing"),
]

CATALOGS = {
    "location": LOCATION_CATALOG,
    "disaster_type": DISASTER_CATALOG,
    "event": EVENT_CATALOG,
    "organization": ORGANIZATION_CATALOG,
    "casualty_type": CASUALTY_CATALOG,
}

ALIASES = {
    "disaster_type": entity_resolver._DISASTER_ALIASES,
    "casualty_type": entity_resolver._CASUALTY_ALIASES,
}


def _term(value: str) -> dict[str, str]:
    return {"type": "literal", "value": value}


class EntityResolverGoldenTests(unittest.TestCase):
    def test_entity_link_accuracy_meets_phase_three_threshold(self) -> None:
        fixture = json.loads(
            (Path(__file__).parent / "fixtures" / "ask_entity_golden_cases.json").read_text(
                encoding="utf-8"
            )
        )
        correct = 0
        for case in fixture["cases"]:
            entity_type = case["entity_type"]
            matches, ambiguities, _ = resolve_mentions(
                [case["mention"]],
                CATALOGS[entity_type],
                entity_type,
                aliases=ALIASES.get(entity_type),
            )
            with self.subTest(mention=case["mention"], entity_type=entity_type):
                self.assertFalse(ambiguities)
                self.assertEqual(len(matches), 1)
                correct += matches[0].id == case["expected_id"]

        accuracy = correct / len(fixture["cases"])
        self.assertGreaterEqual(accuracy, fixture["threshold"])

    def test_hierarchical_location_match_disambiguates_level(self) -> None:
        matches, ambiguities, warnings = resolve_mentions(
            ["Cebu Province"],
            LOCATION_CATALOG,
            "location",
        )

        self.assertEqual(matches[0].id, "0706000000")
        self.assertEqual(matches[0].match_type, "hierarchy")
        self.assertFalse(ambiguities)
        self.assertFalse(warnings)

    def test_fuzzy_match_accepts_clear_typo(self) -> None:
        matches, ambiguities, warnings = resolve_mentions(
            ["Baguo Cty"],
            LOCATION_CATALOG,
            "location",
        )

        self.assertEqual(matches[0].id, "1430300000")
        self.assertEqual(matches[0].match_type, "fuzzy")
        self.assertFalse(ambiguities)
        self.assertFalse(warnings)

    def test_ambiguous_location_is_surfaced_instead_of_guessed(self) -> None:
        matches, ambiguities, warnings = resolve_mentions(
            ["Cebu"],
            LOCATION_CATALOG,
            "location",
        )

        self.assertFalse(matches)
        self.assertFalse(warnings)
        self.assertEqual(len(ambiguities), 1)
        self.assertEqual(
            {candidate.id for candidate in ambiguities[0].candidates},
            {"0706000000", "0730600000"},
        )

    def test_resolved_iris_are_limited_to_catalog_entries(self) -> None:
        allowed = {entry.iri for entries in CATALOGS.values() for entry in entries}
        produced: set[str] = set()
        for entity_type, entries in CATALOGS.items():
            match, _, _ = resolve_mentions(
                [entries[0].label],
                entries,
                entity_type,
                aliases=ALIASES.get(entity_type),
            )
            produced.update(item.iri for item in match)

        self.assertTrue(produced)
        self.assertLessEqual(produced, allowed)


class GraphDbCatalogTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        clear_entity_resolution_caches()

    def tearDown(self) -> None:
        clear_entity_resolution_caches()

    async def test_psgc_catalog_is_read_from_graphdb_and_cached(self) -> None:
        graphdb_iri = "https://sakuna.ph/psgc/0706000000"
        result = {
            "results": {
                "bindings": [
                    {
                        "entity": _term(graphdb_iri),
                        "code": _term("0706000000"),
                        "label": _term("Cebu"),
                        "levelLabel": _term("Province"),
                        "region": _term("https://sakuna.ph/psgc/0700000000"),
                        "regionLabel": _term("Central Visayas"),
                    }
                ]
            }
        }
        plan = AskPlan(intent="list_events", location_mentions=["Cebu Province"])
        with patch(
            "src.services.ask.entity_resolver.execute_sparql",
            new=AsyncMock(return_value=result),
        ) as graphdb:
            first = await resolve_ask_plan("List events in Cebu Province", plan)
            second = await resolve_ask_plan("List events in Cebu Province", plan)

        self.assertEqual(graphdb.await_count, 1)
        self.assertEqual(first.locations[0].iri, graphdb_iri)
        self.assertEqual(second.locations[0].iri, graphdb_iri)
        self.assertEqual(first.locations[0].match_type, "hierarchy")

    async def test_taxonomy_casualty_event_and_organization_resolution(self) -> None:
        plan = AskPlan(
            intent="impact_summary",
            disaster_type_mentions=["typhoon"],
            event_mentions=["Haiyan"],
            organization_mentions=["NDRRMC"],
            metric="dead",
        )
        with (
            patch(
                "src.services.ask.entity_resolver._disaster_catalog",
                new=AsyncMock(return_value=DISASTER_CATALOG),
            ),
            patch(
                "src.services.ask.entity_resolver._event_catalog",
                new=AsyncMock(return_value=EVENT_CATALOG),
            ),
            patch(
                "src.services.ask.entity_resolver._organization_catalog",
                new=AsyncMock(return_value=ORGANIZATION_CATALOG),
            ),
            patch(
                "src.services.ask.entity_resolver._casualty_catalog",
                new=AsyncMock(return_value=CASUALTY_CATALOG),
            ),
        ):
            resolved = await resolve_ask_plan(
                "How many fatalities from Haiyan did NDRRMC report?",
                plan,
            )

        self.assertEqual(resolved.disaster_types[0].id, "TropicalCyclone")
        self.assertEqual(resolved.events[0].id, "event-yolanda")
        self.assertEqual(resolved.organizations[0].id, "NDRRMC")
        self.assertEqual(resolved.casualty_types[0].id, "Dead")
        self.assertFalse(resolved.ambiguities)


class AskAmbiguityGateTests(unittest.IsolatedAsyncioTestCase):
    def _ambiguous_plan(self) -> ResolvedAskPlan:
        candidates = [
            ResolvedEntity(
                iri=entry.iri,
                id=entry.id,
                label=entry.label,
                entity_type="location",
                mention="Cebu",
                match_type="exact" if index == 0 else "hierarchy",
                confidence=1.0 if index == 0 else 0.96,
            )
            for index, entry in enumerate(LOCATION_CATALOG[3:5])
        ]
        ambiguity = EntityAmbiguity(
            mention="Cebu",
            entity_type="location",
            reason="Cebu matches multiple locations.",
            candidates=candidates,
        )
        return ResolvedAskPlan(
            plan=AskPlan(intent="list_events", location_mentions=["Cebu"]),
            ambiguities=[ambiguity],
        )

    async def test_ask_and_preview_stop_before_query_generation(self) -> None:
        plan = AskPlan(intent="list_events", location_mentions=["Cebu"])
        resolved = self._ambiguous_plan()
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
                new=AsyncMock(),
            ) as execute,
            patch(
                "src.services.ask.service.nl_to_sparql",
                new=AsyncMock(),
            ) as generate,
            patch(
                "src.services.ask.service.ground_answer",
                new=AsyncMock(),
            ) as answer,
        ):
            response = await ask_question("List events in Cebu")
            preview = await preview_question("List events in Cebu")

        self.assertEqual(response.status, AskStatus.NEEDS_DISAMBIGUATION)
        self.assertEqual(preview.status, AskStatus.NEEDS_DISAMBIGUATION)
        self.assertEqual(response.sparql, "")
        self.assertEqual(len(response.ambiguities or []), 1)
        execute.assert_not_awaited()
        generate.assert_not_awaited()
        answer.assert_not_awaited()

    async def test_stream_preserves_legacy_events_for_ambiguity(self) -> None:
        plan = AskPlan(intent="list_events", location_mentions=["Cebu"])
        with (
            patch(
                "src.services.ask.service.plan_question",
                new=AsyncMock(return_value=plan),
            ),
            patch(
                "src.services.ask.service.resolve_ask_plan",
                new=AsyncMock(return_value=self._ambiguous_plan()),
            ),
            patch(
                "src.services.ask.service.execute_sparql",
                new=AsyncMock(),
            ) as execute,
        ):
            events = [event async for event in stream_answer_events("List events in Cebu")]

        payloads = [json.loads(event.removeprefix("data: ")) for event in events]
        self.assertEqual([payload["type"] for payload in payloads], ["meta", "token", "done"])
        self.assertEqual(payloads[0]["status"], "needs_disambiguation")
        self.assertEqual(len(payloads[0]["ambiguities"]), 1)
        execute.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

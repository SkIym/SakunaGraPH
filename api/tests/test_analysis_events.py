import csv
import io
import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from src.main import app
from src.schemas.analysis import AnalysisEvent, AnalysisEventsResponse
from src.services.analysis.common import event_filter_where, make_analysis_filters
from src.services.analysis.events import (
    _apply_impacts,
    _apply_metadata,
    events_to_csv,
)
from src.services.common import ServiceError


def _binding(**values: str) -> dict[str, dict[str, str]]:
    return {key: {"value": value} for key, value in values.items()}


def _result(*bindings: dict[str, dict[str, str]]) -> dict:
    return {"results": {"bindings": list(bindings)}}


class AnalysisFilterTests(unittest.TestCase):
    def test_filter_fragment_supports_shared_analysis_contract(self) -> None:
        filters = make_analysis_filters(
            event_type="all",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            location_ids=["1400000000", "1300000000", "1300000000"],
            disaster_types=["Flood"],
            q='storm "enteng"',
        )

        fragment = event_filter_where(filters)

        self.assertEqual(filters.location_ids, ("1300000000", "1400000000"))
        self.assertIn(":isPartOf* ?selectedLocation", fragment)
        self.assertIn("skos:broader* ?selectedDisasterType", fragment)
        self.assertIn('LCASE("storm \\"enteng\\"")', fragment)
        self.assertIn("prov:alternateOf", fragment)

    def test_rejects_invalid_or_injected_filter_values(self) -> None:
        with self.assertRaises(ServiceError):
            make_analysis_filters(location_ids=["1300000000) } UNION {"])
        with self.assertRaises(ServiceError):
            make_analysis_filters(disaster_types=["Flood> } UNION {"])
        with self.assertRaises(ServiceError):
            make_analysis_filters(
                start_date=date(2025, 1, 2),
                end_date=date(2025, 1, 1),
            )


class AnalysisEventMappingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = AnalysisEvent(
            event="https://sakuna.ph/ndrrmc/event/related_incident/1",
            eventName="Example",
            eventType="Incident",
            startDate="2024-01-01",
            source="NDRRMC",
        )
        self.events = {self.event.event: self.event}

    def test_maps_facets_sources_and_impact_totals(self) -> None:
        _apply_metadata(
            self.events,
            _result(
                _binding(
                    event=self.event.event,
                    kind="location",
                    resource="https://sakuna.ph/1300000000",
                    id="1300000000",
                    label="NCR",
                ),
                _binding(
                    event=self.event.event,
                    kind="disasterType",
                    resource="https://sakuna.ph/TropicalCyclone",
                    label="Tropical Cyclone",
                ),
                _binding(
                    event=self.event.event,
                    kind="source",
                    resource="https://sakuna.ph/org/NDRRMC",
                    label="National Disaster Risk Reduction and Management Council",
                ),
            ),
        )
        _apply_impacts(
            self.events,
            _result(
                _binding(event=self.event.event, metric="dead", value="2"),
                _binding(
                    event=self.event.event,
                    metric="affectedFamilies",
                    value="15",
                ),
                _binding(
                    event=self.event.event,
                    metric="damage",
                    value="7.5",
                    unit="https://sakuna.ph/PHP_millions",
                ),
            ),
        )

        self.assertEqual(self.event.locations[0].id, "1300000000")
        self.assertEqual(self.event.disasterTypes[0].id, "TropicalCyclone")
        self.assertEqual(self.event.source, "NDRRMC")
        self.assertEqual(self.event.impact.dead, 2)
        self.assertEqual(self.event.impact.affectedFamilies, 15)
        self.assertEqual(self.event.impact.damageAmount, 7.5)
        self.assertEqual(self.event.impact.damageUnit, "PHP_millions")

    def test_keeps_mixed_damage_units_separate(self) -> None:
        _apply_impacts(
            self.events,
            _result(
                _binding(
                    event=self.event.event,
                    metric="damage",
                    value="10",
                    unit="https://sakuna.ph/PHP_millions",
                ),
                _binding(
                    event=self.event.event,
                    metric="damage",
                    value="20",
                    unit="https://sakuna.ph/USD_thousands",
                ),
            ),
        )

        self.assertIsNone(self.event.impact.damageAmount)
        self.assertIsNone(self.event.impact.damageUnit)
        self.assertEqual(len(self.event.impact.damageByUnit), 2)

    def test_csv_is_flat_and_protects_spreadsheet_cells(self) -> None:
        self.event.eventName = "=SUM(1,1)"
        content = events_to_csv([self.event])
        row = next(csv.DictReader(io.StringIO(content)))

        self.assertEqual(row["eventName"], "'=SUM(1,1)")
        self.assertEqual(row["dead"], "0")


class AnalysisEventRouterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def test_event_query_parses_repeated_filters(self) -> None:
        payload = AnalysisEventsResponse(
            items=[],
            page=2,
            page_size=10,
            total=0,
            sort_by="eventName",
            sort_dir="asc",
        )
        with patch(
            "src.routers.analysis.get_analysis_events",
            new=AsyncMock(return_value=payload),
        ) as mocked:
            response = self.client.get(
                "/api/analysis/events",
                params=[
                    ("location_ids", "1300000000"),
                    ("location_ids", "1400000000"),
                    ("disaster_types", "Flood"),
                    ("page", "2"),
                    ("page_size", "10"),
                    ("sort_by", "eventName"),
                    ("sort_dir", "asc"),
                ],
            )

        self.assertEqual(response.status_code, 200)
        filters = mocked.await_args.kwargs["filters"]
        self.assertEqual(filters.location_ids, ("1300000000", "1400000000"))
        self.assertEqual(filters.disaster_types, ("Flood",))

    def test_export_returns_attachment_headers(self) -> None:
        with patch(
            "src.routers.analysis.get_analysis_events_export",
            new=AsyncMock(return_value="event,eventName\r\n"),
        ):
            response = self.client.get("/api/analysis/events/export.csv")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "text/csv; charset=utf-8")
        self.assertIn("attachment", response.headers["content-disposition"])

    def test_invalid_filter_returns_422_without_querying_graphdb(self) -> None:
        response = self.client.get(
            "/api/analysis/events",
            params={"location_ids": "not-a-psgc-code"},
        )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.json()["detail"],
            "location_ids values must be exactly 10 digits",
        )


if __name__ == "__main__":
    unittest.main()

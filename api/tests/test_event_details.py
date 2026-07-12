import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from src.main import app
from src.schemas.disasters import EventDetailsResponse
from src.services.common import ServiceError
from src.services.disasters.details import (
    _build_event_details,
    _event_details_query,
)


def _binding(**values: str) -> dict[str, dict[str, str]]:
    return {key: {"value": value} for key, value in values.items()}


class EventDetailsServiceTests(unittest.TestCase):
    def test_query_is_limited_to_core_metadata(self) -> None:
        query = _event_details_query("https://sakuna.ph/gda/example")

        self.assertIn(":hasRelatedIncident", query)
        self.assertIn(":remarks", query)
        self.assertIn(":reportLink", query)
        self.assertIn("rdf:predicate owl:sameAs", query)
        for deferred_property in (
            ":hasImpact",
            ":hasResponse",
            ":hasPreparedness",
            ":hasWarning",
        ):
            self.assertNotIn(deferred_property, query)

    def test_builds_core_relationship_and_source_details(self) -> None:
        event_iri = "https://sakuna.ph/gda/event"
        bindings = [
            _binding(
                kind="core",
                resource=event_iri,
                label="Example storm",
                eventClass="https://sakuna.ph/MajorEvent",
                startDate="2024-01-01T00:00:00",
                endDate="2024-01-03T00:00:00",
                remarks="A multi-line note from the source.",
            ),
            _binding(
                kind="location",
                resource="https://sakuna.ph/1300000000",
                id="1300000000",
                label="NCR",
            ),
            _binding(
                kind="disasterType",
                resource="https://sakuna.ph/TropicalCyclone",
                label="Tropical Cyclone",
            ),
            _binding(
                kind="incident",
                resource=f"{event_iri}/related_incident/1",
                label="Flooding incident",
                eventClass="https://sakuna.ph/Incident",
                startDate="2024-01-02T00:00:00",
            ),
            _binding(
                kind="alternate",
                resource="https://sakuna.ph/emdat/alternate",
                label="Alternate storm",
                eventClass="https://sakuna.ph/MajorEvent",
                startDate="2024-01-01",
            ),
            _binding(
                kind="source",
                resource="https://sakuna.ph/source/report",
                reportName="Situation Report 1",
                reportLink="https://example.com/report.pdf",
                obtainedDate="2024-02-01",
                format="pdf",
                attributedTo="https://sakuna.ph/org/NDRRMC",
                attributedToLabel="NDRRMC",
            ),
        ]

        details = _build_event_details(event_iri, bindings)

        self.assertEqual(details.name, "Example storm")
        self.assertEqual(details.eventType, "MajorEvent")
        self.assertEqual(details.startDate, "2024-01-01")
        self.assertEqual(details.remarks, ["A multi-line note from the source."])
        self.assertEqual(details.locations[0].id, "1300000000")
        self.assertEqual(details.disasterTypes[0].id, "TropicalCyclone")
        self.assertEqual(details.incidents[0].name, "Flooding incident")
        self.assertEqual(details.alternates[0].name, "Alternate storm")
        self.assertEqual(details.sources[0].reportName, "Situation Report 1")
        self.assertEqual(details.sources[0].attributedTo[0].label, "NDRRMC")

    def test_missing_event_is_404(self) -> None:
        with self.assertRaises(ServiceError) as context:
            _build_event_details("https://sakuna.ph/missing", [])

        self.assertEqual(context.exception.status_code, 404)


class EventDetailsRouterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def test_details_endpoint_returns_service_response(self) -> None:
        payload = EventDetailsResponse(
            event="https://sakuna.ph/gda/event",
            name="Example",
            eventType="MajorEvent",
        )
        with patch(
            "src.routers.disasters.get_event_details",
            new=AsyncMock(return_value=payload),
        ) as mocked:
            response = self.client.get(
                "/api/disasters/details",
                params={"uri": payload.event},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["name"], "Example")
        mocked.assert_awaited_once_with(uri=payload.event)


if __name__ == "__main__":
    unittest.main()

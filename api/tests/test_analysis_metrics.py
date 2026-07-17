import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from src.main import app
from src.schemas.analysis import (
    AnalysisDamageAmount,
    AnalysisEvent,
    AnalysisEventFacet,
    AnalysisEventImpact,
    AnalysisSummaryResponse,
)
from src.services.analysis.common import make_analysis_filters
from src.services.analysis.metrics import (
    _region_rankings_query,
    get_damage_histogram,
    get_damage_vs_affected,
    get_disaster_counts,
    get_disaster_rankings,
    get_summary,
    get_victim_trends,
)


def _events() -> list[AnalysisEvent]:
    return [
        AnalysisEvent(
            event="https://sakuna.ph/gda/event-1",
            eventName="Alpha flood",
            eventType="MajorEvent",
            startDate="2023-08-01",
            disasterTypes=[AnalysisEventFacet(id="Flood", label="Flood")],
            impact=AnalysisEventImpact(
                dead=2,
                injured=3,
                affectedFamilies=4,
                affectedPersons=20,
                damageByUnit=[AnalysisDamageAmount(unit="PHP", amount=10)],
            ),
        ),
        AnalysisEvent(
            event="https://sakuna.ph/emdat/event-2",
            eventName="Beta cyclone",
            eventType="MajorEvent",
            startDate="2024-01-10",
            disasterTypes=[
                AnalysisEventFacet(id="Storm", label="Storm"),
                AnalysisEventFacet(id="Flood", label="Flood"),
            ],
            impact=AnalysisEventImpact(
                dead=5,
                missing=1,
                affectedFamilies=6,
                affectedPersons=50,
                damageByUnit=[AnalysisDamageAmount(unit="PHP", amount=30)],
            ),
        ),
    ]


class AnalysisMetricServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.filters = make_analysis_filters()

    @patch("src.services.analysis.metrics.get_all_analysis_events", new_callable=AsyncMock)
    async def test_summary_trends_and_rankings_aggregate_filtered_events(self, mocked) -> None:
        mocked.return_value = _events()

        summary = await get_summary(self.filters)
        trends = await get_victim_trends(self.filters)
        counts = await get_disaster_counts(self.filters, "type")
        rankings = await get_disaster_rankings(self.filters)

        self.assertEqual(summary.record_count, 2)
        self.assertEqual(summary.dead, 7)
        self.assertEqual(summary.damage[0].amount, 40)
        self.assertEqual([(item.year, item.dead) for item in trends.items], [(2023, 2), (2024, 5)])
        self.assertEqual([(item.id, item.count) for item in counts.items], [("Flood", 2), ("Storm", 1)])
        self.assertEqual([(item.id, item.dead) for item in rankings.items], [("Flood", 7), ("Storm", 5)])

    @patch("src.services.analysis.metrics.get_all_analysis_events", new_callable=AsyncMock)
    async def test_damage_charts_keep_units_and_event_points(self, mocked) -> None:
        mocked.return_value = _events()

        histogram = await get_damage_histogram(self.filters, bins=2, unit="PHP")
        scatter = await get_damage_vs_affected(self.filters)

        self.assertEqual(sum(item.count for item in histogram.bins), 2)
        self.assertEqual({item.unit for item in histogram.bins}, {"PHP"})
        self.assertEqual(len(scatter.items), 2)
        self.assertEqual(scatter.items[1].affectedPersons, 50)

    def test_region_query_uses_shared_filters_and_region_hierarchy(self) -> None:
        query = _region_rankings_query(self.filters)

        self.assertIn("?location :isPartOf* ?region", query)
        self.assertIn("?region a :Region", query)
        self.assertIn("prov:alternateOf", query)


class AnalysisMetricRouterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def test_summary_uses_the_shared_filter_contract(self) -> None:
        payload = AnalysisSummaryResponse(record_count=4)
        with patch(
            "src.routers.analysis.get_summary",
            new=AsyncMock(return_value=payload),
        ) as mocked:
            response = self.client.get(
                "/api/analysis/summary",
                params=[("location_ids", "1300000000"), ("disaster_types", "Flood")],
            )

        self.assertEqual(response.status_code, 200)
        filters = mocked.await_args.args[0]
        self.assertEqual(filters.location_ids, ("1300000000",))
        self.assertEqual(filters.disaster_types, ("Flood",))

    def test_histogram_rejects_invalid_damage_unit(self) -> None:
        response = self.client.get("/api/analysis/damage-histogram", params={"unit": "PHP> UNION"})

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["detail"], "unit must be a local QUDT unit id")


if __name__ == "__main__":
    unittest.main()

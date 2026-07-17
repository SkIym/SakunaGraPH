import unittest
from unittest.mock import AsyncMock, patch

from src.schemas.analysis import AnalysisEvent, AnalysisEventFacet, AnalysisEventImpact
from src.schemas.ontology import TaxonomyNode
from src.services.analysis.common import make_analysis_filters
from src.services.analysis.timeline import (
    get_calendar_days,
    get_calendar_months,
    get_calendar_years,
    get_category_stacks,
    get_date_events,
)
from src.services.common import ServiceError


def _events() -> list[AnalysisEvent]:
    return [
        AnalysisEvent(
            event="https://sakuna.ph/gda/one",
            eventName="August flood",
            eventType="MajorEvent",
            startDate="2023-08-01",
            disasterTypes=[AnalysisEventFacet(id="Flood", label="Flood")],
            impact=AnalysisEventImpact(dead=2, injured=1),
        ),
        AnalysisEvent(
            event="https://sakuna.ph/emdat/two",
            eventName="August storm",
            eventType="Incident",
            startDate="2023-08-02",
            disasterTypes=[AnalysisEventFacet(id="Storm", label="Storm")],
            impact=AnalysisEventImpact(dead=3, missing=1),
        ),
        AnalysisEvent(
            event="https://sakuna.ph/gda/three",
            eventName="January flood",
            eventType="MajorEvent",
            startDate="2024-01-10",
            disasterTypes=[AnalysisEventFacet(id="Flood", label="Flood")],
        ),
    ]


class AnalysisTimelineServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.filters = make_analysis_filters()

    @patch("src.services.analysis.timeline.get_all_analysis_events", new_callable=AsyncMock)
    async def test_calendar_drilldown_aggregates_dates_and_impacts(self, mocked) -> None:
        mocked.return_value = _events()

        years = await get_calendar_years(self.filters, include_impacts=True)
        months = await get_calendar_months(self.filters, year=2023, include_impacts=False)
        days = await get_calendar_days(self.filters, year=2023, month=8, include_impacts=True)

        self.assertEqual([(item.period, item.count, item.dead) for item in years.items], [("2023", 2, 5), ("2024", 1, 0)])
        self.assertEqual([(item.period, item.count, item.dead) for item in months.items], [("2023-08", 2, None)])
        self.assertEqual([(item.period, item.count) for item in days.items], [("2023-08-01", 1), ("2023-08-02", 1)])

    @patch("src.services.analysis.timeline.get_all_analysis_events", new_callable=AsyncMock)
    @patch("src.services.analysis.timeline.get_disaster_taxonomy", new_callable=AsyncMock)
    async def test_category_stacks_and_date_events_use_filtered_events(self, taxonomy_mocked, events_mocked) -> None:
        events_mocked.return_value = _events()
        taxonomy_mocked.return_value = TaxonomyNode(
            id="root", label="All", group="", definition="", children=[
                TaxonomyNode(id="Hydrological", label="Hydrological", group="", definition="", children=[
                    TaxonomyNode(id="Flood", label="Flood", group="", definition=""),
                ]),
                TaxonomyNode(id="Meteorological", label="Meteorological", group="", definition="", children=[
                    TaxonomyNode(id="Storm", label="Storm", group="", definition=""),
                ]),
            ],
        )

        stacks = await get_category_stacks(self.filters, bucket="month_year")
        date_events = await get_date_events(self.filters, date_prefix="2023-08")

        self.assertEqual(stacks.items[0].period, "2023-08")
        self.assertEqual([(item.id, item.count) for item in stacks.items[0].categories], [("Hydrological", 1), ("Meteorological", 1)])
        self.assertEqual([event.eventName for event in date_events.items], ["August flood", "August storm"])
        with self.assertRaises(ServiceError):
            await get_date_events(self.filters, date_prefix="2023-02-30")


if __name__ == "__main__":
    unittest.main()

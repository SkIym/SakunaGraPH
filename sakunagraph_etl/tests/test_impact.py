from dataclasses import dataclass
import unittest

import polars as pl

from sakunagraph_etl.sources.dromic.rdf import AffectedPopulation, Assistance, Housing, PEvac
from sakunagraph_etl.transform.impact import impact_entities


@dataclass
class ExampleImpact:
    id: int | None = None
    hasLocation: str | None = None
    count: int | None = None
    note: str | None = None


class ImpactFilteringTests(unittest.TestCase):
    def test_location_only_rows_are_removed_and_empty_locations_are_none(self) -> None:
        frame = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "hasLocation": ["https://sakuna.ph/location/1", "  ", "none", None],
                "count": [None, 3, None, 0],
                "note": [None, None, "  ", None],
            }
        )

        impacts = impact_entities(frame, ExampleImpact)

        self.assertEqual([impact.id for impact in impacts], [2, 4])
        self.assertIsNone(impacts[0].hasLocation)
        self.assertEqual(impacts[0].count, 3)
        self.assertEqual(impacts[1].count, 0)

    def test_every_dromic_impact_class_uses_the_same_non_location_rule(self) -> None:
        cases = (
            (AffectedPopulation, "affectedPersons", 8),
            (Housing, "totallyDamagedHouses", 2),
            (Assistance, "contributionAmount", 1.5),
            (PEvac, "evacuationCenters", 1),
        )

        for impact_class, property_name, value in cases:
            with self.subTest(impact_class=impact_class.__name__):
                frame = pl.DataFrame(
                    {
                        "id": ["location-only", "meaningful"],
                        "hasLocation": ["https://sakuna.ph/location/1", ""],
                        property_name: [None, value],
                    }
                )

                impacts = impact_entities(frame, impact_class)

                self.assertEqual(len(impacts), 1)
                self.assertEqual(impacts[0].id, "meaningful")
                self.assertIsNone(impacts[0].hasLocation)


if __name__ == "__main__":
    unittest.main()

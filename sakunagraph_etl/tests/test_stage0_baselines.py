from __future__ import annotations

import hashlib
import json
from pathlib import Path
import unittest


TESTS_ROOT = Path(__file__).parent
CATALOG_PATH = TESTS_ROOT / "baselines.json"
EXPECTED_SOURCES = {"dromic", "emdat", "gda", "ndrrmc", "psgc"}


class Stage0BaselineCatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))

    def test_catalog_covers_every_source(self) -> None:
        self.assertEqual(set(self.catalog), EXPECTED_SOURCES)

    def test_golden_outputs_match_recorded_evidence(self) -> None:
        for source, baseline in self.catalog.items():
            with self.subTest(source=source):
                fixture_path = TESTS_ROOT / baseline["fixture"]
                payload = fixture_path.read_bytes()
                triples = [line for line in payload.decode("utf-8").splitlines() if line]

                self.assertEqual(hashlib.sha256(payload).hexdigest(), baseline["sha256"])
                self.assertEqual(len(triples), baseline["triple_count"])
                self.assertIn(baseline["representative_triple"], triples)
                self.assertTrue(
                    any(f"<{baseline['deterministic_iri']}>" in triple for triple in triples),
                    f"{source} deterministic IRI is absent from its golden fixture",
                )

    def test_fixture_builders_are_recorded(self) -> None:
        for source, baseline in self.catalog.items():
            with self.subTest(source=source):
                self.assertRegex(
                    baseline["fixture_builder"],
                    r"^tests\.test_[a-z0-9_]+\.[A-Za-z0-9_]+$",
                )


if __name__ == "__main__":
    unittest.main()

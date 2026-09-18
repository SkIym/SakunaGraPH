from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
VALIDATION_ROOT = REPOSITORY_ROOT / "ontology" / "validation"
MANIFEST_PATH = VALIDATION_ROOT / "competency-manifest.json"
RUNNER_PATH = REPOSITORY_ROOT / "scripts" / "validate_semantics.py"


class CompetencyQuestionTests(unittest.TestCase):
    def test_manifest_covers_all_questions_categories_and_requirements(self) -> None:
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        cases = manifest["queries"]

        self.assertEqual(
            [f"CQ{index:02d}" for index in range(1, 21)],
            [case["id"] for case in cases],
        )
        self.assertEqual(
            {
                "event-and-type-classification",
                "casualties-and-population",
                "damage",
                "service-disruptions",
                "response-and-preparedness",
                "provenance",
            },
            {case["category"] for case in cases},
        )
        for case in cases:
            self.assertTrue(case["paper_pages"])
            self.assertTrue(case["requirements"])
            self.assertTrue(case["expected_columns"])
            self.assertIn(case["required_inference"], {"none", "OWL2-RL"})
            self.assertIsInstance(case["ordering_matters"], bool)
            self.assertEqual("fixtures/competency-v1.ttl", case["fixture"])
            self.assertTrue((VALIDATION_ROOT / case["query"]).is_file())
            self.assertTrue((VALIDATION_ROOT / case["expected"]).is_file())

    def test_offline_competency_suite(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(RUNNER_PATH), "--competency"],
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )

        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        self.assertIn("PASS: validated all 20 competency questions", completed.stdout)


if __name__ == "__main__":
    unittest.main()

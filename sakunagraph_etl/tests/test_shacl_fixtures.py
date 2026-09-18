from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import unittest

from rdflib import Graph
from rdflib.namespace import RDF, SH

from sakunagraph_etl.quality.shacl_fixtures import load_shacl_manifest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ONTOLOGY_ROOT = REPOSITORY_ROOT / "ontology"
MANIFEST_PATH = ONTOLOGY_ROOT / "validation" / "shacl-manifest.json"
RUNNER_PATH = REPOSITORY_ROOT / "scripts" / "validate_semantics.py"


class ShaclFixtureTests(unittest.TestCase):
    def test_manifest_covers_every_node_shape_and_required_property(self) -> None:
        manifest = load_shacl_manifest(MANIFEST_PATH)
        cases = manifest["cases"]

        shape_graphs: dict[str, Graph] = {}
        for scope, relative_path in manifest["shape_graphs"].items():
            shape_graphs[scope] = Graph().parse(ONTOLOGY_ROOT / relative_path)

        declared_node_shapes = {
            shape.n3()
            for graph in shape_graphs.values()
            for shape in graph.subjects(RDF.type, SH.NodeShape)
        }
        covered_node_shapes = {
            case["shape"]
            for case in cases
            if case["kind"] == "positive" and case.get("mode", "isolated") == "isolated"
        }
        self.assertEqual(declared_node_shapes, covered_node_shapes)

        required_property_shapes = {
            property_shape.n3()
            for graph in shape_graphs.values()
            for node_shape in graph.subjects(RDF.type, SH.NodeShape)
            for property_shape in graph.objects(node_shape, SH.property)
            if (minimum := graph.value(property_shape, SH.minCount)) is not None
            and int(minimum) > 0
        }
        covered_required_shapes = {
            result["source_shape"]
            for case in cases
            if case["kind"] == "negative"
            and "missing-required-property" in case["categories"]
            for result in case["expected_results"]
        }
        self.assertTrue(required_property_shapes.issubset(covered_required_shapes))

    def test_manifest_is_explicitly_post_paper(self) -> None:
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

        self.assertEqual(
            "post-paper-publication-contract",
            manifest["historical_scope"],
        )
        self.assertEqual(
            {"dromic", "emdat", "gda", "ndrrmc", "psgc"},
            set(manifest["source_profiles"]),
        )
        self.assertEqual("block", manifest["severity_policy"]["Violation"])
        self.assertEqual("report", manifest["severity_policy"]["Warning"])
        self.assertEqual("report", manifest["severity_policy"]["Info"])

    def test_offline_shacl_fixture_suite(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(RUNNER_PATH), "--shacl"],
            cwd=REPOSITORY_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )

        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        self.assertIn(
            "PASS: validated all 79 post-paper SHACL fixture cases",
            completed.stdout,
        )


if __name__ == "__main__":
    unittest.main()

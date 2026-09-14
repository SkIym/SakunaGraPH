from __future__ import annotations

import json
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_SCHEMA_PATH = REPOSITORY_ROOT / "ontology" / "release" / "manifest.schema.json"


class OntologyReleaseManifestTests(unittest.TestCase):
    def test_schema_uses_version_info_without_a_version_iri(self) -> None:
        schema = json.loads(MANIFEST_SCHEMA_PATH.read_text(encoding="utf-8"))
        ontology_properties = schema["properties"]["ontology"]["properties"]
        ontology_required = schema["properties"]["ontology"]["required"]

        self.assertEqual("https://sakuna.ph/", ontology_properties["iri"]["const"])
        self.assertIn("versionInfo", ontology_required)
        self.assertNotIn("versionIRI", ontology_properties)

    def test_schema_requires_immutable_release_evidence(self) -> None:
        schema = json.loads(MANIFEST_SCHEMA_PATH.read_text(encoding="utf-8"))
        release_required = schema["properties"]["release"]["required"]

        self.assertTrue({"tag", "commit", "releasedAt", "previousTag"}.issubset(release_required))
        self.assertIn("sha256", schema["properties"]["ontology"]["required"])
        self.assertIn("validation", schema["required"])


if __name__ == "__main__":
    unittest.main()

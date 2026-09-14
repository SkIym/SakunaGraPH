from __future__ import annotations

import hashlib
import unittest
from pathlib import Path
from xml.etree import ElementTree

from rdflib import Graph
from rdflib.namespace import OWL


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
IMPORTS_DIRECTORY = REPOSITORY_ROOT / "ontology" / "imports"
CATALOG_PATH = IMPORTS_DIRECTORY / "catalog-v001.xml"
SAKUNAGRAPH_ONTOLOGY_IRI = "https://sakuna.ph/"
DIRECT_IMPORTS = {
    "http://www.opengis.net/ont/geosparql/1.1",
    "http://www.w3.org/2004/02/skos/core",
    "http://www.w3.org/ns/prov-o-20130430",
    "https://raw.githubusercontent.com/beAWARE-project/ontology/master/beAWARE_ontology",
}
PINNED_SNAPSHOTS = {
    "beAWARE_ontology.owl": "d63f3f666b8bc533d10edb6712f7a7d2764c8056b2a8783766216dda2439698b",
    "geosparql-1.1.ttl": "7e6ee2d0ff5a04bd03cf849e822740204219bf11488a8036522a84d444f33619",
    "skos-2009.rdf": "e79633b8d0564816cee8a99f5c9acf9a0e6fc7257c7209acd684ecad53a89dd6",
    "prov-o-20130430.owl": "71ecff298c82b8c12aca0714a7bcbfd0b798cda9a7b3c89d9aeb0ef358cb79d0",
}


def load_catalog() -> dict[str, Path]:
    root = ElementTree.parse(CATALOG_PATH).getroot()
    namespace = "{urn:oasis:names:tc:entity:xmlns:xml:catalog}"
    return {
        entry.attrib["name"]: (IMPORTS_DIRECTORY / entry.attrib["uri"]).resolve()
        for entry in root.findall(f".//{namespace}uri")
    }


class OntologyImportCatalogTests(unittest.TestCase):
    def test_pinned_snapshot_checksums(self) -> None:
        for filename, expected_digest in PINNED_SNAPSHOTS.items():
            snapshot = IMPORTS_DIRECTORY / filename

            self.assertTrue(snapshot.is_file(), f"Missing vendored ontology: {snapshot}")
            actual_digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
            self.assertEqual(expected_digest, actual_digest, f"Checksum changed for {filename}")

    def test_direct_imports_have_local_catalog_entries(self) -> None:
        catalog = load_catalog()

        self.assertIn(SAKUNAGRAPH_ONTOLOGY_IRI, catalog)
        self.assertTrue(DIRECT_IMPORTS.issubset(catalog))

    def test_complete_import_closure_resolves_and_parses_offline(self) -> None:
        catalog = load_catalog()
        pending = [SAKUNAGRAPH_ONTOLOGY_IRI]
        visited: set[str] = set()

        while pending:
            ontology_iri = pending.pop()
            if ontology_iri in visited:
                continue

            self.assertIn(ontology_iri, catalog, f"No local catalog entry for {ontology_iri}")
            local_path = catalog[ontology_iri]
            self.assertTrue(local_path.is_file(), f"Missing vendored ontology: {local_path}")

            graph = Graph()
            graph.parse(local_path.as_uri())
            visited.add(ontology_iri)

            for imported_iri in graph.objects(None, OWL.imports):
                pending.append(str(imported_iri))

        self.assertTrue(DIRECT_IMPORTS.issubset(visited))


if __name__ == "__main__":
    unittest.main()

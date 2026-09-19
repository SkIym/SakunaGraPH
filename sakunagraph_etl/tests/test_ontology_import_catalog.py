from __future__ import annotations

import hashlib
import unittest
from pathlib import Path
from xml.etree import ElementTree

from rdflib import Graph, Literal, Namespace
from rdflib.namespace import OWL, RDF, RDFS, SKOS, XSD


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
IMPORTS_DIRECTORY = REPOSITORY_ROOT / "ontology" / "imports"
CATALOG_PATH = IMPORTS_DIRECTORY / "catalog-v001.xml"
SAKUNAGRAPH_ONTOLOGY_IRI = "https://sakuna.ph/"
DIRECT_IMPORTS = {
    "http://www.opengis.net/ont/geosparql/1.1",
    "http://qudt.org/3.5.1/qudt-all",
    "http://www.w3.org/2004/02/skos/core",
    "http://www.w3.org/ns/prov-o-20130430",
    "https://raw.githubusercontent.com/beAWARE-project/ontology/master/beAWARE_ontology",
}
PINNED_SNAPSHOTS = {
    "beAWARE_ontology.owl": "d63f3f666b8bc533d10edb6712f7a7d2764c8056b2a8783766216dda2439698b",
    "geosparql-1.1.ttl": "7e6ee2d0ff5a04bd03cf849e822740204219bf11488a8036522a84d444f33619",
    "skos-2009.rdf": "e79633b8d0564816cee8a99f5c9acf9a0e6fc7257c7209acd684ecad53a89dd6",
    "prov-o-20130430.owl": "71ecff298c82b8c12aca0714a7bcbfd0b798cda9a7b3c89d9aeb0ef358cb79d0",
    "qudt-3.5.1-all.ttl": "0a64bd88304a2a89c46badc870928e94c8a75b14fb4918045d5c81c62be8c66c",
    "vaem-2.0.rdf": "b2f19510a089aeee23b7773b7f619fe06f05e6e4185f250a0829df1d8d9cf62c",
}

SG = Namespace("https://sakuna.ph/")
BAW = Namespace(
    "https://raw.githubusercontent.com/beAWARE-project/ontology/master/beAWARE_ontology#"
)
QUDT = Namespace("http://qudt.org/schema/qudt/")
QUDT_PREFIX = Namespace("http://qudt.org/vocab/prefix/")
QUDT_UNIT = Namespace("http://qudt.org/vocab/unit/")
QUDT_QUANTITY_KIND = Namespace("http://qudt.org/vocab/quantitykind/")
LEGACY_QUDT_CURRENCY = Namespace("http://qudt.org/vocab/currency/")


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

    def test_reviewed_external_alignments_are_preserved(self) -> None:
        graph = Graph().parse(REPOSITORY_ROOT / "ontology" / "sakunagraph.ttl")

        self.assertIn((BAW.NaturalDisaster, RDFS.subClassOf, SG.DisasterEvent), graph)
        self.assertIn((BAW.hasDisasterStart, RDFS.subPropertyOf, SG.startDate), graph)
        self.assertIn((BAW.hasDisasterEnd, RDFS.subPropertyOf, SG.endDate), graph)

        for external, local in {
            BAW.Impact: SG.Impact,
            BAW.Incident: SG.Incident,
            BAW.Location: SG.Location,
        }.items():
            self.assertNotIn((external, OWL.equivalentClass, local), graph)
            self.assertNotIn((local, OWL.equivalentClass, external), graph)

        for external, local in {
            BAW.isOfDisasterType: SG.hasDisasterType,
            BAW.hasDisasterStart: SG.startDate,
            BAW.hasDisasterEnd: SG.endDate,
        }.items():
            self.assertNotIn((external, OWL.equivalentProperty, local), graph)
            self.assertNotIn((local, OWL.equivalentProperty, external), graph)

        self.assertNotIn(
            (BAW.hasDisasterOccurrence, OWL.inverseOf, SG.hasDisasterType), graph
        )
        self.assertNotIn(
            (SG.hasDisasterType, OWL.inverseOf, BAW.hasDisasterOccurrence), graph
        )
        self.assertNotIn((BAW.NaturalDisasterType, RDF.type, OWL.NamedIndividual), graph)

    def test_scaled_currency_units_follow_qudt_3_5_1_pattern(self) -> None:
        graph = Graph().parse(REPOSITORY_ROOT / "ontology" / "sakunagraph.ttl")
        expected = {
            SG.PHP_millions: (
                QUDT_UNIT.CCY_PHP,
                QUDT_PREFIX.Mega,
                Literal("1000000.0", datatype=XSD.decimal),
            ),
            SG.USD_thousands: (
                QUDT_UNIT.CCY_USD,
                QUDT_PREFIX.Kilo,
                Literal("1000.0", datatype=XSD.decimal),
            ),
        }

        for local_unit, (base_unit, prefix, multiplier) in expected.items():
            self.assertIn((local_unit, RDF.type, QUDT.CurrencyUnit), graph)
            self.assertIn((local_unit, RDF.type, QUDT.DerivedUnit), graph)
            self.assertIn((local_unit, RDF.type, QUDT.Unit), graph)
            self.assertIn((local_unit, QUDT.scalingOf, base_unit), graph)
            self.assertIn((local_unit, QUDT.prefix, prefix), graph)
            self.assertIn((local_unit, QUDT.conversionMultiplier, multiplier), graph)
            self.assertIn(
                (local_unit, QUDT.hasQuantityKind, QUDT_QUANTITY_KIND.Currency), graph
            )
            self.assertFalse(any(graph.objects(local_unit, QUDT.conversionOffset)))

        self.assertNotIn(
            (SG.PHP_millions, SKOS.broader, LEGACY_QUDT_CURRENCY.PHP), graph
        )
        self.assertNotIn(
            (SG.USD_thousands, SKOS.broader, LEGACY_QUDT_CURRENCY.USD), graph
        )
        self.assertNotIn(
            (QUDT.conversionMultiplier, RDF.type, OWL.AnnotationProperty), graph
        )
        self.assertNotIn(
            (QUDT.hasQuantityKind, RDF.type, OWL.AnnotationProperty), graph
        )

    def test_casualty_type_relation_is_object_property_only(self) -> None:
        graph = Graph().parse(REPOSITORY_ROOT / "ontology" / "sakunagraph.ttl")

        self.assertIn((SG.isOfCasualtyType, RDF.type, OWL.ObjectProperty), graph)
        self.assertIn((SG.isOfCasualtyType, RDFS.range, SG.CasualtyType), graph)
        self.assertNotIn((SG.isOfCasualtyType, RDF.type, OWL.DatatypeProperty), graph)


if __name__ == "__main__":
    unittest.main()

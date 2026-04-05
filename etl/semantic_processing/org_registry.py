"""
json_to_orgs_ttl.py
~~~~~~~~~~~~~~~~~~~~
Convert org_registry.json → orgs.ttl.

Registry shape expected:
    {
        "NDRRMC": [
            "National Disaster Risk Reduction and Management Council",
            "NDRRMCC"
        ],
        ...
    }

Convention used here:
  - The slug (key)      → skos:altLabel  (always the acronym/short form)
  - First alias         → skos:prefLabel (assumed to be the full proper name)
  - Remaining aliases   → skos:altLabel  (abbreviations, variants, misspellings)

If a slug has no aliases at all, the slug itself becomes the prefLabel.

Usage:
    python json_to_orgs_ttl.py                          # uses defaults
    python json_to_orgs_ttl.py registry.json orgs.ttl  # explicit paths
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SKOS, XSD

from mappings.graph import PROV, ORG

DATA_DIR = "../constants/"
OUT_DIR = "../data/rdf/orgs/"

# ── Namespaces ───────────────────────────────────────────────────────────────

org_base_iri = URIRef("https://sakuna.ph/org/")

# ── Helpers ──────────────────────────────────────────────────────────────────

def _lit(text: str) -> Literal:
    return Literal(text.strip(), lang="en")


def _org_iri(slug: str) -> URIRef:
    return ORG[slug]


# ── Core conversion ──────────────────────────────────────────────────────────

def build_graph(registry: dict[str, list[str]]) -> Graph:
    g = Graph()
    g.bind("owl",  OWL)
    g.bind("rdf",  RDF)
    g.bind("rdfs", RDFS)
    g.bind("skos", SKOS)
    g.bind("xsd",  XSD)
    g.bind("orgs",  ORG)

    for slug, aliases in registry.items():
        iri = _org_iri(slug)

        # Type declarations — real-world entity, not a vocabulary concept
        g.add((iri, RDF.type,   OWL.NamedIndividual))
        g.add((iri, RDF.type,   PROV.Organization))

        # Slug is always the short/acronym form → altLabel
        g.add((iri, SKOS.altLabel, _lit(slug)))

        if aliases:
            # First alias → prefLabel (full proper name)
            g.add((iri, SKOS.prefLabel, _lit(aliases[0])))
            # Remaining aliases → altLabel (variants, abbreviations, misspellings)
            for alt in aliases[1:]:
                g.add((iri, SKOS.altLabel, _lit(alt)))
        else:
            # No aliases: slug doubles as the prefLabel
            g.add((iri, SKOS.prefLabel, _lit(slug)))

    return g


def convert(registry_path: Path, output_path: Path) -> None:
    with open(registry_path, encoding="utf-8") as f:
        registry: dict[str, list[str]] = json.load(f)

    g = build_graph(registry)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    g.serialize(destination=str(output_path), format="turtle")
    print(f"Wrote {len(g)} triples → {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert org_registry.json to orgs.ttl."
    )
    parser.add_argument(
        "-i", "--input",
        type=Path,
        default=DATA_DIR + "org_registry.json",
        metavar="PATH",
        help="Path to org_registry.json (default: %(default)s)",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=OUT_DIR + "orgs.ttl",
        metavar="PATH",
        help="Destination path for orgs.ttl (default: %(default)s)",
    )
    args = parser.parse_args()
    convert(args.input, args.output)
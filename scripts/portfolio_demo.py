"""Verify and query SakunaGraPH's redistributable portfolio fixtures.

The demo deliberately avoids raw source data, GraphDB, and a language model. It validates the
immutable fixture catalog, parses the semantic artifacts, and executes a representative SPARQL
query against an in-memory RDFLib graph.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF, SH, SKOS


ROOT = Path(__file__).resolve().parents[1]
ONTOLOGY = ROOT / "ontology" / "sakunagraph.ttl"
TAXONOMY = ROOT / "ontology" / "disaster_type_scheme.ttl"
SHAPES_ROOT = ROOT / "ontology" / "shapes"
BASELINES = ROOT / "sakunagraph_etl" / "tests" / "baselines.json"
GOLDEN_ROOT = BASELINES.parent


def sha256(path: Path) -> str:
    # Git may check text fixtures out with CRLF on Windows. The baseline digest describes the
    # canonical LF representation produced by the RDF jobs, so normalize only line endings.
    payload = path.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(payload).hexdigest()


def named_subject_count(graph: Graph, rdf_type: URIRef) -> int:
    return len(
        {
            subject
            for subject in graph.subjects(RDF.type, rdf_type)
            if isinstance(subject, URIRef)
        }
    )


def load_verified_fixtures() -> tuple[Graph, list[str]]:
    catalog = json.loads(BASELINES.read_text(encoding="utf-8"))
    combined = Graph()
    verified: list[str] = []

    for source, record in sorted(catalog.items()):
        fixture = GOLDEN_ROOT / record["fixture"]
        actual_digest = sha256(fixture)
        if actual_digest != record["sha256"]:
            raise RuntimeError(
                f"Fixture digest mismatch for {source}: "
                f"expected {record['sha256']}, found {actual_digest}"
            )

        fixture_graph = Graph().parse(fixture, format="nt")
        if len(fixture_graph) != record["triple_count"]:
            raise RuntimeError(
                f"Fixture triple-count mismatch for {source}: "
                f"expected {record['triple_count']}, found {len(fixture_graph)}"
            )

        combined += fixture_graph
        verified.append(source)

    return combined, verified


def competency_question_count() -> int:
    path = ROOT / "ontology" / "validation" / "competency_questions.md"
    lines = path.read_text(encoding="utf-8").splitlines()
    return sum(1 for line in lines if line.startswith("**CQ"))


def source_name(event: URIRef) -> str:
    parts = str(event).removeprefix("https://sakuna.ph/").split("/")
    return parts[0] if len(parts) > 1 else "reference"


def main() -> None:
    ontology = Graph().parse(ONTOLOGY, format="turtle")
    taxonomy = Graph().parse(TAXONOMY, format="turtle")
    shapes = Graph()
    for path in sorted(SHAPES_ROOT.rglob("*.ttl")):
        shapes.parse(path, format="turtle")

    fixture_graph, verified_sources = load_verified_fixtures()

    rows = list(
        fixture_graph.query(
            """
            PREFIX skg: <https://sakuna.ph/>

            SELECT ?event ?kind ?name ?disasterType
            WHERE {
              VALUES ?kind { skg:MajorEvent skg:Incident }
              ?event a ?kind ;
                     skg:eventName ?name ;
                     skg:hasDisasterType ?disasterType .
            }
            ORDER BY ?event
            """
        )
    )

    print("SakunaGraPH portfolio smoke demo")
    print("================================")
    print(f"Verified source fixtures : {len(verified_sources)} ({', '.join(verified_sources)})")
    print(f"Combined fixture triples : {len(fixture_graph)}")
    print(f"Named OWL classes        : {named_subject_count(ontology, OWL.Class)}")
    print(f"OWL object properties    : {named_subject_count(ontology, OWL.ObjectProperty)}")
    print(f"OWL datatype properties  : {named_subject_count(ontology, OWL.DatatypeProperty)}")
    print(f"SKOS disaster concepts   : {named_subject_count(taxonomy, SKOS.Concept)}")
    print(f"SHACL node shapes        : {len(set(shapes.subjects(RDF.type, SH.NodeShape)))}")
    print(f"SHACL property shapes    : {len(set(shapes.subjects(RDF.type, SH.PropertyShape)))}")
    print(f"Competency questions     : {competency_question_count()}")
    print()
    print("SPARQL result: source | kind | event | disaster type")
    print("----------------------------------------------------")
    for event, kind, name, disaster_type in rows:
        print(
            f"{source_name(event):7} | {str(kind).rsplit('/', 1)[-1]:10} | "
            f"{str(name):15} | {str(disaster_type).rsplit('/', 1)[-1]}"
        )

    if len(rows) != 4:
        raise RuntimeError(f"Expected four fixture events, found {len(rows)}")

    print()
    print("PASS: semantic artifacts parsed, fixture evidence verified, and SPARQL executed.")


if __name__ == "__main__":
    main()


import logging
from rdflib import URIRef

from mappings.emdat import AffectedPopulation, Assistance, Casualties, DamageGeneral, Event, Recovery, aff_pop_mapping, assistance_mapping, casualties_mapping, damage_gen_mapping, event_mapping, recovery_mapping, source_mapping
from mappings.graph import create_graph, Graph
from transform.emdat import transform_emdat, load_source
import os
import argparse

log = logging.getLogger(__name__)

DATA_DIR = "../data/raw/emdat"
OUT_DIR = "../data/rdf/events/"


def _run_mapping(name: str, fn, entities, cls, g: Graph, *extra) -> None:
    rows = entities.get(cls, [])
    log.info("→ %s: %d rows", name, len(rows))
    fn(rows, g, *extra)
    log.info("✓ %s done", name)


def process_event(input_path: str, g: Graph, src_uri: URIRef):
    log.info("Transforming EM-DAT source: %s", input_path)
    entities = transform_emdat(input_path)
    log.info(
        "Transform produced %d entity classes (total rows: %d)",
        len(entities),
        sum(len(v) for v in entities.values()),
    )

    _run_mapping("event_mapping", event_mapping, entities, Event, g, src_uri)
    _run_mapping("assistance_mapping", assistance_mapping, entities, Assistance, g)
    _run_mapping("recovery_mapping", recovery_mapping, entities, Recovery, g)
    _run_mapping("damage_gen_mapping", damage_gen_mapping, entities, DamageGeneral, g)
    _run_mapping("casualties_mapping", casualties_mapping, entities, Casualties, g)
    _run_mapping("aff_pop_mapping", aff_pop_mapping, entities, AffectedPopulation, g)


def run(out_file: str):
    log.info("=== EM-DAT pipeline start ===")
    log.info("Step 1/5: Creating RDF graph")
    g = create_graph()

    log.info("Step 2/5: Locating latest source file in %s", DATA_DIR)
    files = [
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f))
    ]

    if not files:
        log.error("No files found in directory: %s", DATA_DIR)
        raise FileNotFoundError(f"No files found in directory: {DATA_DIR}")

    path = max(files, key=os.path.getmtime)
    log.info("Selected latest file: %s", path)

    log.info("Step 3/5: Loading source metadata")
    src = load_source(path)
    src_uri = source_mapping(g, src)
    log.info("Source IRI: %s", src_uri)

    log.info("Step 4/5: Processing EM-DAT events")
    process_event(path, g, src_uri)
    log.info("Graph now contains %d triples", len(g))

    log.info("Step 5/5: Serializing graph to %s", OUT_DIR + out_file)
    g.serialize(
        destination=OUT_DIR + out_file,
        format="turtle"
    )
    log.info("=== EM-DAT pipeline complete: %s ===", OUT_DIR + out_file)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="emdat.ttl")
    args = parser.parse_args()

    run(args.out)
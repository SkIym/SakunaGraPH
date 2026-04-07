import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from mappings.graph import create_graph, Graph
from transform.ndrrmc import load_aff_pop, load_agri, load_airport, load_casualties, load_class_suspension, load_comms, load_docalamity, load_events, load_flight, load_housing, load_infra, load_pevac, load_power, load_relief, load_rnb, load_seaport, load_stranded_events, load_incidents, load_provenance, load_water, load_work_suspension
from mappings.ndrrmc import Event, aff_pop_mapping, agri_mapping, airport_mapping, casualties_mapping, class_mapping, comms_mapping, doc_mapping, event_mapping, flight_mapping, housing_mapping, incident_mapping, infra_mapping, pevac_mapping, power_mapping, prov_mapping, relief_mapping, rnb_mapping, seaport_mapping, stranded_mapping, water_mapping, work_mapping
import argparse
import os
from typing import Tuple

log = logging.getLogger(__name__)


def _run_loader_and_mapping(
    name: str,
    loader,
    mapper,
    event_folder: str,
    g: Graph,
    event_iri,
) -> None:
    log.info("→ %s: loading from %s", name, event_folder)
    rows = loader(event_folder)
    if rows:
        count = len(rows) if hasattr(rows, "__len__") else "?"
        log.info("  %s loaded %s rows; mapping...", name, count)
        mapper(g, rows, event_iri)
        log.info("  ✓ %s done", name)
    else:
        log.info("  %s: no rows, skipping", name)


def process_event(args: Tuple[str, Event]) -> Graph:
    # ProcessPoolExecutor workers don't inherit root logger config — set it here.
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s — %(levelname)s — %(name)s — %(message)s",
        )

    DATA_DIR, ev = args
    log.info("Processing event: %s (%s)", ev.eventName, ev.id)

    g = create_graph()

    log.info("→ event_mapping: %s", ev.eventName)
    event_iri = event_mapping(g, ev)
    log.info("  event IRI: %s", event_iri)

    event_folder = os.path.join(DATA_DIR, ev.eventName)

    log.info("→ load_provenance: loading from %s", event_folder)
    prov = load_provenance(event_folder)
    if prov:
        log.info("  provenance loaded; mapping...")
        prov_mapping(g, prov, event_iri)
        log.info("  ✓ prov_mapping done")
    else:
        log.info("  load_provenance: no rows, skipping")

    _run_loader_and_mapping("incidents", load_incidents, incident_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("aff_pop", load_aff_pop, aff_pop_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("casualties", load_casualties, casualties_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("relief", load_relief, relief_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("infra", load_infra, infra_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("housing", load_housing, housing_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("agri", load_agri, agri_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("pevac", load_pevac, pevac_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("rnb", load_rnb, rnb_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("power", load_power, power_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("comms", load_comms, comms_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("doc_calamity", load_docalamity, doc_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("class_suspension", load_class_suspension, class_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("work_suspension", load_work_suspension, work_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("stranded", load_stranded_events, stranded_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("water", load_water, water_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("seaport", load_seaport, seaport_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("airport", load_airport, airport_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("flight", load_flight, flight_mapping, event_folder, g, event_iri)

    log.info("Finished event %s — %d triples", ev.eventName, len(g))
    return g


DATA_DIR = "../data/parsed/ndrrmc"
OUT_DIR = "../data/rdf/events/"


def run(out_file: str, start: int = 0, count: int | None = None):
    log.info("=== NDRRMC pipeline batch start ===")
    log.info("Step 1/4: Creating main graph")
    main_graph = create_graph()

    log.info("Step 2/4: Loading event index from %s", DATA_DIR)
    events = load_events(DATA_DIR)
    log.info("Loaded %d events from index", len(events))

    # ---- batching logic ----
    if count is None:
        batch = events[start:]
    else:
        batch = events[start:start + count]

    log.info("Step 3/4: Processing events %d → %d (batch size %d)", start, start + len(batch), len(batch))

    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_event, (DATA_DIR, ev))
            for ev in batch
        ]

        completed = 0
        for future in as_completed(futures):
            subgraph = future.result()
            main_graph += subgraph
            completed += 1
            log.info("Merged subgraph %d/%d (main graph now %d triples)", completed, len(batch), len(main_graph))

    log.info("Step 4/4: Serializing main graph to %s", out_file)
    main_graph.serialize(
        destination=out_file,
        format="turtle"
    )
    log.info("=== NDRRMC batch complete: %d events → %s ===", len(batch), out_file)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="ndrrmc")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--count", type=int, default=10)
    args = parser.parse_args()

    index = 1
    file_conv = 0
    file_no = 89
    while file_conv < file_no:
        log.info("Starting batch %d (offset %d)", index, args.start + file_conv)
        run(out_file=OUT_DIR + args.out + "-" + str(index) + ".ttl", start=args.start + file_conv, count=args.count)

        index += 1
        file_conv += 10
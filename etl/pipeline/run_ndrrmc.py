from concurrent.futures import ProcessPoolExecutor, as_completed
from mappings.graph import create_graph, Graph
from mappers.ndrrmc_mapper import load_events, load_uuids
from mappings.ndrrmc_mappings import Event, event_mapping, prov_mapping
from mappers.ndrrmc_mapper import load_incidents, load_provenance
import os
from typing import Tuple

def process_event(args: Tuple[str, Event]) -> Graph:
    DATA_DIR, ev = args

    g = create_graph()

    event_iri = event_mapping(g, ev)

    event_folder = os.path.join(DATA_DIR, ev.eventName)

    prov = load_provenance(event_folder)
    if prov:
        prov_mapping(g, prov, event_iri)

    load_incidents(event_folder)

    return g

DATA_DIR = "./data/ndrrmc"
OUT_FILE = "./triples/ndrrmc.ttl"


def run():
    # Base graph
    main_graph = create_graph()

    # UUIDs must be done once
    load_uuids(DATA_DIR)

    events = load_events(DATA_DIR)

    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_event, (DATA_DIR, ev))
            for ev in events
        ]

        for future in as_completed(futures):
            subgraph = future.result()
            main_graph += subgraph  # safe merge

    main_graph.serialize(
        destination=OUT_FILE,
        format="turtle"
    )

    print(f"Serialized {len(events)} events → {OUT_FILE}")


if __name__ == "__main__":
    run()
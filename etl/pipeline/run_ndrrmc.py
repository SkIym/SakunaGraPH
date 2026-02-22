from concurrent.futures import ProcessPoolExecutor, as_completed
from mappings.graph import create_graph, Graph
from transform.ndrrmc_mapper import load_aff_pop, load_casualties, load_events, load_uuids
from mappings.ndrrmc_mappings import Event, aff_pop_mapping, casualties_mapping, event_mapping, incident_mapping, prov_mapping
from transform.ndrrmc_mapper import load_incidents, load_provenance
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

    inci = load_incidents(event_folder)
    if inci:
        incident_mapping(g, inci, event_iri)

    
    aff_pop = load_aff_pop(event_folder)
    if aff_pop:
        aff_pop_mapping(g, aff_pop, event_iri)
    
    casualties = load_casualties(event_folder)
    if casualties:
        casualties_mapping(g, casualties, event_iri)

    return g

DATA_DIR = "../data/parsed/ndrrmc_mini"
OUT_FILE = "../data/rdf/ndrrmc_mini.ttl"


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
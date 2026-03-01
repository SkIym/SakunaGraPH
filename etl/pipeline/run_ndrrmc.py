from concurrent.futures import ProcessPoolExecutor, as_completed
from mappings.graph import create_graph, Graph
from transform.ndrrmc import load_aff_pop, load_agri, load_casualties, load_class_suspension, load_comms, load_docalamity, load_events, load_housing, load_infra, load_pevac, load_power, load_relief, load_rnb, load_stranded_events, load_uuids, load_incidents, load_provenance, load_water, load_work_suspension
from mappings.ndrrmc import Event, aff_pop_mapping, agri_mapping, casualties_mapping, class_mapping, comms_mapping, doc_mapping, event_mapping, housing_mapping, incident_mapping, infra_mapping, pevac_mapping, power_mapping, prov_mapping, relief_mapping, rnb_mapping, stranded_mapping, water_mapping, work_mapping

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

    relief = load_relief(event_folder)
    if relief:
        relief_mapping(g, relief, event_iri)

    infra = load_infra(event_folder)
    if infra:
        infra_mapping(g, infra, event_iri)

    housing = load_housing(event_folder)
    if housing:
        housing_mapping(g, housing, event_iri)

    agri = load_agri(event_folder)
    if agri:
        agri_mapping(g, agri, event_iri)

    pevac = load_pevac(event_folder)
    if pevac:
        pevac_mapping(g, pevac, event_iri)

    rnb = load_rnb(event_folder)
    if rnb:
        rnb_mapping(g, rnb, event_iri)
    
    power = load_power(event_folder)
    if power:
        power_mapping(g, power, event_iri)

    comms = load_comms(event_folder)
    if comms:
        comms_mapping(g, comms, event_iri)

    doc = load_docalamity(event_folder)
    if doc:
        doc_mapping(g, doc, event_iri)

    class_sus = load_class_suspension(event_folder)
    if class_sus:
        class_mapping(g, class_sus, event_iri)

    work_sus = load_work_suspension(event_folder)
    if work_sus:
        work_mapping(g, work_sus, event_iri)

    stranded = load_stranded_events(event_folder)
    if stranded:
        stranded_mapping(g, stranded, event_iri)


    water = load_water(event_folder)
    if water:    
        water_mapping(g, water, event_iri)

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
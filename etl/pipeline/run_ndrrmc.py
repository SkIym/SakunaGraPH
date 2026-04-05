from concurrent.futures import ProcessPoolExecutor, as_completed
from mappings.graph import create_graph, Graph
from transform.ndrrmc import load_aff_pop, load_agri, load_airport, load_casualties, load_class_suspension, load_comms, load_docalamity, load_events, load_flight, load_housing, load_infra, load_pevac, load_power, load_relief, load_rnb, load_seaport, load_stranded_events, load_incidents, load_provenance, load_water, load_work_suspension
from mappings.ndrrmc import Event, aff_pop_mapping, agri_mapping, airport_mapping, casualties_mapping, class_mapping, comms_mapping, doc_mapping, event_mapping, flight_mapping, housing_mapping, incident_mapping, infra_mapping, pevac_mapping, power_mapping, prov_mapping, relief_mapping, rnb_mapping, seaport_mapping, stranded_mapping, water_mapping, work_mapping
import argparse
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

    seaport = load_seaport(event_folder)
    if seaport:
        seaport_mapping(g, seaport, event_iri)

    airport = load_airport(event_folder)
    if airport:
        airport_mapping(g, airport, event_iri)

    flight = load_flight(event_folder)
    if flight:
        flight_mapping(g, flight, event_iri)

    return g


DATA_DIR = "../data/parsed/ndrrmc"
OUT_DIR = "../data/rdf/events/"


def run(out_file: str, start: int = 0, count: int | None = None):
    main_graph = create_graph()

    events = load_events(DATA_DIR)

    # ---- batching logic ----
    if count is None:
        batch = events[start:]
    else:
        batch = events[start:start + count]

    print(f"Processing events {start} → {start + len(batch)}")

    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_event, (DATA_DIR, ev))
            for ev in batch
        ]

        for future in as_completed(futures):
            subgraph = future.result()
            main_graph += subgraph

    main_graph.serialize(
        destination=out_file,
        format="turtle"
    )

    print(f"Serialized {len(batch)} events → {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="ndrrmc")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--count", type=int, default=10)
    args = parser.parse_args()


    index = 1
    file_conv = 0
    file_no = 89
    while file_conv < file_no:

        run(out_file=OUT_DIR+args.out+"-"+str(index)+".ttl", start=args.start+file_conv, count=args.count, )

        index+=1
        file_conv += 10
    # run()
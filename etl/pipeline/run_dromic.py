import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple

from rdflib import Graph

from mappings.dromic import Event, aff_pop_mapping, event_mapping, housing_mapping, prov_mapping
from transform.dromic import load_aff_pop, load_event, load_housing, load_provenance
from mappings.graph import create_graph
import os
import time

DATA_DIR = "../data/parsed/dromic/"
OUT_DIR = "../data/rdf/events/"

def process_event(folder_path: str) -> Graph:

    g = create_graph()
    ev = load_event(os.path.join(folder_path, "metadata.json"))
    # print(f"Processing event {ev.eventName}")

    event_iri = event_mapping(g, ev)

    prov = load_provenance(os.path.join(folder_path, "source.json"))
    
    prov_mapping(g, prov, event_iri)

    aps = load_aff_pop(folder_path)
    if aps:
        aff_pop_mapping(g, aps, event_iri)

    hs = load_housing(folder_path)
    if hs:
        housing_mapping(g, hs, event_iri)

    return g


def run(sub_data_dir: str, out_file: str):
    main_graph = create_graph()

    ec = 0
    for folder in next(os.walk(sub_data_dir))[1]:
        folder_path = os.path.join(sub_data_dir, folder)
        main_graph += process_event(folder_path)

        ec += 1

    main_graph.serialize(destination=out_file, format="turtle")
    print(f"Serialized {ec} events → {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=str, required=True)
    parser.add_argument("--out", type=str, default="dromic")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--count", type=int, default=50)
    args = parser.parse_args()

    sub_data_dir = DATA_DIR + str(args.year)

    run(
        sub_data_dir=sub_data_dir,
        out_file=f"{OUT_DIR}{args.out}-{args.year}.ttl",
    )

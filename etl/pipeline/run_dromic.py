import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple

from rdflib import Graph

from mappings.dromic import Event, event_mapping
from transform.dromic import load_events
from mappings.graph import create_graph
import os
import time

DATA_DIR = "../data/parsed/dromic/"
OUT_DIR = "../data/rdf/events/"

def process_event(args: Tuple[str, Event]) -> Graph:
    data_dir, ev = args
    g = create_graph()
    event_mapping(g, ev)

    return g


def run(events: List[Event],sub_data_dir: str, out_file: str, start: int = 0, count: int | None = None):
    main_graph = create_graph()

    batch = events[start:] if count is None else events[start:start + count]

    print(f"Processing events {start} → {start + len(batch)}")

    for ev in batch:
        main_graph += process_event((sub_data_dir, ev))

    # with ProcessPoolExecutor() as executor:
    #     futures = [
    #         executor.submit(process_event, (sub_data_dir, ev))
    #         for ev in batch
    #     ]
    #     for future in as_completed(futures):
    #         main_graph += future.result()

    main_graph.serialize(destination=out_file, format="turtle")
    print(f"Serialized {len(batch)} events → {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=str)
    parser.add_argument("--out", type=str, default="dromic")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--count", type=int, default=50)
    args = parser.parse_args()

    sub_data_dir = DATA_DIR + str(args.year)
    file_no = len(os.listdir(sub_data_dir))

    events = load_events(sub_data_dir)

    index = 1
    file_conv = 0

    while file_conv < file_no:
        run(
            events=events,
            sub_data_dir=sub_data_dir,
            out_file=f"{OUT_DIR}{args.out}-{args.year}-{index}.ttl",
            start=file_conv,  # no longer adding args.start here
            count=args.count,
        )
        index += 1
        file_conv += args.count
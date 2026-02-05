from mappings.graph import create_graph
from mappings.ndrrmc_mappings import event_mapping, prov_mapping
from mappers.ndrrmc_mapper import load_events, load_uuids, load_provenance
import os

DATA_DIR = "./data/ndrrmc"
OUT_FILE = "./triples/ndrrmc.ttl"


def run():
    g = create_graph()

    # Load uuids to each event
    load_uuids(DATA_DIR)

    events = load_events(DATA_DIR)


    for ev in events:
        event_iri = event_mapping(g, ev)

        event_folder = os.path.join(DATA_DIR, ev.eventName)
        prov = load_provenance(event_folder)

        if prov:
            prov_mapping(g, prov, event_iri)

    g.serialize(
        destination=OUT_FILE,
        format="turtle"
    )

    print(f"Serialized {len(events)} events → {OUT_FILE}")


if __name__ == "__main__":
    run()

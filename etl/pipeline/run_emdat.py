
from rdflib import URIRef

from mappings.emdat import AffectedPopulation, Assistance, Casualties, DamageGeneral, Event, Recovery, aff_pop_mapping, assistance_mapping, casualties_mapping, damage_gen_mapping, event_mapping, recovery_mapping, source_mapping
from mappings.graph import create_graph, Graph
from transform.emdat import transform_emdat, load_source
import os
import argparse

DATA_DIR = "../data/raw/emdat"
OUT_DIR = "../data/rdf/events/"

def process_event(input_path: str, g: Graph, src_uri: URIRef):
    
    entities = transform_emdat(input_path)

    event_mapping(entities[Event], g, src_uri)
    assistance_mapping(entities[Assistance], g)
    recovery_mapping(entities[Recovery], g)
    damage_gen_mapping(entities[DamageGeneral], g)
    casualties_mapping(entities[Casualties], g)
    aff_pop_mapping(entities[AffectedPopulation], g)


def run(out_file: str):
    g = create_graph()

    # find latest file
    files = [
        os.path.join(DATA_DIR, f) 
        for f in os.listdir(DATA_DIR) 
        if os.path.isfile(os.path.join(DATA_DIR, f))
    ]
    
    if not files:
        raise FileNotFoundError(f"No files found in directory: {DATA_DIR}")

    path = max(files, key=os.path.getmtime)

    src = load_source(path)
    src_uri = source_mapping(g, src)

    process_event(path, g, src_uri)

    g.serialize(
        destination=OUT_DIR+out_file,
        format="turtle"
    )

    # print(f"Serialized {len(batch)} events → {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="emdat.ttl")
    args = parser.parse_args()

    run(args.out)
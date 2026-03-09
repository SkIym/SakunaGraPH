
from mappings.emdat import source_mapping
from mappings.graph import create_graph
from transform.emdat import load_source
import os
import argparse

DATA_DIR = "../data/raw/emdat"
OUT_DIR = "../data/rdf/"

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
    source_mapping(g, src)

    # events = load_events(DATA_DIR)

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
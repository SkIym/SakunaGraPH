
from rdflib import URIRef
from mappings.gda_mapping import (
    Assistance, AffectedPopulation, Casualties, CommunicationLineDisruption,
    DeclarationOfCalamity, DamageGeneral, Evacuation, Event, HousingDamage,
    Incident, InfrastructureDamage, PowerDisruption, Preparedness, Recovery,
    Relief, Rescue, RoadAndBridgesDamage, SeaportDisruption,
    WaterDisruption, evacuation_mapping, event_mapping, preparedness_mapping
)

from mappings.graph import SKG, create_graph, Graph
from transform.geog_archive import transform_gda
import os
import argparse

DATA_DIR = "../data/raw/static/"
OUT_DIR = "../data/rdf/"

def process_event(input_path: str, g: Graph, src_uri: URIRef):
    
    entities = transform_gda(input_path)

    event_mapping(entities[Event], g, src_uri)
    preparedness_mapping(entities[Preparedness], g)
    evacuation_mapping(entities[Evacuation], g)


def run(out_file: str):
    g = create_graph()
    src_uri = URIRef(SKG.GDA)

    process_event(DATA_DIR + "geog-archive-cleaned.xlsx", g, src_uri)

    g.serialize(
        destination=OUT_DIR+out_file,
        format="turtle"
    )

    # print(f"Serialized {len(batch)} events → {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="gda.ttl")
    args = parser.parse_args()

    run(args.out)

from rdflib import URIRef
from mappings.gda_mapping import (
    Assistance, AffectedPopulation, Casualties, CommunicationLineDisruption,
    DeclarationOfCalamity, DamageGeneral, Evacuation, Event, HousingDamage,
    Incident, InfrastructureDamage, PowerDisruption, Preparedness, Recovery, Relief,
    Rescue, RoadAndBridgesDamage, SeaportDisruption,
    WaterDisruption, aff_pop_mapping, assistance_mapping, calamity_mapping, casualties_mapping, comms_disruption_mapping, damage_gen_mapping, evacuation_mapping, event_mapping, housing_damage_mapping, incident_mapping, infra_damage_mapping, power_disruption_mapping, preparedness_mapping, recovery_mapping, relief_mapping, rescue_mapping, rnb_damage_mapping, seaport_disruption_mapping, water_disruption_mapping
)

from mappings.graph import SKG, create_graph, Graph
from transform.gda import transform_gda
import os
import argparse

DATA_DIR = "../data/raw/static/"
OUT_DIR = "../data/rdf/"

def process_event(input_path: str, g: Graph, src_uri: URIRef):
    
    entities = transform_gda(input_path)

    event_mapping(entities[Event], g, src_uri)
    incident_mapping(entities[Incident], g)
    preparedness_mapping(entities[Preparedness], g)
    evacuation_mapping(entities[Evacuation], g)
    rescue_mapping(entities[Rescue], g)
    calamity_mapping(entities[DeclarationOfCalamity], g)
    aff_pop_mapping(entities[AffectedPopulation], g)
    casualties_mapping(entities[Casualties], g)
    housing_damage_mapping(entities[HousingDamage], g)
    infra_damage_mapping(entities[InfrastructureDamage], g)
    damage_gen_mapping(entities[DamageGeneral], g)
    power_disruption_mapping(entities[PowerDisruption], g)
    comms_disruption_mapping(entities[CommunicationLineDisruption], g)
    rnb_damage_mapping(entities[RoadAndBridgesDamage], g)
    seaport_disruption_mapping(entities[SeaportDisruption], g)
    water_disruption_mapping(entities[WaterDisruption], g)
    assistance_mapping(entities[Assistance], g)
    relief_mapping(entities[Relief], g)
    recovery_mapping(entities[Recovery], g)
    

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
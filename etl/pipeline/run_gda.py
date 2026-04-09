
import logging
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

log = logging.getLogger(__name__)

DATA_DIR = "../data/raw/static/"
OUT_DIR = "../data/rdf/events/"


def _run_mapping(name: str, fn, entities, cls, g: Graph, *extra) -> None:
    rows = entities.get(cls, [])
    log.info("→ %s: %d rows", name, len(rows))
    fn(rows, g, *extra)
    log.info("✓ %s done", name)


def process_event(input_path: str, g: Graph, src_uri: URIRef):
    log.info("Transforming GDA source: %s", input_path)
    entities = transform_gda(input_path)
    log.info(
        "Transform produced %d entity classes (total rows: %d)",
        len(entities),
        sum(len(v) for v in entities.values()),
    )

    _run_mapping("event_mapping", event_mapping, entities, Event, g, src_uri)
    _run_mapping("incident_mapping", incident_mapping, entities, Incident, g)
    _run_mapping("preparedness_mapping", preparedness_mapping, entities, Preparedness, g)
    _run_mapping("evacuation_mapping", evacuation_mapping, entities, Evacuation, g)
    _run_mapping("rescue_mapping", rescue_mapping, entities, Rescue, g)
    _run_mapping("calamity_mapping", calamity_mapping, entities, DeclarationOfCalamity, g)
    _run_mapping("aff_pop_mapping", aff_pop_mapping, entities, AffectedPopulation, g)
    _run_mapping("casualties_mapping", casualties_mapping, entities, Casualties, g)
    _run_mapping("housing_damage_mapping", housing_damage_mapping, entities, HousingDamage, g)
    _run_mapping("infra_damage_mapping", infra_damage_mapping, entities, InfrastructureDamage, g)
    _run_mapping("damage_gen_mapping", damage_gen_mapping, entities, DamageGeneral, g)
    _run_mapping("power_disruption_mapping", power_disruption_mapping, entities, PowerDisruption, g)
    _run_mapping("comms_disruption_mapping", comms_disruption_mapping, entities, CommunicationLineDisruption, g)
    _run_mapping("rnb_damage_mapping", rnb_damage_mapping, entities, RoadAndBridgesDamage, g)
    _run_mapping("seaport_disruption_mapping", seaport_disruption_mapping, entities, SeaportDisruption, g)
    _run_mapping("water_disruption_mapping", water_disruption_mapping, entities, WaterDisruption, g)
    _run_mapping("assistance_mapping", assistance_mapping, entities, Assistance, g)
    _run_mapping("relief_mapping", relief_mapping, entities, Relief, g)
    _run_mapping("recovery_mapping", recovery_mapping, entities, Recovery, g)


def run(out_file: str):
    log.info("=== GDA pipeline start ===")
    log.info("Step 1/3: Creating RDF graph")
    g = create_graph()
    src_uri = URIRef(SKG.GDA)

    log.info("Step 2/3: Processing GDA events")
    process_event(DATA_DIR + "geog-archive-cleaned.xlsx", g, src_uri)
    log.info("Graph now contains %d triples", len(g))

    log.info("Step 3/3: Serializing graph to %s", OUT_DIR + out_file)
    g.serialize(
        destination=OUT_DIR + out_file,
        format="turtle"
    )
    log.info("=== GDA pipeline complete: %s ===", OUT_DIR + out_file)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="gda.ttl")
    args = parser.parse_args()

    run(args.out)
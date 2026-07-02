import argparse
import logging
from typing import Any, Iterable

from rdflib import URIRef

from mappings.gda import (
    Assistance,
    AffectedPopulation,
    Casualties,
    CommunicationLineDisruption,
    DamageGeneral,
    DeclarationOfCalamity,
    Evacuation,
    Event,
    HousingDamage,
    Incident,
    InfrastructureDamage,
    PowerDisruption,
    Preparedness,
    Recovery,
    Relief,
    Rescue,
    RoadAndBridgesDamage,
    SeaportDisruption,
    WaterDisruption,
    aff_pop_mapping,
    assistance_mapping,
    calamity_mapping,
    casualties_mapping,
    comms_disruption_mapping,
    damage_gen_mapping,
    evacuation_mapping,
    event_mapping,
    housing_damage_mapping,
    incident_mapping,
    infra_damage_mapping,
    power_disruption_mapping,
    preparedness_mapping,
    recovery_mapping,
    relief_mapping,
    rescue_mapping,
    rnb_damage_mapping,
    seaport_disruption_mapping,
    water_disruption_mapping,
)
from mappings.graph import SKG, Graph, create_graph
from validate.validate import (
    ShaclValidationError,
    ShaclValidator,
    validation_focus_nodes,
)
from transform.gda import transform_gda


log = logging.getLogger(__name__)

DATA_DIR = "../data/raw/static/"
OUT_DIR = "../data/rdf/events/"
DEFAULT_BATCH_SIZE = 100


def _run_mapping(
    name: str,
    fn,
    entities: dict[type, list[Any]],
    cls: type,
    g: Graph,
    *extra,
) -> None:
    rows = entities.get(cls, [])
    log.info("Mapping %s: %d rows", name, len(rows))
    fn(rows, g, *extra)
    log.info("Finished %s", name)


def _load_entities(input_path: str) -> dict[type, list[Any]]:
    log.info("Transforming GDA source: %s", input_path)
    entities = transform_gda(input_path)
    log.info(
        "Transform produced %d entity classes (total rows: %d)",
        len(entities),
        sum(len(v) for v in entities.values()),
    )
    return entities


def _map_entities(entities: dict[type, list[Any]], g: Graph, src_uri: URIRef) -> None:
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
    _run_mapping(
        "comms_disruption_mapping",
        comms_disruption_mapping,
        entities,
        CommunicationLineDisruption,
        g,
    )
    _run_mapping("rnb_damage_mapping", rnb_damage_mapping, entities, RoadAndBridgesDamage, g)
    _run_mapping("seaport_disruption_mapping", seaport_disruption_mapping, entities, SeaportDisruption, g)
    _run_mapping("water_disruption_mapping", water_disruption_mapping, entities, WaterDisruption, g)
    _run_mapping("assistance_mapping", assistance_mapping, entities, Assistance, g)
    _run_mapping("relief_mapping", relief_mapping, entities, Relief, g)
    _run_mapping("recovery_mapping", recovery_mapping, entities, Recovery, g)


def process_event(input_path: str, g: Graph, src_uri: URIRef) -> None:
    entities = _load_entities(input_path)
    _map_entities(entities, g, src_uri)


def process_event_batches(
    input_path: str,
    g: Graph,
    src_uri: URIRef,
    *,
    validator: ShaclValidator,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fail_fast: bool = True,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    entities = _load_entities(input_path)
    event_ids = _event_ids(entities)
    if not event_ids:
        log.warning("No GDA events found for validation")
        return

    failures: list[ShaclValidationError] = []
    batches = list(_batched(event_ids, batch_size))
    for batch_no, batch_ids in enumerate(batches, start=1):
        batch_graph = create_graph()
        batch_entities = _filter_entities_by_event_ids(entities, batch_ids)

        log.info(
            "Processing GDA validation batch %d/%d (%d events)",
            batch_no,
            len(batches),
            len(batch_ids),
        )
        _map_entities(batch_entities, batch_graph, src_uri)

        label = f"GDA batch {batch_no}/{len(batches)}"
        try:
            result = validator.validate_graph(
                batch_graph,
                label=label,
                # focus_nodes=validation_focus_nodes(batch_graph),
                focus_nodes=None
            )
        except ShaclValidationError as exc:
            failures.append(exc)
            log.error("SHACL validation failed for %s", label)
            log.error("Batch event ids: %s", ", ".join(batch_ids))
            log.error(exc.result.results_text)
            if fail_fast:
                raise
            continue

        log.info(
            "SHACL validation passed for %s (%d data triples, %d validation triples)",
            result.label,
            result.data_triples,
            result.validation_triples,
        )
        g += batch_graph

    if failures:
        raise RuntimeError(
            f"SHACL validation failed for {len(failures)} GDA batch(es); see logs."
        )


def _event_ids(entities: dict[type, list[Any]]) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for row in entities.get(Event, []):
        event_id = str(row.id)
        if event_id not in seen:
            ids.append(event_id)
            seen.add(event_id)
    return ids


def _filter_entities_by_event_ids(
    entities: dict[type, list[Any]],
    event_ids: Iterable[str],
) -> dict[type, list[Any]]:
    wanted = set(event_ids)
    return {
        cls: [row for row in rows if str(getattr(row, "id", "")) in wanted]
        for cls, rows in entities.items()
    }


def _batched(values: list[str], batch_size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), batch_size):
        yield values[start : start + batch_size]


def run(
    out_file: str,
    validate_output: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fail_fast: bool = True,
    include_context_graphs: bool = True,
) -> None:
    log.info("=== GDA pipeline start ===")
    log.info("Step 1/3: Creating RDF graph")
    g = create_graph()
    src_uri = URIRef(SKG.GDA)
    input_path = DATA_DIR + "geog-archive-cleaned.xlsx"

    if validate_output:
        log.info(
            "Step 2/3: Processing GDA events with SHACL validation batches of %d",
            batch_size,
        )
        if not include_context_graphs:
            log.info("SHACL validation context graphs are disabled")
        validator = ShaclValidator.from_paths(
            include_context_graphs=include_context_graphs,
            ontology_graph=None,
        )
        process_event_batches(
            input_path,
            g,
            src_uri,
            validator=validator,
            batch_size=batch_size,
            fail_fast=fail_fast,
        )
    else:
        log.info("Step 2/3: Processing GDA events")
        process_event(input_path, g, src_uri)

    log.info("Graph now contains %d triples", len(g))
    log.info("Step 3/3: Serializing graph to %s", OUT_DIR + out_file)
    g.serialize(destination=OUT_DIR + out_file, format="turtle")
    log.info("=== GDA pipeline complete: %s ===", OUT_DIR + out_file)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="gda.ttl")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run batch SHACL validation before serializing the GDA graph.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of GDA event ids per validation batch.",
    )
    parser.add_argument(
        "--no-fail-fast",
        action="store_true",
        help="Validate all batches before failing on SHACL errors.",
    )
    parser.add_argument(
        "--no-context",
        action="store_true",
        help="Run SHACL validation without PSGC, disaster-type, organization, or provenance context graphs.",
    )
    args = parser.parse_args()

    run(
        args.out,
        validate_output=args.validate,
        batch_size=args.batch_size,
        fail_fast=not args.no_fail_fast,
        include_context_graphs=not args.no_context,
    )

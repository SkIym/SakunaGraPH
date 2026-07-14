import argparse
import logging
import os
from typing import Any, Iterable

from rdflib import URIRef

from mappings.emdat import (
    AffectedPopulation,
    Assistance,
    Casualties,
    DamageGeneral,
    Event,
    Recovery,
    aff_pop_mapping,
    assistance_mapping,
    casualties_mapping,
    damage_gen_mapping,
    event_mapping,
    recovery_mapping,
    source_mapping,
)
from mappings.graph import Graph, create_graph
from transform.emdat import load_source, transform_emdat
from validate.validate import (
    ShaclValidationError,
    ShaclValidator,
    validation_focus_nodes,
)


log = logging.getLogger(__name__)

DATA_DIR = "../data/raw/emdat"
OUT_DIR = "../data/rdf/events/emdat"
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
    log.info("Transforming EM-DAT source: %s", input_path)
    entities = transform_emdat(input_path)
    log.info(
        "Transform produced %d entity classes (total rows: %d)",
        len(entities),
        sum(len(v) for v in entities.values()),
    )
    return entities


def _map_entities(entities: dict[type, list[Any]], g: Graph, src_uri: URIRef) -> None:
    _run_mapping("event_mapping", event_mapping, entities, Event, g, src_uri)
    _run_mapping("assistance_mapping", assistance_mapping, entities, Assistance, g)
    _run_mapping("recovery_mapping", recovery_mapping, entities, Recovery, g)
    _run_mapping("damage_gen_mapping", damage_gen_mapping, entities, DamageGeneral, g)
    _run_mapping("casualties_mapping", casualties_mapping, entities, Casualties, g)
    _run_mapping("aff_pop_mapping", aff_pop_mapping, entities, AffectedPopulation, g)


def process_event(input_path: str, g: Graph, src_uri: URIRef) -> None:
    entities = _load_entities(input_path)
    _map_entities(entities, g, src_uri)


def validate_mapped_graph(
    g: Graph,
    *,
    validator: ShaclValidator,
    label: str,
) -> None:
    try:
        result = validator.validate_graph(
            g,
            label=label,
            focus_nodes=validation_focus_nodes(g),
        )
    except ShaclValidationError as exc:
        log.error("SHACL validation failed for %s", label)
        log.error(exc.result.results_text)
        raise

    log.info(
        "SHACL validation passed for %s (%d data triples, %d validation triples)",
        result.label,
        result.data_triples,
        result.validation_triples,
    )


def process_event_batches(
    entities: dict[type, list[Any]],
    g: Graph,
    src_uri: URIRef,
    *,
    validator: ShaclValidator,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fail_fast: bool = True,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    event_ids = _event_ids(entities)
    if not event_ids:
        log.warning("No EM-DAT events found for validation")
        return

    failures: list[ShaclValidationError] = []
    batches = list(_batched(event_ids, batch_size))
    source_triples = tuple(g.triples((src_uri, None, None)))
    for batch_no, batch_ids in enumerate(batches, start=1):
        batch_graph = create_graph()
        for triple in source_triples:
            batch_graph.add(triple)

        batch_entities = _filter_entities_by_event_ids(entities, batch_ids)

        log.info(
            "Processing EM-DAT validation batch %d/%d (%d events)",
            batch_no,
            len(batches),
            len(batch_ids),
        )
        _map_entities(batch_entities, batch_graph, src_uri)

        label = f"EM-DAT batch {batch_no}/{len(batches)}"
        try:
            result = validator.validate_graph(
                batch_graph,
                label=label,
                focus_nodes=validation_focus_nodes(batch_graph),
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
            f"SHACL validation failed for {len(failures)} EM-DAT batch(es); see logs."
        )


def _latest_source_file() -> str:
    log.info("Locating latest source file in %s", DATA_DIR)
    files = [
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f))
    ]

    if not files:
        log.error("No files found in directory: %s", DATA_DIR)
        raise FileNotFoundError(f"No files found in directory: {DATA_DIR}")

    path = max(files, key=os.path.getmtime)
    log.info("Selected latest file: %s", path)
    return path


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
    validate_by_batch: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fail_fast: bool = True,
    include_context_graphs: bool = True,
) -> None:
    log.info("=== EM-DAT pipeline start ===")
    log.info("Step 1/5: Creating RDF graph")
    g = create_graph()

    log.info("Step 2/5: Locating latest source file")
    path = _latest_source_file()

    log.info("Step 3/5: Loading source metadata")
    src = load_source(path)
    src_uri = source_mapping(g, src)
    log.info("Source IRI: %s", src_uri)

    log.info("Step 4/5: Processing EM-DAT events")
    if validate_output and not include_context_graphs:
        log.info("SHACL validation context graphs are disabled")

    if validate_output:
        validator = ShaclValidator.from_paths(
            include_context_graphs=include_context_graphs,
        )

        entities = _load_entities(path)
        if validate_by_batch:
            log.info(
                "Processing EM-DAT events with SHACL validation batches of %d",
                batch_size,
            )
            process_event_batches(
                entities,
                g,
                src_uri,
                validator=validator,
                batch_size=batch_size,
                fail_fast=fail_fast,
            )
        else:
            _map_entities(entities, g, src_uri)
            validate_mapped_graph(
                g,
                validator=validator,
                label="EM-DAT output",
            )
    else:
        process_event(path, g, src_uri)

    log.info("Graph now contains %d triples", len(g))

    log.info("Step 5/5: Serializing graph to %s", OUT_DIR + out_file)
    g.serialize(destination=OUT_DIR + out_file, format="turtle")
    log.info("=== EM-DAT pipeline complete: %s ===", OUT_DIR + out_file)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="emdat.ttl")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run SHACL validation before serializing the EM-DAT graph.",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Validate EM-DAT events in batches instead of validating the full graph.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of EM-DAT event ids per validation batch when --batch is set.",
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
        validate_by_batch=args.batch,
        batch_size=args.batch_size,
        fail_fast=not args.no_fail_fast,
        include_context_graphs=not args.no_context,
    )

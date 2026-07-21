import argparse
import logging
from pathlib import Path
from typing import Any, Iterable

from rdflib import URIRef

from .rdf import (
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
from sakunagraph_etl.rdf.graph import SKG, Graph, create_graph
from sakunagraph_etl.quality.shacl import (
    ShaclValidationError,
    ShaclValidator,
    validation_focus_nodes,
)
from .transform import transform_gda
from sakunagraph_etl.config import PROFILE_CHOICES, SETTINGS, load_settings
from sakunagraph_etl.config import EtlSettings
from sakunagraph_etl.io import (
    ArtifactRunResult,
    Storage,
    local_input_paths_from_manifest,
    record_artifact_run,
)
from sakunagraph_etl.quality.contracts import validate_source_input
from sakunagraph_etl.quality.schemas import QualityPolicy, enforce_production_quality


log = logging.getLogger(__name__)

DATA_DIR = SETTINGS.paths.raw_root / "static"
OUT_DIR = SETTINGS.paths.event_rdf_root / "gda"
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


def _load_entities(
    input_path: str | Path,
    *,
    debug_dir: str | Path | None = None,
) -> dict[type, list[Any]]:
    log.info("Transforming GDA source: %s", input_path)
    entities = transform_gda(input_path, debug_dir=debug_dir)
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


def process_event(
    input_path: str | Path,
    g: Graph,
    src_uri: URIRef,
    *,
    debug_dir: str | Path | None = None,
) -> None:
    entities = _load_entities(input_path, debug_dir=debug_dir)
    _map_entities(entities, g, src_uri)


def process_event_batches(
    input_path: str | Path,
    g: Graph,
    src_uri: URIRef,
    *,
    validator: ShaclValidator,
    debug_dir: str | Path | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fail_fast: bool = True,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    entities = _load_entities(input_path, debug_dir=debug_dir)
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
    *,
    input_path: str | Path | None = None,
    out_dir: str | Path = OUT_DIR,
    debug_dir: str | Path | None = None,
    validate_output: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fail_fast: bool = True,
    include_context_graphs: bool = True,
    input_manifest: str | Path | None = None,
    artifact_storage: Storage | None = None,
    settings: EtlSettings = SETTINGS,
) -> ArtifactRunResult | None:
    log.info("=== GDA pipeline start ===")
    log.info("Step 1/3: Creating RDF graph")
    g = create_graph()
    src_uri = URIRef(SKG.GDA)
    if input_path is not None:
        input_path = Path(input_path)
    elif input_manifest is not None:
        manifest_inputs = local_input_paths_from_manifest(input_manifest)
        workbooks = [path for path in manifest_inputs if path.suffix.lower() in {".xlsx", ".xls"}]
        if len(workbooks) != 1:
            raise ValueError("GDA input manifest must select exactly one workbook")
        input_path = workbooks[0]
    else:
        input_path = DATA_DIR / "geog-archive-cleaned.xlsx"

    quality_report = None
    if input_path.is_file():
        quality_report = validate_source_input(
            "gda",
            input_path,
            policy=QualityPolicy.from_settings(settings),
        )
        enforce_production_quality(quality_report, settings.profile)

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
            debug_dir=debug_dir,
            batch_size=batch_size,
            fail_fast=fail_fast,
        )
    else:
        log.info("Step 2/3: Processing GDA events")
        process_event(input_path, g, src_uri, debug_dir=debug_dir)

    log.info("Graph now contains %d triples", len(g))
    out_path = Path(out_file)
    if not out_path.is_absolute():
        out_path = Path(out_dir) / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Step 3/3: Serializing graph to %s", out_path)
    g.serialize(destination=str(out_path), format="turtle")
    log.info("=== GDA pipeline complete: %s ===", out_path)
    if not input_path.is_file():
        log.warning("Skipping artifact manifest because the mocked input does not exist: %s", input_path)
        return None
    return record_artifact_run(
        "gda",
        input_paths=(input_path,),
        output_paths=(out_path,),
        validation_status="PASSED" if validate_output else "NOT_RUN",
        settings=settings,
        storage=artifact_storage,
        parameters={
            "validate": validate_output,
            "batch_size": batch_size,
            "include_context_graphs": include_context_graphs,
        },
        metadata={
            "triple_count": len(g),
            **({"quality": quality_report.to_dict()} if quality_report is not None else {}),
        },
    )


def build_parser(*, require_input: bool = False) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build SakunaGraPH RDF from GDA.")
    parser.add_argument("--out", type=str, default="gda.ttl")
    parser.add_argument(
        "--input",
        type=Path,
        required=require_input,
        help="Explicit GDA workbook path.",
    )
    parser.add_argument("--out-dir", type=Path, help="RDF output directory.")
    parser.add_argument("--debug-dir", type=Path, help="Optional diagnostic CSV root.")
    parser.add_argument("--input-manifest", type=Path, help="Explicit input run manifest.")
    parser.add_argument("--profile", choices=PROFILE_CHOICES, help="Deployment profile.")
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
        help="Run SHACL validation without external context graphs.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.input is None and args.input_manifest is None:
        parser.error("one of --input or --input-manifest is required")
    return _run_from_args(args)


def compatibility_main(argv: list[str] | None = None) -> int:
    """Retain the historical fixed workbook default for the legacy command."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    return _run_from_args(build_parser().parse_args(argv))


def _run_from_args(args: argparse.Namespace) -> int:
    settings = load_settings(args.profile)

    run(
        args.out,
        input_path=(
            args.input
            if args.input is not None or args.input_manifest is not None
            else settings.paths.raw_root / "static" / "geog-archive-cleaned.xlsx"
        ),
        out_dir=args.out_dir or settings.paths.event_rdf_root / "gda",
        debug_dir=args.debug_dir or settings.paths.debug_root,
        validate_output=args.validate,
        batch_size=args.batch_size,
        fail_fast=not args.no_fail_fast,
        include_context_graphs=not args.no_context,
        input_manifest=args.input_manifest,
        settings=settings,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

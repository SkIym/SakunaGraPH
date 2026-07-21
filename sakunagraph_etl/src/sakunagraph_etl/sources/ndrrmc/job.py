from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Iterable
from pathlib import Path

from sakunagraph_etl.rdf.graph import Graph, create_graph
from .rdf import (
    Event,
    aff_pop_mapping,
    agri_mapping,
    airport_mapping,
    casualties_mapping,
    class_mapping,
    comms_mapping,
    doc_mapping,
    event_mapping,
    flight_mapping,
    housing_mapping,
    incident_mapping,
    infra_mapping,
    pevac_mapping,
    power_mapping,
    prov_mapping,
    relief_mapping,
    rnb_mapping,
    seaport_mapping,
    stranded_mapping,
    water_mapping,
    work_mapping,
)
from .transform import (
    load_aff_pop,
    load_agri,
    load_airport,
    load_casualties,
    load_class_suspension,
    load_comms,
    load_docalamity,
    load_events,
    load_flight,
    load_housing,
    load_incidents,
    load_infra,
    load_pevac,
    load_power,
    load_provenance,
    load_relief,
    load_rnb,
    load_seaport,
    load_stranded_events,
    load_water,
    load_work_suspension,
)
from sakunagraph_etl.quality.shacl import (
    ShaclValidationError,
    ShaclValidator,
    validation_focus_nodes,
)
from sakunagraph_etl.config import PROFILE_CHOICES, SETTINGS, EtlSettings, load_settings
from sakunagraph_etl.io import (
    ArtifactRunResult,
    Storage,
    local_input_paths_from_manifest,
    record_artifact_run,
)
from sakunagraph_etl.quality.contracts import validate_source_input
from sakunagraph_etl.quality.schemas import QualityPolicy, enforce_production_quality


log = logging.getLogger(__name__)

DATA_DIR = SETTINGS.paths.parsed_root / "ndrrmc"
OUT_DIR = SETTINGS.paths.event_rdf_root / "ndrrmc"
DEFAULT_BATCH_SIZE = 10


def _configure_worker_logging() -> None:
    # ProcessPoolExecutor workers do not inherit root logger configuration.
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        )


def _run_loader_and_mapping(
    name: str,
    loader,
    mapper,
    event_folder: str,
    g: Graph,
    event_iri,
    *,
    loader_kwargs: dict | None = None,
) -> list[Path]:
    log.info("Loading %s from %s", name, event_folder)
    rows = loader(event_folder, **(loader_kwargs or {}))
    if rows:
        count = len(rows) if hasattr(rows, "__len__") else "?"
        log.info("Mapping %s: %s rows", name, count)
        mapper(g, rows, event_iri)
        log.info("Finished %s", name)
    else:
        log.info("%s: no rows, skipping", name)


def _event_folders(data_dir: str) -> list[str]:
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(f"NDRRMC parsed data directory not found: {data_dir}")

    return [
        folder
        for folder in next(os.walk(data_dir))[1]
        if os.path.exists(os.path.join(data_dir, folder, "metadata.json"))
    ]


def _quality_folders(data_dir: str | Path, selected_folders: Iterable[str]) -> list[str]:
    """Include silent discovery rejects in quality accounting."""

    root = Path(data_dir)
    missing_metadata = sorted(
        path.name
        for path in root.iterdir()
        if path.is_dir() and not (path / "metadata.json").is_file()
    )
    return list(dict.fromkeys([*selected_folders, *missing_metadata]))


def _load_events(data_dir: str, folders: Iterable[str]) -> list[Event]:
    folder_names = list(folders)
    log.info(
        "Transforming NDRRMC event metadata for %d folders",
        len(folder_names),
    )
    events = load_events(data_dir, folder_names)
    log.info("Transform produced %d NDRRMC events", len(events))
    return events


def _map_event(
    data_dir: str | Path,
    ev: Event,
    g: Graph,
    *,
    debug_dir: str | Path | None = None,
) -> None:
    log.info("Processing event: %s (%s)", ev.eventName, ev.id)

    log.info("Mapping event: %s", ev.eventName)
    event_iri = event_mapping(g, ev)
    log.info("Event IRI: %s", event_iri)

    event_folder = os.path.join(data_dir, ev.eventName)

    log.info("Loading provenance from %s", event_folder)
    prov = load_provenance(event_folder)
    if prov:
        log.info("Mapping provenance")
        prov_mapping(g, prov, event_iri)
        log.info("Finished provenance")
    else:
        log.info("Provenance: no rows, skipping")

    _run_loader_and_mapping(
        "incidents",
        load_incidents,
        incident_mapping,
        event_folder,
        g,
        event_iri,
        loader_kwargs={"debug_dir": debug_dir},
    )
    _run_loader_and_mapping("aff_pop", load_aff_pop, aff_pop_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("casualties", load_casualties, casualties_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("relief", load_relief, relief_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("infra", load_infra, infra_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping(
        "housing",
        load_housing,
        housing_mapping,
        event_folder,
        g,
        event_iri,
        loader_kwargs={"debug_dir": debug_dir},
    )
    _run_loader_and_mapping("agri", load_agri, agri_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("pevac", load_pevac, pevac_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("rnb", load_rnb, rnb_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("power", load_power, power_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("comms", load_comms, comms_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("doc_calamity", load_docalamity, doc_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping(
        "class_suspension",
        load_class_suspension,
        class_mapping,
        event_folder,
        g,
        event_iri,
        loader_kwargs={"debug_dir": debug_dir},
    )
    _run_loader_and_mapping("work_suspension", load_work_suspension, work_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("stranded", load_stranded_events, stranded_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("water", load_water, water_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("seaport", load_seaport, seaport_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("airport", load_airport, airport_mapping, event_folder, g, event_iri)
    _run_loader_and_mapping("flight", load_flight, flight_mapping, event_folder, g, event_iri)

    log.info("Finished event %s; graph now has %d triples", ev.eventName, len(g))


def _map_events(
    data_dir: str | Path,
    events: Iterable[Event],
    g: Graph,
    *,
    debug_dir: str | Path | None = None,
) -> None:
    for ev in events:
        _map_event(data_dir, ev, g, debug_dir=debug_dir)


def process_event(
    args: tuple[str | Path, Event] | tuple[str | Path, Event, str | Path | None],
) -> Graph:
    _configure_worker_logging()

    data_dir, ev = args[:2]
    debug_dir = args[2] if len(args) == 3 else None
    graph = create_graph()
    _map_event(data_dir, ev, graph, debug_dir=debug_dir)
    return graph


def _validate_batch_graph(
    g: Graph,
    *,
    validator: ShaclValidator,
    label: str,
    event_ids: Iterable[str],
) -> None:
    try:
        result = validator.validate_graph(
            g,
            label=label,
            focus_nodes=validation_focus_nodes(g),
        )
    except ShaclValidationError as exc:
        log.error("SHACL validation failed for %s", label)
        log.error("Batch event ids: %s", ", ".join(event_ids))
        log.error(exc.result.results_text)
        raise

    log.info(
        "SHACL validation passed for %s (%d data triples, %d validation triples)",
        result.label,
        result.data_triples,
        result.validation_triples,
    )


def process_event_batches(
    folders: Iterable[str],
    out_prefix: str,
    *,
    data_dir: str | Path = DATA_DIR,
    out_dir: str | Path = OUT_DIR,
    debug_dir: str | Path | None = None,
    validator: ShaclValidator | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fail_fast: bool = True,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    folder_names = list(folders)
    if not folder_names:
        log.warning("No NDRRMC events found")
        return []

    failures: list[ShaclValidationError] = []
    batches = list(_batched(folder_names, batch_size))
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for batch_no, batch_folders in enumerate(batches, start=1):
        label = f"NDRRMC batch {batch_no}/{len(batches)}"
        log.info(
            "Processing %s (%d event folders)",
            label,
            len(batch_folders),
        )

        events = _load_events(data_dir, batch_folders)
        if not events:
            log.warning("%s produced no events after transformation", label)
            continue

        batch_graph = create_graph()
        _map_events(data_dir, events, batch_graph, debug_dir=debug_dir)

        event_ids = [event.id for event in events]
        if validator is not None:
            try:
                _validate_batch_graph(
                    batch_graph,
                    validator=validator,
                    label=label,
                    event_ids=event_ids,
                )
            except ShaclValidationError as exc:
                failures.append(exc)
                if fail_fast:
                    raise
                continue

        out_path = out_dir / f"{out_prefix}-{batch_no}.ttl"
        log.info("Serializing %s to %s", label, out_path)
        batch_graph.serialize(destination=str(out_path), format="turtle")
        written.append(out_path)
        log.info(
            "Finished %s: %d events, %d triples",
            label,
            len(events),
            len(batch_graph),
        )

    if failures:
        raise RuntimeError(
            f"SHACL validation failed for {len(failures)} NDRRMC batch(es); see logs."
        )
    return written


def _batched(values: list[str], batch_size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), batch_size):
        yield values[start : start + batch_size]


def run(
    out_file: str = "ndrrmc",
    *,
    data_dir: str | Path = DATA_DIR,
    out_dir: str | Path = OUT_DIR,
    debug_dir: str | Path | None = None,
    start: int = 0,
    count: int | None = None,
    limit: int | None = None,
    batch_size: int | None = None,
    validate_output: bool = False,
    fail_fast: bool = True,
    include_context_graphs: bool = True,
    input_manifest: str | Path | None = None,
    artifact_storage: Storage | None = None,
    settings: EtlSettings = SETTINGS,
) -> ArtifactRunResult | None:
    if start < 0:
        raise ValueError("start must be greater than or equal to zero")
    if limit is not None and limit < 0:
        raise ValueError("limit must be greater than or equal to zero")

    if batch_size is not None:
        effective_batch_size = batch_size
    elif count is not None:
        log.warning("--count is deprecated; use --batch-size instead")
        effective_batch_size = count
    else:
        effective_batch_size = DEFAULT_BATCH_SIZE

    if effective_batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    log.info("=== NDRRMC pipeline start ===")
    log.info("Step 1/4: Discovering NDRRMC event folders")
    if input_manifest is not None:
        manifest_inputs = local_input_paths_from_manifest(input_manifest)
        event_directories = sorted(
            {
                path.parent
                for path in manifest_inputs
                if (path.parent / "metadata.json").is_file()
            }
        )
        if not event_directories:
            raise ValueError("NDRRMC input manifest contains no event folders")
        data_dir = Path(os.path.commonpath(event_directories))
        if all(directory.parent == data_dir for directory in event_directories):
            folders = [directory.name for directory in event_directories]
        else:
            data_dir = event_directories[0].parent
            folders = [directory.name for directory in event_directories]
    else:
        folders = _event_folders(os.fspath(data_dir))
    stop = start + limit if limit is not None else None
    selected_folders = folders[start:stop]
    quality_report = validate_source_input(
        "ndrrmc",
        data_dir,
        policy=QualityPolicy.from_settings(settings),
        folders=_quality_folders(data_dir, selected_folders),
    )
    enforce_production_quality(quality_report, settings.profile)
    log.info(
        "Selected %d of %d event folders starting at offset %d",
        len(selected_folders),
        len(folders),
        start,
    )

    validator = None
    if validate_output:
        log.info("Step 2/4: Loading SHACL validation context")
        if not include_context_graphs:
            log.info("SHACL validation context graphs are disabled")
        validator = ShaclValidator.from_paths(
            include_context_graphs=include_context_graphs,
        )
    else:
        log.info("Step 2/4: SHACL validation disabled")

    log.info(
        "Step 3/4: Processing NDRRMC events in batches of %d",
        effective_batch_size,
    )
    written = process_event_batches(
        selected_folders,
        out_file,
        data_dir=data_dir,
        out_dir=out_dir,
        debug_dir=debug_dir,
        validator=validator,
        batch_size=effective_batch_size,
        fail_fast=fail_fast,
    )

    log.info("Step 4/4: Batch files written under %s", out_dir)
    log.info("=== NDRRMC pipeline complete ===")
    if not written:
        return None
    return record_artifact_run(
        "ndrrmc",
        input_paths=(Path(data_dir) / folder for folder in selected_folders),
        output_paths=written,
        validation_status="PASSED" if validate_output else "NOT_RUN",
        settings=settings,
        storage=artifact_storage,
        parameters={
            "start": start,
            "limit": limit,
            "batch_size": effective_batch_size,
            "validate": validate_output,
        },
        metadata={
            "event_count": len(selected_folders),
            "quality": quality_report.to_dict(),
        },
    )


def build_parser(*, require_input: bool = False) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build SakunaGraPH RDF from NDRRMC.")
    parser.add_argument("--out", type=str, default="ndrrmc")
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=require_input,
        help="Parsed NDRRMC input directory.",
    )
    parser.add_argument("--out-dir", type=Path, help="RDF output directory.")
    parser.add_argument("--debug-dir", type=Path, help="Optional diagnostic CSV root.")
    parser.add_argument("--input-manifest", type=Path, help="Explicit input run manifest.")
    parser.add_argument("--profile", choices=PROFILE_CHOICES, help="Deployment profile.")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Deprecated alias for --batch-size.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of NDRRMC event folders to process after --start.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of NDRRMC event folders per output and validation batch.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run batch SHACL validation before serializing each NDRRMC graph.",
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
    if args.data_dir is None and args.input_manifest is None:
        parser.error("one of --data-dir or --input-manifest is required")
    return _run_from_args(args)


def compatibility_main(argv: list[str] | None = None) -> int:
    """Retain configured input defaults for the legacy pipeline command."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    return _run_from_args(build_parser().parse_args(argv))


def _run_from_args(args: argparse.Namespace) -> int:
    settings = load_settings(args.profile)

    run(
        args.out,
        data_dir=args.data_dir or settings.paths.parsed_root / "ndrrmc",
        out_dir=args.out_dir or settings.paths.event_rdf_root / "ndrrmc",
        debug_dir=args.debug_dir or settings.paths.debug_root,
        start=args.start,
        count=args.count,
        limit=args.limit,
        batch_size=args.batch_size,
        validate_output=args.validate,
        fail_fast=not args.no_fail_fast,
        include_context_graphs=not args.no_context,
        input_manifest=args.input_manifest,
        settings=settings,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

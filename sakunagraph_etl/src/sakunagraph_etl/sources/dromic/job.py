from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Iterable
from pathlib import Path

from .rdf import (
    aff_pop_mapping,
    assistance_mapping,
    event_mapping,
    housing_mapping,
    pevac_mapping,
    prov_mapping,
)
from sakunagraph_etl.rdf.graph import Graph, create_graph
from .transform import (
    load_aff_pop,
    load_assistance,
    load_event,
    load_housing,
    load_provenance,
)
from sakunagraph_etl.quality.shacl import (
    ShaclValidationError,
    ShaclValidator,
    validation_focus_nodes,
)
from sakunagraph_etl.config import PROFILE_CHOICES, SETTINGS, load_settings
from sakunagraph_etl.config import EtlSettings
from sakunagraph_etl.io import (
    ArtifactRunResult,
    NetworkFileStorage,
    Storage,
    local_input_paths_from_manifest,
    record_artifact_run,
)
from sakunagraph_etl.quality.contracts import validate_source_input
from sakunagraph_etl.quality.schemas import QualityPolicy, enforce_production_quality
from .state import DromicStateStore, EventStatus, EventStatusRecord, STATE_FILENAME
from .versioning import select_latest_report_folders


log = logging.getLogger(__name__)

DATA_DIR = SETTINGS.paths.parsed_root / "dromic"
OUT_DIR = SETTINGS.paths.event_rdf_root / "dromic"
DEFAULT_BATCH_SIZE = 100


def _parse_unparsed_documents(
    input_dir: str | Path,
    output_dir: str | Path,
    *,
    year: str,
) -> tuple[int, int, int]:
    """Run the resumable parser without importing its heavy stack at CLI startup."""

    from .parse import parse_pending

    result = parse_pending(input_dir, output_dir)
    log.info(
        "DROMIC parser result for %s: %d parsed, %d skipped, %d failed",
        year,
        result.parsed,
        result.skipped,
        result.failed,
    )
    return result.parsed, result.skipped, result.failed


def _check_parsed_failures(year_dir: str | Path) -> tuple[int, int]:
    """Refresh authoritative failure state before RDF transformation."""

    from .quality import check_year

    return check_year(Path(year_dir))


def _record_assistance_failure(folder_path: str, needs_rerun_path: str, exc: Exception) -> None:
    log.warning("Skipping assistance for %s: %s", folder_path, exc)
    folder = Path(folder_path).stem.strip()
    DromicStateStore(Path(needs_rerun_path).parent).update(
        [EventStatusRecord.create(
            folder,
            EventStatus.MAPPING_ERROR,
            "dromic-assistance-mapping",
            reason=str(exc),
        )]
    )


def _map_event(
    folder_path: str,
    g: Graph,
    needs_rerun_path: str,
    *,
    debug_dir: str | Path | None = None,
) -> None:
    log.info("Processing DROMIC event folder: %s", folder_path)

    ev = load_event(os.path.join(folder_path, "metadata.json"))
    event_iri = event_mapping(g, ev)
    log.info("Event IRI: %s", event_iri)

    prov = load_provenance(os.path.join(folder_path, "source.json"))
    # Provenance is intentionally event-level. Impact, response, and
    # preparedness resources are traced to the current report through their
    # owning event rather than carrying duplicate wasDerivedFrom statements.
    prov_mapping(g, prov, event_iri)

    aps, pevacs = load_aff_pop(folder_path)
    if aps:
        aff_pop_mapping(g, aps, event_iri)
    if pevacs:
        pevac_mapping(g, pevacs, event_iri)

    hs = load_housing(folder_path)
    if hs:
        housing_mapping(g, hs, event_iri)

    try:
        assistance = load_assistance(folder_path, debug_dir=debug_dir)
        if assistance:
            assistance_mapping(g, assistance, event_iri)
    except Exception as exc:
        _record_assistance_failure(folder_path, needs_rerun_path, exc)

    log.info("Finished DROMIC event %s; graph now has %d triples", ev.eventName, len(g))


def process_event(
    folder_path: str,
    needs_rerun_path: str,
    *,
    debug_dir: str | Path | None = None,
) -> Graph:
    graph = create_graph()
    _map_event(folder_path, graph, needs_rerun_path, debug_dir=debug_dir)
    return graph


def load_needs_rerun(needs_rerun_path: str) -> set[str]:
    """Load authoritative failures, falling back to the compatibility file."""
    year_dir = Path(needs_rerun_path).parent
    if (year_dir / STATE_FILENAME).exists():
        return DromicStateStore(year_dir).events_requiring_rerun()
    if not os.path.exists(needs_rerun_path):
        return set()
    with open(needs_rerun_path, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def _source_filename(folder_path: Path) -> str | None:
    source_path = folder_path / "source.json"
    if not source_path.is_file():
        return None
    try:
        import json

        source = json.loads(source_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    report_name = source.get("reportName") if isinstance(source, dict) else None
    if not isinstance(report_name, str) or not report_name:
        return None
    lowered = report_name.casefold()
    if lowered.endswith(".docx") or lowered.endswith(".doc"):
        return str(Path(report_name).with_suffix(".pdf"))
    return report_name


def _event_folders(
    sub_data_dir: str,
    needs_rerun: set[str],
    failed_sources: set[str] | None = None,
) -> tuple[list[str], int]:
    if not os.path.isdir(sub_data_dir):
        raise FileNotFoundError(f"DROMIC parsed data directory not found: {sub_data_dir}")

    failed_source_names = {
        name.casefold()
        for name in (failed_sources or ())
    }
    folders: list[str] = []
    skipped = 0
    for folder in next(os.walk(sub_data_dir))[1]:
        folder_path = Path(sub_data_dir) / folder
        source_name = _source_filename(folder_path)
        if (
            folder in needs_rerun
            or (
                source_name is not None
                and source_name.casefold() in failed_source_names
            )
        ):
            skipped += 1
            continue

        if (folder_path / "metadata.json").exists():
            folders.append(folder)

    return folders, skipped


def _quality_folders(
    sub_data_dir: str | Path,
    selected_folders: Iterable[str],
) -> list[str]:
    """Include silent discovery rejects in quality accounting."""

    from .quality import is_event_directory

    root = Path(sub_data_dir)
    missing_metadata = sorted(
        path.name
        for path in root.iterdir()
        if is_event_directory(path) and not (path / "metadata.json").is_file()
    )
    return list(dict.fromkeys([*selected_folders, *missing_metadata]))


def _validate_batch_graph(
    g: Graph,
    *,
    validator: ShaclValidator,
    label: str,
    folders: Iterable[str],
) -> None:
    try:
        result = validator.validate_graph(
            g,
            label=label,
            focus_nodes=validation_focus_nodes(g),
        )
    except ShaclValidationError as exc:
        log.error("SHACL validation failed for %s", label)
        log.error("Batch folders: %s", ", ".join(folders))
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
    g: Graph,
    *,
    sub_data_dir: str,
    needs_rerun_path: str,
    debug_dir: str | Path | None = None,
    validator: ShaclValidator | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fail_fast: bool = True,
    state_store: DromicStateStore | None = None,
) -> int:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    folder_names = list(folders)
    if not folder_names:
        log.warning("No DROMIC events found")
        return 0

    failures: list[ShaclValidationError] = []
    processed = 0
    batches = list(_batched(folder_names, batch_size))
    for batch_no, batch_folders in enumerate(batches, start=1):
        label = f"DROMIC batch {batch_no}/{len(batches)}"
        log.info("Processing %s (%d event folders)", label, len(batch_folders))

        batch_graph = create_graph()
        for folder in batch_folders:
            folder_path = os.path.join(sub_data_dir, folder)
            try:
                _map_event(
                    folder_path,
                    batch_graph,
                    needs_rerun_path,
                    debug_dir=debug_dir,
                )
            except Exception as error:
                if state_store is not None:
                    state_store.update(
                        [EventStatusRecord.create(
                            folder,
                            EventStatus.MAPPING_ERROR,
                            "dromic-job",
                            reason=str(error),
                        )]
                    )
                if fail_fast:
                    raise
                log.exception("Skipping DROMIC mapping failure for %s", folder)

        if validator is not None:
            try:
                _validate_batch_graph(
                    batch_graph,
                    validator=validator,
                    label=label,
                    folders=batch_folders,
                )
            except ShaclValidationError as exc:
                failures.append(exc)
                if state_store is not None:
                    state_store.update(
                        EventStatusRecord.create(
                            folder,
                            EventStatus.MAPPING_ERROR,
                            "dromic-validation",
                            reason=str(exc),
                        )
                        for folder in batch_folders
                    )
                if fail_fast:
                    raise
                continue

        g += batch_graph
        processed += len(batch_folders)
        if state_store is not None:
            state_store.update(
                EventStatusRecord.create(
                    folder,
                    EventStatus.MAPPED,
                    "dromic-job",
                    reason="RDF mapping completed",
                )
                for folder in batch_folders
            )
            if validator is not None:
                state_store.update(
                    EventStatusRecord.create(
                        folder,
                        EventStatus.MAPPED,
                        "dromic-validation",
                        reason="SHACL validation passed",
                    )
                    for folder in batch_folders
                )
        log.info(
            "Finished %s: %d events, %d triples",
            label,
            len(batch_folders),
            len(batch_graph),
        )

    if failures:
        raise RuntimeError(
            f"SHACL validation failed for {len(failures)} DROMIC batch(es); see logs."
        )

    return processed


def _batched(values: list[str], batch_size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), batch_size):
        yield values[start : start + batch_size]


def run(
    sub_data_dir: str | Path,
    out_file: str | Path,
    *,
    input_dir: str | Path | None = None,
    year: str | None = None,
    force_transform: bool = False,
    start: int = 0,
    count: int | None = None,
    limit: int | None = None,
    batch_size: int | None = None,
    validate_output: bool = False,
    fail_fast: bool = True,
    include_context_graphs: bool = True,
    debug_dir: str | Path | None = None,
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

    sub_data_dir = os.fspath(sub_data_dir)
    out_file = Path(out_file)
    log.info("=== DROMIC pipeline start: %s ===", sub_data_dir)
    effective_year = year or Path(sub_data_dir).name
    if input_dir is not None and input_manifest is not None:
        raise ValueError("input_dir and input_manifest cannot be used together")
    if input_dir is not None:
        log.info("Step 1/6: Parsing new or updated DROMIC documents from %s", input_dir)
        parsed_count, parser_skipped, parser_failed = _parse_unparsed_documents(
            input_dir,
            sub_data_dir,
            year=effective_year,
        )
        if parsed_count == 0 and out_file.is_file() and not force_transform:
            log.info(
                "No new or updated DROMIC documents for %s; keeping %s unchanged",
                effective_year,
                out_file,
            )
            return record_artifact_run(
                "dromic",
                input_paths=(),
                output_paths=(out_file,),
                validation_status="NOT_RUN",
                settings=settings,
                storage=artifact_storage,
                parameters={
                    "year": effective_year,
                    "unchanged": True,
                },
                metadata={
                    "parsed_document_count": parsed_count,
                    "parser_skipped_count": parser_skipped,
                    "parser_failed_count": parser_failed,
                },
            )
    else:
        log.info("Step 1/6: Raw parsing skipped; no PDF input directory selected")

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
            raise ValueError("DROMIC input manifest contains no event folders")
        sub_data_dir = os.fspath(event_directories[0].parent)
        manifest_folders = [directory.name for directory in event_directories]
    else:
        manifest_folders = None

    log.info("Step 2/6: Checking parsed DROMIC folders for known failures")
    flagged, checked = _check_parsed_failures(sub_data_dir)
    log.info(
        "Checked %d parsed folder(s); %d failed folder(s) will be excluded",
        checked,
        flagged,
    )

    state_store = DromicStateStore(sub_data_dir)
    needs_rerun_path = os.path.join(sub_data_dir, "_needs_rerun.txt")
    state_manifest = state_store.load()
    needs_rerun = state_store.events_requiring_rerun(state_manifest)
    failed_sources = state_store.failed_sources(state_manifest)
    failed_source_names = {name.casefold() for name in failed_sources}

    log.info("Step 3/6: Discovering eligible DROMIC event folders")
    if manifest_folders is None:
        folders, skipped = _event_folders(
            sub_data_dir,
            needs_rerun,
            failed_sources,
        )
    else:
        folders = [
            folder
            for folder in manifest_folders
            if folder not in needs_rerun
            and (
                (_source_filename(Path(sub_data_dir) / folder) or "").casefold()
                not in failed_source_names
            )
        ]
        skipped = len(manifest_folders) - len(folders)
    folders, superseded = select_latest_report_folders(sub_data_dir, folders)
    skipped += superseded
    if superseded:
        log.info(
            "Excluded %d superseded DROMIC report folder(s) from active facts",
            superseded,
        )
    stop = start + limit if limit is not None else None
    selected_folders = folders[start:stop]
    quality_report = validate_source_input(
        "dromic",
        sub_data_dir,
        policy=QualityPolicy.from_settings(settings),
        folders=_quality_folders(sub_data_dir, selected_folders),
    )
    enforce_production_quality(quality_report, settings.profile)
    log.info(
        "Selected %d of %d event folders starting at offset %d (skipped %d)",
        len(selected_folders),
        len(folders),
        start,
        skipped,
    )

    validator = None
    if validate_output:
        log.info("Step 4/6: Loading SHACL validation context")
        if not include_context_graphs:
            log.info("SHACL validation context graphs are disabled")
        validator = ShaclValidator.from_paths(
            include_context_graphs=include_context_graphs,
        )
    else:
        log.info("Step 4/6: SHACL validation disabled")

    log.info(
        "Step 5/6: Processing DROMIC events in batches of %d",
        effective_batch_size,
    )
    main_graph = create_graph()
    processed = process_event_batches(
        selected_folders,
        main_graph,
        sub_data_dir=sub_data_dir,
        needs_rerun_path=needs_rerun_path,
        debug_dir=debug_dir,
        validator=validator,
        batch_size=effective_batch_size,
        fail_fast=fail_fast,
        state_store=state_store,
    )

    out_file.parent.mkdir(parents=True, exist_ok=True)
    log.info("Step 6/6: Serializing graph to %s", out_file)
    turtle = main_graph.serialize(format="turtle", encoding="utf-8")
    if not isinstance(turtle, bytes):
        turtle = turtle.encode("utf-8")
    NetworkFileStorage(out_file.parent).write_bytes(out_file.name, turtle, atomic=True)
    log.info(
        "=== DROMIC pipeline complete: %d events -> %s (skipped %d) ===",
        processed,
        out_file,
        skipped,
    )
    if not selected_folders:
        return None
    return record_artifact_run(
        "dromic",
        input_paths=(Path(sub_data_dir) / folder for folder in selected_folders),
        output_paths=(out_file,),
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
            "event_count": processed,
            "skipped_event_count": skipped,
            "quality": quality_report.to_dict(),
        },
    )


def _year_dirs(data_dir: str | Path) -> list[str]:
    data_dir = os.fspath(data_dir)
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(f"DROMIC parsed data root not found: {data_dir}")
    return list(next(os.walk(data_dir))[1])


def _pipeline_years(
    data_dir: str | Path,
    input_root: str | Path,
) -> list[str]:
    """Discover years available in either parsed data or raw ``YEAR`` inputs."""

    years = {
        path.name
        for path in Path(data_dir).iterdir()
        if path.is_dir() and path.name.isdigit()
    } if Path(data_dir).is_dir() else set()

    raw_root = Path(input_root)
    if raw_root.is_dir():
        years.update(
            path.name
            for path in raw_root.iterdir()
            if path.is_dir()
            and path.name.isdigit()
        )

    if not years:
        raise FileNotFoundError(
            f"No DROMIC year directories found under {data_dir} or {input_root}"
        )
    return sorted(years, key=int)


def build_parser(*, require_input: bool = False) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build SakunaGraPH RDF from DROMIC.")
    parser.add_argument("--year", type=str, default="2026")
    parser.add_argument("--out", type=str, default="dromic")
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=require_input,
        help="Parsed DROMIC root containing year directories.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        help=(
            "PDF input directory for the selected year. The pipeline parses only "
            "documents whose recorded acquisition version is not current."
        ),
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
        help="Maximum number of DROMIC event folders to process after --start.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Number of DROMIC event folders per validation batch. Defaults to 100.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run batch SHACL validation before merging each DROMIC batch.",
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
    parser.add_argument(
        "--force-transform",
        action="store_true",
        help="Transform the selected year even when parsing finds no changed PDFs.",
    )
    parser.add_argument("--all", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args(argv)
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
    data_dir = args.data_dir or settings.paths.parsed_root / "dromic"
    out_dir = args.out_dir or settings.paths.event_rdf_root / "dromic"
    debug_dir = args.debug_dir or settings.paths.debug_root
    raw_root = settings.paths.raw_root / "dromic"

    if args.all:
        for folder in _pipeline_years(data_dir, args.input_dir or raw_root):
            run(
                sub_data_dir=data_dir / folder,
                out_file=out_dir / f"{args.out}-{folder}.ttl",
                input_dir=(
                    None
                    if args.input_manifest is not None
                    else (
                        (args.input_dir / folder)
                        if args.input_dir is not None
                        else raw_root / folder
                    )
                ),
                year=folder,
                start=args.start,
                count=args.count,
                limit=args.limit,
                batch_size=args.batch_size,
                validate_output=args.validate,
                fail_fast=not args.no_fail_fast,
                include_context_graphs=not args.no_context,
                debug_dir=debug_dir,
                input_manifest=args.input_manifest,
                force_transform=args.force_transform,
                settings=settings,
            )
    else:
        run(
            sub_data_dir=data_dir / str(args.year),
            out_file=out_dir / f"{args.out}-{args.year}.ttl",
            input_dir=(
                None
                if args.input_manifest is not None
                else args.input_dir or raw_root / str(args.year)
            ),
            year=str(args.year),
            start=args.start,
            count=args.count,
            limit=args.limit,
            batch_size=args.batch_size,
            validate_output=args.validate,
            fail_fast=not args.no_fail_fast,
            include_context_graphs=not args.no_context,
            debug_dir=debug_dir,
            input_manifest=args.input_manifest,
            force_transform=args.force_transform,
            settings=settings,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""End-to-end EM-DAT job with typed, injectable stage boundaries."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import logging
from pathlib import Path
from typing import Any, Iterable, Sequence

from rdflib import URIRef

from sakunagraph_etl.config import PROFILE_CHOICES, SETTINGS, EtlSettings, load_settings
from sakunagraph_etl.io import (
    ArtifactManifest,
    JsonManifestStore,
    LocalFileStorage,
    ManifestStore,
    RunManifest,
    Storage,
    record_artifact_run,
    stable_run_id,
)
from sakunagraph_etl.quality.contracts import EMDAT_SCHEMA
from sakunagraph_etl.quality.schemas import QualityPolicy, enforce_production_quality, validate_table
from sakunagraph_etl import __version__
from sakunagraph_etl.rdf import (
    Graph,
    ShaclValidationService,
    ValidationService,
    create_graph,
    validation_focus_nodes,
)

from .parse import latest_workbook, parse_workbook
from .rdf import (
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
from .transform import EmdatEntityMap, EmdatTransformer, load_source, transform_emdat


log = logging.getLogger(__name__)

DATA_DIR = SETTINGS.paths.raw_root / "emdat"
OUT_DIR = SETTINGS.paths.event_rdf_root / "emdat"
DEFAULT_BATCH_SIZE = 100


@dataclass(frozen=True, slots=True)
class EmdatJobResult:
    input_path: Path
    output_path: Path
    manifest_path: Path | None
    triple_count: int
    output_sha256: str
    canonical_rdf_sha256: str
    run_id: str
    immutable_manifest_key: str


def _run_mapping(
    name: str,
    fn: Any,
    entities: EmdatEntityMap,
    cls: type,
    graph: Graph,
    *extra: Any,
) -> None:
    rows = entities.get(cls, [])
    log.info("Mapping %s: %d rows", name, len(rows))
    fn(rows, graph, *extra)


def _map_entities(entities: EmdatEntityMap, graph: Graph, source_uri: URIRef) -> None:
    _run_mapping("event_mapping", event_mapping, entities, Event, graph, source_uri)
    _run_mapping("assistance_mapping", assistance_mapping, entities, Assistance, graph)
    _run_mapping("recovery_mapping", recovery_mapping, entities, Recovery, graph)
    _run_mapping("damage_gen_mapping", damage_gen_mapping, entities, DamageGeneral, graph)
    _run_mapping("casualties_mapping", casualties_mapping, entities, Casualties, graph)
    _run_mapping("aff_pop_mapping", aff_pop_mapping, entities, AffectedPopulation, graph)


def _load_entities(
    input_path: str | Path,
    *,
    debug_dir: str | Path | None = None,
) -> EmdatEntityMap:
    """Compatibility helper retained for callers of the former runner."""

    return transform_emdat(input_path, debug_dir=debug_dir)


def process_event(
    input_path: str | Path,
    graph: Graph,
    source_uri: URIRef,
    *,
    debug_dir: str | Path | None = None,
) -> None:
    _map_entities(_load_entities(input_path, debug_dir=debug_dir), graph, source_uri)


def validate_mapped_graph(
    graph: Graph,
    *,
    validation_service: ValidationService,
    label: str,
) -> None:
    outcome = validation_service.validate(
        graph,
        label=label,
        focus_nodes=validation_focus_nodes(graph),
        raise_on_error=False,
    )
    if not outcome.conforms:
        log.error("SHACL validation failed for %s\n%s", label, outcome.details)
        raise RuntimeError(f"SHACL validation failed for {label}: {outcome.details}")
    log.info(
        "SHACL validation passed for %s (%d data triples, %d validation triples)",
        outcome.label,
        outcome.data_triples,
        outcome.validation_triples,
    )


def process_event_batches(
    entities: EmdatEntityMap,
    graph: Graph,
    source_uri: URIRef,
    *,
    validation_service: ValidationService,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fail_fast: bool = True,
) -> None:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    event_ids = _event_ids(entities)
    if not event_ids:
        log.warning("No EM-DAT events found for validation")
        return

    failures: list[str] = []
    batches = list(_batched(event_ids, batch_size))
    source_triples = tuple(graph.triples((source_uri, None, None)))
    for batch_no, batch_ids in enumerate(batches, start=1):
        batch_graph = create_graph()
        for triple in source_triples:
            batch_graph.add(triple)
        _map_entities(_filter_entities_by_event_ids(entities, batch_ids), batch_graph, source_uri)

        label = f"EM-DAT batch {batch_no}/{len(batches)}"
        outcome = validation_service.validate(
            batch_graph,
            label=label,
            focus_nodes=validation_focus_nodes(batch_graph),
            raise_on_error=False,
        )
        if not outcome.conforms:
            failures.append(label)
            log.error("SHACL validation failed for %s\n%s", label, outcome.details)
            if fail_fast:
                raise RuntimeError(f"SHACL validation failed for {label}: {outcome.details}")
            continue
        graph += batch_graph

    if failures:
        raise RuntimeError(
            f"SHACL validation failed for {len(failures)} EM-DAT batch(es): "
            + ", ".join(failures)
        )


def _latest_source_file(
    data_dir: str | Path = DATA_DIR,
    *,
    storage: Storage | None = None,
) -> Path:
    """Compatibility name backed by the source package parser boundary."""

    return latest_workbook(data_dir, storage=storage)


def _event_ids(entities: EmdatEntityMap) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for row in entities.get(Event, []):
        event_id = str(row.id)
        if event_id not in seen:
            ids.append(event_id)
            seen.add(event_id)
    return ids


def _filter_entities_by_event_ids(
    entities: EmdatEntityMap,
    event_ids: Iterable[str],
) -> EmdatEntityMap:
    wanted = set(event_ids)
    return {
        cls: [row for row in rows if str(getattr(row, "id", "")) in wanted]
        for cls, rows in entities.items()
    }


def _batched(values: list[str], batch_size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), batch_size):
        yield values[start : start + batch_size]


def canonical_rdf_bytes(graph: Graph) -> bytes:
    """Return a stable N-Triples representation for semantic regression checks."""

    from rdflib.compare import to_canonical_graph

    serialized = to_canonical_graph(graph).serialize(format="nt")
    lines = sorted(line.strip() for line in serialized.splitlines() if line.strip())
    return (("\n".join(lines) + "\n") if lines else "").encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _resolve_output_path(out_file: str | Path, out_dir: str | Path) -> Path:
    output_path = Path(out_file)
    if not output_path.is_absolute():
        output_path = Path(out_dir) / output_path
    return output_path.expanduser().resolve(strict=False)


def _default_manifest_path(settings: EtlSettings, output_path: Path) -> Path:
    return settings.paths.logs_root / "manifests" / "emdat" / f"{output_path.stem}.json"


def run(
    out_file: str | Path = "emdat.ttl",
    *,
    input_path: str | Path | None = None,
    data_dir: str | Path = DATA_DIR,
    out_dir: str | Path = OUT_DIR,
    debug_dir: str | Path | None = None,
    validate_output: bool = False,
    validate_by_batch: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fail_fast: bool = True,
    include_context_graphs: bool = True,
    transformer: EmdatTransformer | None = None,
    validation_service: ValidationService | None = None,
    source_storage: Storage | None = None,
    output_storage: Storage | None = None,
    manifest_store: ManifestStore | None = None,
    manifest_path: str | Path | None = None,
    input_manifest: str | Path | None = None,
    artifact_storage: Storage | None = None,
    settings: EtlSettings = SETTINGS,
) -> EmdatJobResult:
    """Run EM-DAT from workbook input through validated, manifested RDF output."""

    log.info("=== EM-DAT pipeline start ===")
    path = (
        Path(input_path).expanduser().resolve(strict=True)
        if input_path is not None
        else latest_workbook(
            data_dir,
            storage=source_storage,
            input_manifest=input_manifest,
        )
    )
    workbook = parse_workbook(path)
    quality_report = validate_table(
        workbook.rows,
        EMDAT_SCHEMA,
        policy=QualityPolicy.from_settings(settings),
    )
    enforce_production_quality(quality_report, settings.profile)
    transformed = (transformer or EmdatTransformer()).transform(workbook, debug_dir=debug_dir)

    graph = create_graph()
    source_uri = source_mapping(graph, transformed.source)

    service = validation_service
    if validate_output and service is None:
        service = ShaclValidationService(include_context_graphs=include_context_graphs)

    if validate_output and validate_by_batch:
        assert service is not None
        process_event_batches(
            transformed.entities,
            graph,
            source_uri,
            validation_service=service,
            batch_size=batch_size,
            fail_fast=fail_fast,
        )
    else:
        _map_entities(transformed.entities, graph, source_uri)
        if validate_output:
            assert service is not None
            validate_mapped_graph(graph, validation_service=service, label="EM-DAT output")

    output_path = _resolve_output_path(out_file, out_dir)
    turtle = graph.serialize(format="turtle", encoding="utf-8")
    if not isinstance(turtle, bytes):
        turtle = turtle.encode("utf-8")
    sink = output_storage or LocalFileStorage(output_path.parent)
    output_key: str | Path = output_path.name if output_storage is None else Path(out_file)
    written_path = sink.write_bytes(output_key, turtle, atomic=True)

    output_digest = _sha256(turtle)
    canonical_digest = _sha256(canonical_rdf_bytes(graph))
    input_bytes = path.read_bytes()
    selected_manifest_path = (
        Path(manifest_path).expanduser().resolve(strict=False)
        if manifest_path is not None
        else _default_manifest_path(settings, output_path).resolve(strict=False)
    )
    store = manifest_store
    if store is None:
        store = JsonManifestStore(
            LocalFileStorage(selected_manifest_path.parent),
            selected_manifest_path.name,
        )
    input_digest = _sha256(input_bytes)
    run_id = stable_run_id(
        "emdat",
        ((path.name, input_digest, len(input_bytes)),),
        parameters={
            "validate": validate_output,
            "batch": validate_by_batch,
            "batch_size": batch_size,
        },
    )
    validation_status = "PASSED" if validate_output else "NOT_RUN"
    manifest = RunManifest(
        run_id=run_id,
        pipeline="emdat",
        created_at=datetime.now(timezone.utc).isoformat(),
        profile=settings.profile.value,
        artifacts=(
            ArtifactManifest(
                path=str(path),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                size_bytes=len(input_bytes),
                sha256=input_digest,
                role="input",
                validation_status="NOT_APPLICABLE",
                code_version=__version__,
                original_path=str(path),
            ),
            ArtifactManifest(
                path=str(written_path),
                media_type="text/turtle",
                size_bytes=len(turtle),
                sha256=output_digest,
                role="output",
                validation_status=validation_status,
                code_version=__version__,
                original_path=str(written_path),
            ),
        ),
        metadata={
            "canonical_rdf_sha256": canonical_digest,
            "entity_count": transformed.entity_count,
            "source_rows": workbook.row_count,
            "quality": quality_report.to_dict(),
            "triple_count": len(graph),
            "validated": validate_output,
        },
        code_version=__version__,
        status="COMPLETED",
        schema_version=2,
    )
    store.save(manifest)

    immutable = record_artifact_run(
        "emdat",
        input_paths=(path,),
        output_paths=(written_path,),
        validation_status=validation_status,
        settings=settings,
        storage=artifact_storage,
        parameters={
            "validate": validate_output,
            "batch": validate_by_batch,
            "batch_size": batch_size,
        },
        metadata={
            "canonical_rdf_sha256": canonical_digest,
            "quality": quality_report.to_dict(),
            "triple_count": len(graph),
        },
    )

    log.info("=== EM-DAT pipeline complete: %s ===", written_path)
    return EmdatJobResult(
        input_path=path,
        output_path=written_path,
        manifest_path=selected_manifest_path,
        triple_count=len(graph),
        output_sha256=output_digest,
        canonical_rdf_sha256=canonical_digest,
        run_id=run_id,
        immutable_manifest_key=immutable.manifest_key,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build SakunaGraPH RDF from EM-DAT.")
    parser.add_argument("--out", default="emdat.ttl")
    parser.add_argument("--input", type=Path, help="Explicit EM-DAT workbook path.")
    parser.add_argument("--data-dir", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--debug-dir", type=Path)
    parser.add_argument("--manifest", type=Path, help="Run manifest output path.")
    parser.add_argument("--input-manifest", type=Path, help="Explicit input run manifest.")
    parser.add_argument("--profile", choices=PROFILE_CHOICES)
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--batch", action="store_true")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--no-fail-fast", action="store_true")
    parser.add_argument("--no-context", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    args = build_parser().parse_args(argv)
    settings = load_settings(args.profile)
    run(
        args.out,
        input_path=args.input,
        data_dir=args.data_dir or settings.paths.raw_root / "emdat",
        out_dir=args.out_dir or settings.paths.event_rdf_root / "emdat",
        debug_dir=args.debug_dir or settings.paths.debug_root,
        validate_output=args.validate,
        validate_by_batch=args.batch,
        batch_size=args.batch_size,
        fail_fast=not args.no_fail_fast,
        include_context_graphs=not args.no_context,
        manifest_path=args.manifest,
        input_manifest=args.input_manifest,
        settings=settings,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DATA_DIR",
    "DEFAULT_BATCH_SIZE",
    "EmdatJobResult",
    "OUT_DIR",
    "build_parser",
    "canonical_rdf_bytes",
    "main",
    "process_event",
    "process_event_batches",
    "run",
    "validate_mapped_graph",
]

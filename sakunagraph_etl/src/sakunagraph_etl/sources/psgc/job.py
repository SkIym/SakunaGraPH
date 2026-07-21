from __future__ import annotations

import argparse
import logging
import shutil
from pathlib import Path

from rdflib import Graph

from . import rdf as psgc_datafile
from sakunagraph_etl.quality.shacl import ShaclValidationError, validate_graph
from sakunagraph_etl.config import PROFILE_CHOICES, SETTINGS, EtlSettings, load_settings
from sakunagraph_etl.io import (
    Storage,
    local_input_paths_from_manifest,
    record_artifact_run,
)
from sakunagraph_etl.quality.contracts import PSGC_SCHEMA
from sakunagraph_etl.quality.schemas import QualityPolicy, enforce_production_quality, validate_table


log = logging.getLogger(__name__)

DEFAULT_INPUT_DIR = SETTINGS.paths.raw_root / "psgc"
DEFAULT_OUTPUT_DIR = SETTINGS.paths.rdf_root / "psgc"
DEFAULT_SHAPES_PATH = SETTINGS.paths.ontology_root / "shapes" / "psgc" / "shapes.ttl"
EXCEL_DATAFILE_PATTERNS = ("*.xlsx", "*.xlsm", "*.xls")


def build_psgc_graph(input_path: Path, *, include_barangay: bool) -> tuple[Graph, int, int]:
    log.info("Loading PSGC workbook: %s", input_path)
    df = psgc_datafile.load_dataframe(input_path)
    log.info(
        "Loaded %d PSGC rows across %d levels: %s",
        len(df),
        df["geo_level"].n_unique(),
        sorted(df["geo_level"].unique().to_list()),
    )

    g = psgc_datafile.init_graph()
    psgc_datafile.add_island_group(g)
    individual_count, part_count = psgc_datafile.build_abox(
        g,
        df,
        include_barangay=include_barangay,
    )
    return g, individual_count, part_count


def validate_psgc_graph(path: Path, *, shapes_path: Path) -> None:
    log.info("Validating staged PSGC RDF against %s", shapes_path)
    graph = Graph()
    graph.parse(path)
    result = validate_graph(
        graph,
        label=str(path),
        shapes_graph=shapes_path,
        ontology_graph=None,
        include_context_graphs=False,
        include_default_context=False,
    )
    log.info(
        "SHACL validation passed for %s (%d data triples)",
        result.label,
        result.data_triples,
    )


def run(
    *,
    input_path: str | Path,
    out_file: str | Path = "psgc.ttl",
    out_dir: str | Path = DEFAULT_OUTPUT_DIR,
    staged_out: str | Path | None = None,
    shapes_path: str | Path = DEFAULT_SHAPES_PATH,
    include_barangay: bool = False,
    keep_staged: bool = False,
    rdf_format: str = "turtle",
    allow_input_discovery: bool = False,
    input_manifest: str | Path | None = None,
    artifact_storage: Storage | None = None,
    settings: EtlSettings = SETTINGS,
) -> Path:
    log.info("=== PSGC pipeline start ===")
    if input_manifest is not None:
        manifest_inputs = [
            path
            for path in local_input_paths_from_manifest(input_manifest)
            if path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}
        ]
        if len(manifest_inputs) != 1:
            raise ValueError("PSGC input manifest must select exactly one workbook")
        input_path = manifest_inputs[0]
    input_path = _resolve_input_path(input_path, allow_discovery=allow_input_discovery)
    output_path = _resolve_output_path(out_file, out_dir)
    staged_path = _resolve_staged_path(staged_out, output_path, out_dir)
    shapes_path = Path(shapes_path)

    quality_report = validate_table(
        psgc_datafile.load_dataframe(input_path),
        PSGC_SCHEMA,
        policy=QualityPolicy.from_settings(settings),
    )
    enforce_production_quality(quality_report, settings.profile)

    if staged_path == output_path:
        raise ValueError("staged output path must differ from final output path")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    staged_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("Step 1/3: Transforming PSGC workbook to staged RDF")
    graph, individual_count, part_count = build_psgc_graph(
        input_path,
        include_barangay=include_barangay,
    )
    log.info(
        "Built PSGC graph: %d individuals, %d isPartOf triples, %d total triples",
        individual_count,
        part_count,
        len(graph),
    )
    graph.serialize(destination=str(staged_path), format=rdf_format)
    log.info("Wrote staged PSGC RDF: %s", staged_path)

    log.info("Step 2/3: Validating staged RDF")
    try:
        validate_psgc_graph(staged_path, shapes_path=shapes_path)
    except ShaclValidationError:
        log.error("Validation failed; final PSGC RDF was not updated: %s", output_path)
        record_artifact_run(
            "psgc",
            input_paths=(input_path, shapes_path),
            output_paths=(staged_path,),
            validation_status="FAILED",
            settings=settings,
            storage=artifact_storage,
            parameters={"include_barangay": include_barangay, "format": rdf_format},
            metadata={"quality": quality_report.to_dict()},
        )
        raise

    log.info("Step 3/3: Promoting staged RDF to final output")
    if keep_staged:
        shutil.copyfile(staged_path, output_path)
        log.info("Copied staged RDF to final output and kept staged file")
    else:
        staged_path.replace(output_path)
        log.info("Moved staged RDF to final output")

    log.info("=== PSGC pipeline complete: %s ===", output_path)
    record_artifact_run(
        "psgc",
        input_paths=(input_path, shapes_path),
        output_paths=(output_path,),
        validation_status="PASSED",
        settings=settings,
        storage=artifact_storage,
        parameters={"include_barangay": include_barangay, "format": rdf_format},
        metadata={
            "triple_count": len(graph),
            "individual_count": individual_count,
            "part_count": part_count,
            "quality": quality_report.to_dict(),
        },
    )
    return output_path


def _resolve_output_path(path: str | Path, output_dir: str | Path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    return Path(output_dir) / path


def _resolve_input_path(path: str | Path, *, allow_discovery: bool = False) -> Path:
    path = Path(path)
    if path.is_file():
        return path
    if path.is_dir():
        if not allow_discovery:
            raise IsADirectoryError(
                f"PSGC input must be an explicit workbook, not a directory: {path}"
            )
        return _latest_psgc_datafile(path)
    raise FileNotFoundError(f"PSGC input path does not exist: {path}")


def _latest_psgc_datafile(input_dir: Path) -> Path:
    candidates = _matching_files(input_dir, EXCEL_DATAFILE_PATTERNS)
    if not candidates:
        patterns = ", ".join(EXCEL_DATAFILE_PATTERNS)
        raise FileNotFoundError(
            f"No PSGC Excel datafile found in {input_dir} matching {patterns}"
        )

    if len(candidates) != 1:
        raise ValueError(
            f"Multiple PSGC workbooks found in {input_dir}; pass --input or --input-manifest"
        )
    log.info("Selected sole PSGC datafile: %s", candidates[0])
    return candidates[0]


def _matching_files(input_dir: Path, patterns: tuple[str, ...]) -> list[Path]:
    seen: set[Path] = set()
    matches: list[Path] = []
    for pattern in patterns:
        for path in input_dir.glob(pattern):
            if path.is_file() and path not in seen:
                matches.append(path)
                seen.add(path)
    return matches


def _resolve_staged_path(
    staged_out: str | Path | None,
    output_path: Path,
    output_dir: str | Path,
) -> Path:
    if staged_out is None:
        return output_path.with_name(f"{output_path.stem}.staged{output_path.suffix}")

    staged_path = Path(staged_out)
    if staged_path.is_absolute():
        return staged_path
    return Path(output_dir) / staged_path


def build_parser(*, require_input: bool = False) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Transform, validate, and publish PSGC RDF."
    )
    parser.add_argument(
        "--input",
        "-i",
        default=None,
        required=require_input,
        help=(
            "Explicit PSGC workbook to transform. Legacy pipeline commands also "
            "accept a directory for compatibility."
        ),
    )
    parser.add_argument(
        "--out",
        "-o",
        default="psgc.ttl",
        help="Final PSGC RDF filename or path. Relative paths resolve under data/rdf/psgc.",
    )
    parser.add_argument("--out-dir", type=Path, help="PSGC RDF output directory.")
    parser.add_argument("--input-manifest", type=Path, help="Explicit input run manifest.")
    parser.add_argument("--profile", choices=PROFILE_CHOICES, help="Deployment profile.")
    parser.add_argument(
        "--staged-out",
        default=None,
        help="Staged RDF filename or path to validate before promotion.",
    )
    parser.add_argument(
        "--shapes",
        default=None,
        help="PSGC SHACL shapes file.",
    )
    parser.add_argument(
        "--format",
        "-f",
        default="turtle",
        choices=["turtle", "xml", "n3", "nt", "json-ld"],
        help="RDF serialization format.",
    )
    parser.add_argument(
        "--barangay",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Include barangay-level PSGC rows.",
    )
    parser.add_argument(
        "--keep-staged",
        action="store_true",
        help="Keep the staged RDF file after copying it to the final output.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.input is None and args.input_manifest is None:
        parser.error("one of --input or --input-manifest is required")
    return _run_from_args(args, allow_input_discovery=False)


def compatibility_main(argv: list[str] | None = None) -> int:
    """Retain legacy directory discovery for the old pipeline command only."""

    args = build_parser().parse_args(argv)
    return _run_from_args(args, allow_input_discovery=True)


def _run_from_args(
    args: argparse.Namespace,
    *,
    allow_input_discovery: bool,
) -> int:
    settings = load_settings(args.profile)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    run(
        input_path=args.input or settings.paths.raw_root / "psgc",
        out_file=args.out,
        out_dir=args.out_dir or settings.paths.rdf_root / "psgc",
        staged_out=args.staged_out,
        shapes_path=args.shapes or settings.paths.ontology_root / "shapes" / "psgc" / "shapes.ttl",
        include_barangay=args.barangay,
        keep_staged=args.keep_staged,
        rdf_format=args.format,
        allow_input_discovery=allow_input_discovery,
        input_manifest=args.input_manifest,
        settings=settings,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

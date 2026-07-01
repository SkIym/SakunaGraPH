from __future__ import annotations

import argparse
import logging
import shutil
from pathlib import Path

from rdflib import Graph

from transform import psgc_datafile
from validate.validate import ShaclValidationError, validate_graph


log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = ROOT_DIR / "data" / "raw" / "psgc"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "data" / "rdf" / "psgc"
DEFAULT_SHAPES_PATH = ROOT_DIR / "ontology" / "shapes" / "psgc" / "shapes.ttl"
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
    input_path: str | Path = DEFAULT_INPUT_DIR,
    out_file: str | Path = "psgc.ttl",
    staged_out: str | Path | None = None,
    shapes_path: str | Path = DEFAULT_SHAPES_PATH,
    include_barangay: bool = False,
    keep_staged: bool = False,
    rdf_format: str = "turtle",
) -> Path:
    log.info("=== PSGC pipeline start ===")
    input_path = _resolve_input_path(input_path)
    output_path = _resolve_output_path(out_file)
    staged_path = _resolve_staged_path(staged_out, output_path)
    shapes_path = Path(shapes_path)

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
        raise

    log.info("Step 3/3: Promoting staged RDF to final output")
    if keep_staged:
        shutil.copyfile(staged_path, output_path)
        log.info("Copied staged RDF to final output and kept staged file")
    else:
        staged_path.replace(output_path)
        log.info("Moved staged RDF to final output")

    log.info("=== PSGC pipeline complete: %s ===", output_path)
    return output_path


def _resolve_output_path(path: str | Path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    return DEFAULT_OUTPUT_DIR / path


def _resolve_input_path(path: str | Path) -> Path:
    path = Path(path)
    if path.is_file():
        return path
    if path.is_dir():
        return _latest_psgc_datafile(path)
    raise FileNotFoundError(f"PSGC input path does not exist: {path}")


def _latest_psgc_datafile(input_dir: Path) -> Path:
    candidates = _matching_files(input_dir, EXCEL_DATAFILE_PATTERNS)
    if not candidates:
        patterns = ", ".join(EXCEL_DATAFILE_PATTERNS)
        raise FileNotFoundError(
            f"No PSGC Excel datafile found in {input_dir} matching {patterns}"
        )

    latest = max(candidates, key=lambda file: file.stat().st_mtime_ns)
    log.info("Selected latest PSGC datafile: %s", latest)
    return latest


def _matching_files(input_dir: Path, patterns: tuple[str, ...]) -> list[Path]:
    seen: set[Path] = set()
    matches: list[Path] = []
    for pattern in patterns:
        for path in input_dir.glob(pattern):
            if path.is_file() and path not in seen:
                matches.append(path)
                seen.add(path)
    return matches


def _resolve_staged_path(staged_out: str | Path | None, output_path: Path) -> Path:
    if staged_out is None:
        return output_path.with_name(f"{output_path.stem}.staged{output_path.suffix}")

    staged_path = Path(staged_out)
    if staged_path.is_absolute():
        return staged_path
    return DEFAULT_OUTPUT_DIR / staged_path


def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Transform, validate, and publish PSGC RDF."
    )
    parser.add_argument(
        "--input",
        "-i",
        default=str(DEFAULT_INPUT_DIR),
        help=(
            "PSGC workbook or directory to transform. Directory inputs use the "
            "latest Excel file by filesystem metadata timestamp."
        ),
    )
    parser.add_argument(
        "--out",
        "-o",
        default="psgc.ttl",
        help="Final PSGC RDF filename or path. Relative paths resolve under data/rdf/psgc.",
    )
    parser.add_argument(
        "--staged-out",
        default=None,
        help="Staged RDF filename or path to validate before promotion.",
    )
    parser.add_argument(
        "--shapes",
        default=str(DEFAULT_SHAPES_PATH),
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
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    run(
        input_path=args.input,
        out_file=args.out,
        staged_out=args.staged_out,
        shapes_path=args.shapes,
        include_barangay=args.barangay,
        keep_staged=args.keep_staged,
        rdf_format=args.format,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())

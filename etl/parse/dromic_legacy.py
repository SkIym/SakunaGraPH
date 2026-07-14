"""Parser wrapper for pre-2015 DROMIC PDFs with repeated table headers.

The newer :mod:`dromic` parser remains unchanged.  This module repairs only
``number_of_damaged_houses.csv`` from the PDF's native table grid by default.
Use ``--full-parse`` only when an event has not yet been parsed at all.

Older reports commonly repeat the housing-table header on every page.  Docling
occasionally combines those continuation pages into a shifted grid, causing a
damage value to be associated with the next municipality.  pdfplumber's native
grid extraction retains the four source columns reliably for this layout.

Run from ``etl/``:

    python -m parse.dromic_legacy --year 2014 --single "DSWD ... MARIO ...pdf"

The default command requires an existing parsed event folder and changes only
its housing CSV.  Add ``--full-parse`` for a new report, which first delegates
all non-housing parsing to the original parser.
"""

from __future__ import annotations

import argparse
from collections import defaultdict, deque
import json
from pathlib import Path
import re
import tempfile
from typing import Iterable

import pandas as pd
import pdfplumber


# The set is only a fallback for PDFs whose embedded font information is
# incomplete.  In normal cases hierarchy is inferred from the bold/italic
# formatting used by DROMIC's tables.
PHILIPPINE_PROVINCES = {
    "abra", "agusan del norte", "agusan del sur", "aklan", "albay",
    "antique", "apayao", "aurora", "basilan", "bataan", "batanes",
    "batangas", "benguet", "biliran", "bohol", "bukidnon", "bulacan",
    "cagayan", "camarines norte", "camarines sur", "camiguin", "capiz",
    "catanduanes", "cavite", "cebu", "cotabato", "davao de oro",
    "davao del norte", "davao del sur", "davao occidental", "davao oriental",
    "dinagat islands", "eastern samar", "guimaras", "ifugao", "ilocos norte",
    "ilocos sur", "iloilo", "isabela", "kalinga", "la union", "laguna",
    "lanaodel norte", "lanaodel sur", "leyte", "maguindanao", "marinduque",
    "masbate", "misamis occidental", "misamis oriental", "mountain province",
    "negros occidental", "negros oriental", "northern samar", "nueva ecija",
    "nueva vizcaya", "occidental mindoro", "oriental mindoro", "palawan",
    "pampanga", "pangasinan", "quezon", "quirino", "rizal", "romblon",
    "samar", "sarangani", "siquijor", "sorsogon", "south cotabato",
    "southern leyte", "sultan kudarat", "sulu", "surigao del norte",
    "surigao del sur", "tarlac", "tawi-tawi", "zambales", "zamboanga del norte",
    "zamboanga del sur", "zamboanga sibugay",
}

REGION_RE = re.compile(
    r"^(?:REGION\s+[IVXLC]+(?:\s*[-–]\s*[A-Z])?|NCR|CAR|ARMM|BARMM|"
    r"MIMAROPA|CALABARZON|CARAGA|SOCCSKSARGEN|NIR)$",
    re.IGNORECASE,
)


def _clean(value: object) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or None


def _key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _is_housing_table(table: list[list[object]]) -> bool:
    """Identify a table without treating nearby assistance tables as housing."""
    header = " ".join(
        cell for row in table[:8] for item in row if (cell := _clean(item))
    ).lower()
    return "damaged" in header and "house" in header and "totally" in header and "partially" in header


def _first_data_row(table: list[list[object]]) -> int | None:
    for index, row in enumerate(table):
        text = " ".join(cell for item in row if (cell := _clean(item))).lower()
        if "total" in text and "totally" in text and "partially" in text:
            return index + 1
    return None


def _font_styles_by_label(page: pdfplumber.page.Page) -> dict[str, deque[str]]:
    """Map first-column labels to their printed hierarchy style on one page."""
    lines: dict[float, list[dict]] = defaultdict(list)
    for word in page.extract_words(extra_attrs=["fontname"], y_tolerance=2):
        lines[round(float(word["top"]), 1)].append(word)

    styles: dict[str, deque[str]] = defaultdict(deque)
    for words in lines.values():
        location_words = [word for word in words if float(word["x0"]) < 285]
        if not location_words:
            continue
        label = " ".join(word["text"] for word in sorted(location_words, key=lambda item: item["x0"]))
        label = _clean(label)
        if not label:
            continue
        font_names = [str(word.get("fontname", "")).lower() for word in location_words]
        if any("bold" in name for name in font_names):
            style = "bold"
        elif any("italic" in name or "oblique" in name for name in font_names):
            style = "italic"
        else:
            style = "normal"
        styles[_key(label)].append(style)
    return styles


def _style_for(label: str, styles: dict[str, deque[str]]) -> str | None:
    candidates = styles.get(_key(label))
    return candidates.popleft() if candidates else None


def _table_rows(
    table: list[list[object]], styles: dict[str, deque[str]]
) -> Iterable[tuple[str, str | None, str | None, str | None, str | None]]:
    """Yield ``(label, total, totally, partially, printed_style)`` rows."""
    first_row = _first_data_row(table)
    if first_row is None:
        return

    for raw in table[first_row:]:
        cells = [_clean(cell) for cell in raw]
        if len(cells) < 4:
            continue
        label = cells[0]
        if not label:
            continue
        lowered = label.lower()
        if lowered in {"province/city/ municipality", "province/city/municipality"}:
            continue
        if any(token in lowered for token in ("number of", "damaged houses", "totally", "partially")):
            continue
        # The source layout is location | total | totally | partially.  Keeping
        # '-' untouched is intentional: transform.helpers.to_int converts it to
        # null consistently with the rest of the DROMIC pipeline.
        yield label, cells[1], cells[2], cells[3], _style_for(label, styles)


def extract_housing_damage(pdf_path: Path) -> pd.DataFrame:
    """Extract all continuation pages of a legacy housing table in reading order."""
    records: list[dict[str, str | None]] = []
    current_region: str | None = None
    current_province: str | None = None

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            styles = _font_styles_by_label(page)
            for table in page.extract_tables():
                if not _is_housing_table(table):
                    continue
                for label, total, totally, partially, style in _table_rows(table, styles):
                    if label.upper() == "TOTAL":
                        continue
                    if REGION_RE.fullmatch(label):
                        current_region = label
                        current_province = None
                        continue
                    # A municipality can share a province name (e.g. Quezon,
                    # Nueva Vizcaya).  Trust an explicit italic style before
                    # falling back to the province-name list.
                    if style == "bold" or (style is None and label.lower() in PHILIPPINE_PROVINCES):
                        current_province = label
                        continue

                    # A row containing only placeholder dashes is not a
                    # housing-damage observation.  Dropping it here prevents
                    # downstream RDF from creating a node with only a location.
                    if all(value in {None, "-"} for value in (total, totally, partially)):
                        continue

                    # Detail rows inherit their geographic context across page
                    # breaks, precisely where the generic parser used to drift.
                    records.append(
                        {
                            "region": current_region,
                            "province": current_province,
                            "municipality": label,
                            "totalDamagedHouses": total,
                            "totallyDamagedHouses": totally,
                            "partiallyDamagedHouses": partially,
                        }
                    )

    return pd.DataFrame(
        records,
        columns=[
            "region", "province", "municipality", "totalDamagedHouses",
            "totallyDamagedHouses", "partiallyDamagedHouses",
        ],
    )


def _find_output_folder(output_root: Path, pdf_path: Path) -> Path:
    """Find the normal parser's output folder from its provenance JSON."""
    for source_path in output_root.rglob("source.json"):
        try:
            source = json.loads(source_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        report_name = Path(str(source.get("reportName", ""))).name
        # The archive manifest can retain the original .doc filename even
        # though the downloaded input was converted to PDF for parsing.
        if Path(report_name).stem == pdf_path.stem:
            return source_path.parent
    raise FileNotFoundError(
        f"No parsed DROMIC folder references {pdf_path.name!r} under {output_root}"
    )


def repair_housing_csv(pdf_path: Path, output_root: Path) -> Path:
    output_folder = _find_output_folder(output_root, pdf_path)
    housing = extract_housing_damage(pdf_path)
    if housing.empty:
        raise ValueError(f"No damaged-houses table found in {pdf_path.name}")
    destination = output_folder / "number_of_damaged_houses.csv"
    housing.to_csv(destination, index=False)
    print(f"Rebuilt {destination} ({len(housing)} municipality rows)")
    return destination


def _manifest_entries(manifest_path: Path) -> list[dict]:
    """Accept both old list manifests and the current ``{\"entries\": [...]}`` form."""
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    if isinstance(raw, list):
        candidates = raw
    elif isinstance(raw, dict):
        candidates = next(
            (
                raw[key]
                for key in ("entries", "files", "items", "data")
                if isinstance(raw.get(key), list)
            ),
            [],
        )
    else:
        candidates = []
    return [entry for entry in candidates if isinstance(entry, dict)]


def parse_and_repair(pdf_path: Path, output_root: Path) -> Path:
    # Import here so ``--repair-only`` can run without initialising Docling.
    try:
        from . import dromic as base_parser  # type: ignore[import-not-found]
    except ImportError:  # supports ``python parse/dromic_legacy.py`` too
        import dromic as base_parser

    original_extract_metadata = base_parser.extract_report_metadata

    def extract_metadata_with_compatible_manifest(doc, source_pdf: Path, manifest_path: Path):
        # dromic.py expects the manifest itself to be a list.  Supply a
        # temporary list-shaped view so the original metadata logic can remain
        # untouched while newer archive manifests continue to work.
        entries = _manifest_entries(manifest_path)
        if not entries:
            return original_extract_metadata(doc, source_pdf, manifest_path)
        with tempfile.TemporaryDirectory() as temp_dir:
            compatible_manifest = Path(temp_dir) / "manifest.json"
            compatible_manifest.write_text(json.dumps(entries), encoding="utf-8")
            return original_extract_metadata(doc, source_pdf, compatible_manifest)

    base_parser.extract_report_metadata = extract_metadata_with_compatible_manifest
    try:
        base_parser.process_file(pdf_path, output_root)
    finally:
        base_parser.extract_report_metadata = original_extract_metadata
    return repair_housing_csv(pdf_path, output_root)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", required=True)
    parser.add_argument("--single", help="PDF filename within the selected year")
    parser.add_argument(
        "--full-parse",
        action="store_true",
        help="Run the unchanged parser first; required only for unparsed reports.",
    )
    parser.add_argument("--repair-only", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.full_parse and args.repair_only:
        raise ValueError("Use either --full-parse or --repair-only, not both.")
    input_root = Path(f"../data/raw/dromic-new/{args.year}-pdf")
    output_root = Path(f"../data/parsed/dromic/{args.year}")
    files = [input_root / args.single] if args.single else sorted(input_root.glob("*.pdf"))

    for pdf_path in files:
        if not pdf_path.is_file():
            raise FileNotFoundError(pdf_path)
        if args.full_parse:
            parse_and_repair(pdf_path, output_root)
        else:
            repair_housing_csv(pdf_path, output_root)


if __name__ == "__main__":
    main()

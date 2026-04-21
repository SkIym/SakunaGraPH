from typing import Any

from docling.document_converter import DocumentConverter
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import PdfFormatOption
from docling.datamodel.base_models import InputFormat
import pandas as pd
import pdfplumber
import re
import subprocess   
from pathlib import Path
from docx2pdf import convert
from rapidfuzz import fuzz
from datetime import datetime
from dataclasses import dataclass, asdict
import json

RAW_PATH = "../data/raw/dromic/2022/DSWD-DROMIC-Terminal-Report-on-Tropical-Depression-Obet-07-November-2022-6PM.pdf"

PDF_CACHE   = Path("../data/interim/pdf_cache")
output_dir = Path("./dump")

processed_tables: list[tuple[str, pd.DataFrame]] = []

# metadata

@dataclass
class DromicEvent:
    eventName      : str = ""
    eventType      : str = ""
    location       : str = ""
    reportNumber   : int = 0
    reportDate     : str = ""
    startDate      : str = ""
    endDate        : str = ""
    lastUpdateDate : str = ""
    reportName     : str = ""
    recordedBy     : str = "DROMIC"
    obtainedDate   : str = ""
    reportLink     : str = ""
    downloadUrl    : str = ""
    page           : int = 0
    remarks        : str = ""

def extract_report_metadata(doc, pdf_path: Path, manifest_path: Path) -> DromicEvent:
    first_page_texts = [
        item.text.strip()
        for item in doc.texts
        if item.prov and item.prov[0].page_no == 1
        and item.text.strip()
    ]
    full_text = "\n".join(first_page_texts)

    manifest_entry = None
    if manifest_path.exists():
        with open(manifest_path) as f:
            raw = json.load(f)
        manifest_entry = next(
            (e for e in raw if e.get("filename") == pdf_path.name), None
        )

    event_name = ""
    title_match = re.search(
        r'DSWD\s+DROMIC\s+(?:Situation\s+Report|Terminal\s+Report|Report)\s+'
        r'(?:No\.\s*\d+\s+)?on\s+(?:the\s+)?(.+?)(?:\n|,\s*\d{2}\s+\w+|\s+\d{2}\s+\w+)',
        full_text, re.IGNORECASE
    )
    if title_match:
        event_name = title_match.group(1).strip()

    EVENT_TYPE_KEYWORDS = {
        "Tropical Depression": "Tropical Depression",
        "Tropical Storm":      "Tropical Storm",
        "Typhoon":             "Typhoon",
        "Earthquake":          "Earthquake",
        "Flood":               "Flood",
        "Landslide":           "Landslide",
        "Armed Conflict":      "Armed Conflict",
        "Disorganization":      "Armed Conflict",
        "Fire":                "Fire",
        "Volcanic":            "Volcanic Eruption",
        "Demolition":          "Demolition Incident",
    }
    event_type = "Unknown"
    for kw, label in EVENT_TYPE_KEYWORDS.items():
        if kw.lower() in event_name.lower() or kw.lower() in full_text[:500].lower():
            event_type = label
            break

    report_number = 0
    num_match = re.search(r'(?:Report|Situation Report)\s+No[.\s]+(\d+)', full_text, re.IGNORECASE)
    if num_match:
        report_number = int(num_match.group(1))

    report_date = ""
    date_match = re.search(
        r'(\d{1,2}\s+\w+\s+\d{4}),?\s*\d{1,2}(?:AM|PM)',
        full_text, re.IGNORECASE
    )
    if date_match:
        try:
            report_date = datetime.strptime(date_match.group(1), "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            report_date = date_match.group(1)

    location = ""
    loc_match = re.search(
        r'(?:in|at)\s+(Brgy\.[^,\n]+(?:,\s*[^,\n]+){1,3})',
        full_text, re.IGNORECASE
    )
    if loc_match:
        location = loc_match.group(1).strip()

    MONTHS = (
        "January|February|March|April|May|June|July|August|"
        "September|October|November|December"
    )

    def parse_dmy(day: str, month: str, year: str) -> str:
        try:
            return datetime.strptime(f"{day} {month} {year}", "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            return ""

    remarks = ""
    overview_match = re.search(
        r'(?:Situation Overview|Background)\s*\n+(.+?)(?:\n{2,}|\Z)',
        full_text, re.IGNORECASE | re.DOTALL
    )
    if overview_match:
        remarks = re.sub(r'\s+', ' ', overview_match.group(1)).strip()
    else:
        source_match = re.search(r'Source:\s*(.+)', full_text)
        if source_match:
            remarks = source_match.group(1).strip()

    # Combine for better temporal extraction
    text_for_dates = full_text + "\n" + remarks

    MONTHS = (
        "January|February|March|April|May|June|July|August|"
        "September|October|November|December"
    )

    def parse_dmy(day: str, month: str, year: str) -> str:
        try:
            return datetime.strptime(f"{day} {month} {year}", "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            return ""

    start_date, end_date = "", ""

    # ── Range detection ─────────────────────────────────────────────────────
    range_match = re.search(
        rf'(?:from|between)\s+(\d{{1,2}})\s+({MONTHS})?\s+(?:and|to)\s+(\d{{1,2}})\s+({MONTHS})\s+(\d{{4}})',
        text_for_dates, re.IGNORECASE
    )
    if range_match:
        d1, m1, d2, m2, yr = range_match.groups()
        start_date = parse_dmy(d1, m1 or m2, yr)
        end_date   = parse_dmy(d2, m2, yr)
    else:
        # ── Single-date fallback ────────────────────────────────────────────
        on_match = re.search(
            rf'On\s+(\d{{1,2}})\s+({MONTHS})\s+(\d{{4}})',
            text_for_dates, re.IGNORECASE
        )

        if on_match:
            start_date = parse_dmy(*on_match.groups())
        else:
            # Handle: "On September 26, 2025"
            on_match_alt = re.search(
                rf'On\s+({MONTHS})\s+(\d{{1,2}}),\s*(\d{{4}})',
                text_for_dates, re.IGNORECASE
            )
            if on_match_alt:
                month, day, year = on_match_alt.groups()
                start_date = parse_dmy(day, month, year)

    # ── Final fallback ──────────────────────────────────────────────────────
    if not end_date and start_date:
        end_date = start_date

    return DromicEvent(
        eventName    = event_name,
        eventType    = event_type,
        location     = location,
        reportNumber = report_number,
        lastUpdateDate   = report_date,
        startDate    = start_date,
        endDate      = end_date,
        remarks      = remarks,
        reportName   = manifest_entry["filename"]     if manifest_entry else pdf_path.name,
        reportLink   = manifest_entry["post_url"]     if manifest_entry else "",
        downloadUrl  = manifest_entry["download_url"] if manifest_entry else "",
        obtainedDate = manifest_entry["downloaded_at"] if manifest_entry else "",
        page         = manifest_entry["page"]         if manifest_entry else 0,
        recordedBy   = "DROMIC",
    )


METADATA_KEYS = {"eventName", "eventType", "location", "reportNumber",
                 "reportDate", "startDate", "endDate", "remarks"}

def generate_json(event: DromicEvent, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    event_dict = asdict(event)
    metadata = {k: v for k, v in event_dict.items() if k in METADATA_KEYS}
    source   = {k: v for k, v in event_dict.items() if k not in METADATA_KEYS}
    with open(output_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)
    with open(output_dir / "source.json", "w") as f:
        json.dump(source, f, indent=4)

# 0. Preprocess docx to pdf

def convert_docx_to_pdf(docx_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = output_dir / docx_path.with_suffix(".pdf").name
    convert(str(docx_path), str(pdf_path))
    return pdf_path

def resolve_pdf(path: Path, pdf_cache_dir: Path) -> Path:
    """Return path as-is if PDF, otherwise convert and return the PDF path."""
    if path.suffix.lower() == ".pdf":
        return path
    elif path.suffix.lower() == ".docx":
        pdf_path = pdf_cache_dir / path.with_suffix(".pdf").name
        if not pdf_path.exists():  # idempotent — skip if already converted
            convert_docx_to_pdf(path, pdf_cache_dir)
        return pdf_path
    else:
        raise ValueError(f"Unsupported format: {path.suffix}")
    
PDF_PATH = resolve_pdf(Path(RAW_PATH), PDF_CACHE)
# ── 1. Docling setup ──────────────────────────────────────────────────────────

pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = False
pipeline_options.do_table_structure = True
pipeline_options.table_structure_options.do_cell_matching = True
pipeline_options.images_scale = 1.0

converter = DocumentConverter(
    format_options={
        InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
    }
)

result = converter.convert(PDF_PATH)
doc = result.document

manifest_path = PDF_PATH.parent / "manifest.json"  # or per-file json
event = extract_report_metadata(doc, PDF_PATH, manifest_path)
generate_json(event, output_dir)

print(f"Tables found: {len(doc.tables)}\n")


# ── 2. Font analysis helpers ──────────────────────────────────────────────────

def is_bold(fontname: str) -> bool:
    return "bold" in fontname.lower()

def is_italic(fontname: str) -> bool:
    return any(x in fontname.lower() for x in ["italic", "oblique"])

EXPECTED_SIZE_MIN = 8.0
EXPECTED_SIZE_MAX = 11.0  # excludes the size=16 heading bleed

def classify_level(words: list[dict]) -> str | None:
    if not words:
        return None

    words = [w for w in words if EXPECTED_SIZE_MIN <= w["size"] <= EXPECTED_SIZE_MAX]
    if not words:
        return None

    text = " ".join(w["text"] for w in words).strip()
    bold_count   = sum(1 for w in words if is_bold(w["fontname"]))
    italic_count = sum(1 for w in words if is_italic(w["fontname"]))
    total        = len(words)
    majority_bold   = bold_count   > total / 2
    majority_italic = italic_count > total / 2
    all_caps = text == text.upper() and any(c.isalpha() for c in text)

    # ── DEBUG ──
    print(f"    classify → text={text!r} bold={bold_count}/{total} italic={italic_count}/{total} all_caps={all_caps} fonts={[w['fontname'] for w in words]}")

    if majority_italic:
        return "municipality"
    elif majority_bold and all_caps:
        return "region"
    elif majority_bold and not all_caps:
        return "province"
    return None

def find_location_column(df: pd.DataFrame) -> int | None:
    """
    Find the column index whose header contains all three keywords:
    'region', 'province', and 'municipality' (case-insensitive).
    Falls back to checking if any single column header contains at least one.
    """
    keywords = {"region", "province", "municipality"}
    combined_header = " ".join(str(c) for c in df.columns).lower()

    # Check if all three keywords appear somewhere across all headers combined
    if all(k in combined_header for k in keywords):
        # Find the specific column that contains the most keyword matches
        best_col, best_score = 0, 0
        for i, col in enumerate(df.columns):
            score = sum(1 for k in keywords if k in str(col).lower())
            if score > best_score:
                best_col, best_score = i, score
        return best_col

    return None  # no location column found in this table


# ── 3. pdfplumber cell font extraction ───────────────────────────────────────

def extract_cell_words_on_page(plumber_page, cell_bbox):
    x0, top, x1, bottom = cell_bbox
    INSET = 1.5  # points — keeps us away from cell borders
    cropped = plumber_page.crop((x0 + INSET, top + INSET, x1 - INSET, bottom - INSET))
    return cropped.extract_words(extra_attrs=["fontname", "size"])

# Caption-content consistency keywords
CAPTION_CONTENT_KEYWORDS: list[tuple[str, str]] = [
    ("damaged house",        "DAMAGED"),
    ("cost of assistance",   "COST"),
    ("affected",       "AFFECTED"),
    ("displaced",            "DISPLACED"),
    ("evacuation",           "EVACUATION"),
]

def validate_caption(caption: str, df: pd.DataFrame) -> str:
    """
    Check if the caption is consistent with the table's columns.
    If the caption mentions a topic but the columns don't match,
    clear the caption and let it fall back to column-based naming.
    """
    if not caption.strip():
        return caption

    caption_lower = caption.lower()
    col_text = " ".join(df.columns).upper()

    for cap_kw, col_kw in CAPTION_CONTENT_KEYWORDS:
        if cap_kw in caption_lower:
            if col_kw not in col_text:
                print(f"  [caption mismatch] Caption mentions '{cap_kw}' but columns have no '{col_kw}' — clearing caption")
                return ""
            break  # caption matches columns, keep it

    return caption

# ── 4. Main loop ──────────────────────────────────────────────────────────────

with pdfplumber.open(PDF_PATH) as pdf:

    for i, table in enumerate(doc.tables):
        caption    = table.caption_text(doc)
        prov_pages = [p.page_no for p in table.prov]
        print(f"=== Table {i+1} | pages={prov_pages} | caption={caption!r} ===")

        df = table.export_to_dataframe(doc)
        caption = validate_caption(caption, df)  # validate before processing

        # ── Find the location column ──────────────────────────────────────────
        loc_col_idx = find_location_column(df)
        if loc_col_idx is None:
            print("  [skip] No location column detected.\n")
            df.to_csv(f"./dump/{i}.csv", index=False)
            continue

        loc_col_name = df.columns[loc_col_idx]
        print(f"  Location column: index={loc_col_idx}, name={loc_col_name!r}")

        # ── Build a (page_no → pdfplumber page) map for this table's pages ───
        plumber_pages = {
            pno: pdf.pages[pno - 1]   # pdfplumber is 0-indexed
            for pno in prov_pages
        }

        # ── Walk Docling's table cells to get bounding boxes per row ─────────
        # doc.tables[i].data is a TableData with a grid of TableCell objects
        # Each TableCell has .bbox (in Docling's page coordinate space) and
        # .prov entries that tell us which page it's on.

        # ── Build row_bbox_map ────────────────────────────────────────────────────────
        row_bbox_map: dict[int, tuple[int, tuple]] = {}

        SKIP_TEXTS = {"GRAND TOTAL", "GRAND  TOTAL"}  # handle double-space variants

        for cell in table.data.table_cells:
            if cell.column_header:
                continue
            if cell.start_col_offset_idx != loc_col_idx:
                continue
            if cell.text.strip() == str(loc_col_name).strip():
                continue
            if cell.text.strip().upper() in SKIP_TEXTS:          # ← new
                continue

            row_idx = cell.start_row_offset_idx
            bbox    = cell.bbox
            row_bbox_map[row_idx] = (prov_pages[0], (bbox.l, bbox.t, bbox.r, bbox.b))

        # Derive offset: minimum row_idx in the map = first data row → maps to df row 0
        if not row_bbox_map:
            print("  [skip] No data cells found.\n")
            df.to_csv(f"./dump/{i}.csv", index=False)
            continue

        # ── Classify each data row by font ────────────────────────────────────
        # We offset by 1 if Docling's df has a header row at row 0
        # (Docling exports the first row as column headers by default)
        # so Docling cell row_idx=1 → df row index 0, etc.

        cell_text_to_row_idx: dict[str, int] = {}
        for row_idx, (page_no, bbox) in sorted(row_bbox_map.items()):
            plumber_page = plumber_pages.get(page_no)
            words = extract_cell_words_on_page(plumber_page, bbox)
            cell_text = " ".join(w["text"] for w in words).strip()
            cell_text_to_row_idx[cell_text] = row_idx

        # Find the first df row whose text appears in the bbox map
        min_row_idx = None
        for df_row_i in range(len(df)):
            candidate = str(df.iloc[df_row_i, loc_col_idx]).strip()
            if candidate in cell_text_to_row_idx:
                min_row_idx = cell_text_to_row_idx[candidate] - df_row_i
                break

        if min_row_idx is None:
            min_row_idx = min(row_bbox_map.keys())  # fallback

        # ── classify each cell ────────────────────────────────────────────────
        level_per_df_row: dict[int, str | None] = {}

        for row_idx, (page_no, bbox) in row_bbox_map.items():
            plumber_page = plumber_pages.get(page_no)
            if plumber_page is None:
                continue
            words = extract_cell_words_on_page(plumber_page, bbox)
            level = classify_level(words)
            df_row = row_idx - min_row_idx
            if 0 <= df_row < len(df):
                level_per_df_row[df_row] = level

        # ── Forward-fill region / province / municipality ─────────────────────
        region_col       = []
        province_col     = []
        municipality_col = []

        current_region   = None
        current_province = None
        current_muni     = None

        for df_row in range(len(df)):
            level = level_per_df_row.get(df_row)
            text  = str(df.iloc[df_row, loc_col_idx]).strip()

            if level == "region":
                current_region   = text
                current_province = None
                current_muni     = None
            elif level == "province":
                current_province = text
                current_muni     = None
            elif level == "municipality":
                current_muni     = text

            region_col.append(current_region)
            province_col.append(current_province)
            municipality_col.append(current_muni)

        df.insert(0, "region",       region_col)
        df.insert(1, "province",     province_col)
        df.insert(2, "municipality", municipality_col)

        processed_tables.append((caption or "", df))
        print(df.to_string())
        print()

    
def slugify(text: str) -> str:
    """Convert caption to a safe filename."""
    text = text.strip()
    text = re.sub(r'[^\w\s-]', '', text)   # remove non-word chars except hyphen
    text = re.sub(r'[\s]+', '_', text)      # spaces → underscores
    text = re.sub(r'_+', '_', text)         # collapse multiple underscores
    return text.strip('_')

def normalize_col(col: str) -> str:
    col = col.strip()
    col = re.sub(r'\*+', '', col)
    col = re.sub(r'\.+$', '', col)
    col = re.sub(r'\(ECs?\)', '', col)
    col = re.sub(r'[\s\.]+', ' ', col)
    return col.strip().upper()

def col_signature(df: pd.DataFrame) -> frozenset[str]:
    return frozenset(normalize_col(c) for c in df.columns)

LOCATION_COLS = {"region", "province", "municipality"}

def col_similarity(df1: pd.DataFrame, df2: pd.DataFrame) -> float:
    cols1 = [normalize_col(c) for c in df1.columns
             if c.lower() not in LOCATION_COLS]
    cols2 = [normalize_col(c) for c in df2.columns
             if c.lower() not in LOCATION_COLS]
    if not cols1 or not cols2:
        return 0.0
    scores: list[float] = []
    for c1 in cols1:
        best = max(fuzz.token_set_ratio(c1, c2) for c2 in cols2)
        scores.append(best / 100)
    return sum(scores) / len(scores)

def fix_caption_sequence(processed_tables: list[tuple[str, pd.DataFrame]]) -> list[tuple[str, pd.DataFrame]]:
    result = list(processed_tables)
    captions = [c for c, _ in result]

    numbered = [(i, int(m.group(1))) for i, c in enumerate(captions)
                if (m := re.search(r'Table\s+(\d+)', c))]

    if len(numbered) < 2:
        return result

    for idx in range(1, len(numbered)):
        pos_prev, num_prev = numbered[idx - 1]
        pos_curr, num_curr = numbered[idx]

        if num_curr - num_prev <= 1:
            continue

        # Just log the gap — don't assign any caption
        captionless_between = [
            i for i in range(pos_prev + 1, pos_curr)
            if not captions[i].strip()
        ]
        if captionless_between:
            print(f"  [caption gap] Tables {num_prev+1}–{num_curr-1} have no caption, will use column names as filename")

    return result  # unchanged

processed_tables = fix_caption_sequence(processed_tables)
# ── grouping logic: merge by signature OR by high similarity to adjacent table ─
SIMILARITY_THRESHOLD = 0.95

print("\n=== processed_tables before grouping ===")
for i, (caption, df) in enumerate(processed_tables):
    print(f"  [{i}] caption={caption!r} | cols={list(df.columns)[:3]}... | rows={len(df)}")

print("\n=== similarity matrix ===")
for i in range(len(processed_tables)):
    for j in range(i+1, len(processed_tables)):
        df1 = processed_tables[i][1]
        df2 = processed_tables[j][1]
        sim = col_similarity(df1, df2)
        if sim > 0.3:  # only show non-trivial similarities
            print(f"  [{i}] vs [{j}]: similarity={sim:.2f}")

groups: dict[int, list[tuple[str, pd.DataFrame]]] = {}
group_representatives: dict[int, pd.DataFrame] = {}
group_last_idx: dict[int, int] = {}  # track last position added to each group
group_id = 0

for pos, (caption, df) in enumerate(processed_tables):
    merged = False

    for gid, rep_df in group_representatives.items():
        # Only consider merging if this table is adjacent to the last one in group
        if pos - group_last_idx[gid] > 1:
            continue  # gap between them — don't merge
        # Don't merge if this table has its own non-generic caption
        # and the group already has a captioned table
        group_has_caption = any(
            bool(re.search(r'Table\s+\d+\.\s+\w', c))
            for c, _ in groups[gid]
        )
        this_has_caption = bool(re.search(r'Table\s+\d+\.\s+\w', caption))
        if group_has_caption and this_has_caption:
            continue  # both have real captions — they're different tables

        sim = col_similarity(df, rep_df)
        if sim >= SIMILARITY_THRESHOLD:
            groups[gid].append((caption, df))
            group_last_idx[gid] = pos
            merged = True
            break

    if not merged:
        groups[group_id] = [(caption, df)]
        group_representatives[group_id] = df
        group_last_idx[group_id] = pos
        group_id += 1

def align_columns(base_df: pd.DataFrame, other_df: pd.DataFrame) -> pd.DataFrame:
    base_cols  = list(base_df.columns)
    base_norms = [normalize_col(c) for c in base_cols]
    used_targets: set[str] = set()
    rename_map: dict[str, str] = {}

    for col in other_df.columns:
        norm = normalize_col(col)
        best_col, best_score = None, 0

        for i, bc in enumerate(base_norms):
            if base_cols[i] in used_targets:
                continue
            score = fuzz.token_set_ratio(norm, bc)
            if score > best_score:
                best_score = score
                best_col   = base_cols[i]

        if best_score >= 85 and best_col and best_col != col:
            rename_map[col]  = best_col
            used_targets.add(best_col)

    result = other_df.rename(columns=rename_map)
    # Dedup columns within this df before returning
    result = result.loc[:, ~result.columns.duplicated()]
    return result


# ── save each group ───────────────────────────────────────────────────────────
for gid, items in groups.items():
    caption = next((c for c, _ in items if c.strip()), "")

    if not caption:
        sample_cols = [
            normalize_col(c) for c in items[0][1].columns
            if c.lower() not in LOCATION_COLS
            and normalize_col(c) not in {
                normalize_col(lc) for lc in ["REGION / PROVINCE / MUNICIPALITY",
                                            "REGION / PROVINCE / MUNICIPALITY."]
            }
        ]
        caption = "_".join(sample_cols) if sample_cols else f"table_{gid}"

    filename = slugify(caption) + ".csv"

    # Dedup base_df too before using as reference
    base_caption, base_df = items[0]
    base_df = base_df.loc[:, ~base_df.columns.duplicated()]

    aligned = [base_df] + [align_columns(base_df, df) for _, df in items[1:]]

    # Reindex each df to base columns only before concat to avoid schema drift
    base_cols = list(base_df.columns)
    aligned = [df.reindex(columns=base_cols) for df in aligned]

    merged_df = pd.concat(aligned, ignore_index=True)

    # Handle duplicate filename
    counter = 1
    original = filename
    while (output_dir / filename).exists():
        filename = slugify(caption) + f"_{counter}.csv"
        counter += 1

    merged_df.to_csv(output_dir / filename, index=False)
    print(f"Saved: {filename} ({len(merged_df)} rows from {len(items)} table(s))")
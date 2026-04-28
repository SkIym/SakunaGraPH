import argparse
import os
from typing import Any
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode, TableStructureOptions
from docling.datamodel.base_models import InputFormat
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
import pandas as pd
import pdfplumber
import re  
from pathlib import Path
from pdfplumber.page import Page
from rapidfuzz import fuzz
from datetime import datetime
from dataclasses import dataclass, asdict
import json
import torch
from docling_core.types.doc.document import DoclingDocument


os.environ["PYTHONUTF8"] = "1"
SIMILARITY_THRESHOLD = 0.95
output_dir = Path("./dump")

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
    page           : int | None = 0
    remarks        : str = ""

def parse_dmy(day: str, month: str, year: str) -> str:
    try:
        return datetime.strptime(f"{day} {month} {year}", "%d %B %Y").strftime("%Y-%m-%d")
    except ValueError:
        return ""

    
def extract_report_metadata(doc: DoclingDocument, pdf_path: Path, manifest_path: Path) -> DromicEvent:
    first_page_texts = [
        item.text.strip()
        for item in doc.texts
        if item.prov and item.prov[0].page_no == 1
        and item.text.strip()
    ]
    full_text = "\n".join(first_page_texts)

    manifest_entry = None
    if manifest_path.exists():
        with open(manifest_path, encoding="utf-8", errors="replace") as f:
            raw = json.load(f)

        manifest_entry = next(
            (e for e in raw if Path(e.get("filename")).stem == pdf_path.stem), None
        )

    event_name = ""
    title_match = re.search(
        r'DSWD\s+DROMIC\s+(?:Situation\s+Report|Terminal\s+Report|Report)\s+'
        r'(?:No\.?\s*\d+\s+|#\d+\s+)?'          # ← add #N variant
        r'(?:on\s+)?(?:the\s+)?(.+?)(?:\n|,\s*\d{2}\s+\w+|\s+\d{2}\s+\w+)',
        full_text, re.IGNORECASE
    )

    if title_match:
        event_name = title_match.group(1).strip()


    EVENT_TYPE_KEYWORDS = {
        "Tropical Depression": "Tropical Cyclone",
        "Tropical Storm":      "Tropical Cyclone",
        "Typhoon":             "Tropical Cyclone",
        "Earthquake":          "Ground Movement",
        "Tornado":               "Tornado",
        "Flood":               "Flood",
        "Flashflood":          "Flashflood",
        "Flash Flood":          "Flashflood",
        "Landslide":           "Landslide",
        "Mudslide":            "Mudslide",
        "Armed Conflict":      "Armed Conflict",
        "Monsoon":              "Storm General",
        "Tail End":             "Storm General",
        "Continuous rain":      "Storm General",
        "Lightning":      "Thunderstorm",
        "strong wind":      "Severe weather",
        "Disorganization":      "Armed Conflict",
        "Fire":                "Fire",
        "Volcanic":            "VolcanicActivityGeneral",
        "Volcano":            "VolcanicActivityGeneral",
        "Demolition":          "Collapse",
        "Eruption":             "VolcanicActivityGeneral",

    }
    event_type = ""
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

    # Try Sitio/Purok/Brgy. in event_name first
    loc_match = re.search(
        r'(?:in|at)\s+((?:Sitio|Purok|Brgy\.)[^,\n]+(?:,\s*[^,\n]+){1,4})',
        event_name, re.IGNORECASE
    )
    # Try plain "in location" from event_name (e.g. "in Mabini, Batangas")
    if not loc_match:
        loc_match = re.search(
            r'\bin\s+(.+)$',
            event_name.strip(), 
            re.IGNORECASE
        )
    # Fall back to full_text with Brgy/Sitio
    if not loc_match:
        loc_match = re.search(
            r'(?:in|at)\s+((?:Sitio|Purok|Brgy\.)[^,\n]+(?:,\s*[^,\n]+){1,4})',
            full_text, re.IGNORECASE
        )
    if loc_match:
        location = loc_match.group(1).strip()

    MONTHS = (
        "January|February|March|April|May|June|July|August|"
        "September|October|November|December"
    )

    HEADER_PATTERNS: list[str | re.Pattern[str]] = [
        r'Republic of the Philippines\s*',
        r'Disaster Response Operations Monitoring and Information Center\s*',
        r'DISASTER RESPONSE ASSISTANCE AND MANAGEMENT BUREAU[^\n]*\n',
        r'Page\s+\d+\s+of\s+\d+\|[^\n]*\n',
        r'DSWD\s+DROMIC\s+(?:Situation\s+Report|Terminal\s+Report)[^\n]*\n',
        r'(?:Situation\s+Report|Terminal\s+Report)\s+on\s+[^\n]*\n',
        r'as\s+of\s+\d{1,2}\s+\w+\s+\d{4}[^\n]*\n',
        event_name
    ]
    remarks = ""
    clean_text = full_text

    # print(full_text)
    for pat in HEADER_PATTERNS:
        clean_text = re.sub(pat, '', clean_text, flags=re.IGNORECASE)
    clean_text = clean_text.strip()
    # print(clean_text)

    # Tier 1: explicit section headers
    overview_match = re.search(
        r'(?:Situation Overview|Background|SUMMARY)\s*\n+(.+?)(?=\n{2,}|Source:|^\s*\d+\.|$)',
        clean_text, re.IGNORECASE | re.DOTALL | re.MULTILINE
    )
    if overview_match:
        raw = overview_match.group(1).strip()
        first_para = re.split(r'\n{2,}|Source:', raw)[0].strip()
        if len(first_para.split()) < 10:
            paras = re.split(r'\n{2,}|Source:', raw)
            first_para = next((p for p in paras if len(p.split()) >= 10), first_para)
        remarks = re.sub(r'\s+', ' ', first_para).strip()

        # print("tier 1: ", remarks)


    # Tier 2: "This is the final report..." or "On/At <date>..." pattern
    if not remarks:
        match = re.search(
            r'(This\s+is\s+the\s+final\s+report.+?)(?=\s+\d+\.\s+[A-Z]|Table\s+\d+|Source:|\Z)',
            clean_text,
            re.IGNORECASE | re.DOTALL
        )

        if match:
            remarks = re.sub(r'\s+', ' ', match.group(1)).strip()
        # print("tier 2: ", remarks)

    # Tier 3: first meaningful paragraph not matching header/numbering patterns
    if not remarks:
        sentences = re.split(r'(?<=[.!?])\s+', clean_text)

        for s in sentences:
            s_clean = re.split(r'Source:', s)[0].strip()

            if (
                len(s_clean.split()) > 12
                and not s_clean.isupper()
                and not re.match(r'^\d+\.', s_clean)          # skip "1. ..."
                and not re.match(r'^Table\s+\d+', s_clean)    # skip table titles
                and not re.match(r'^Page\s+\d+', s_clean)     # skip page headers
                and 'DROMIC' not in s_clean[:40]
            ):
                remarks = re.sub(r'\s+', ' ', s_clean).strip()
                break

        # print("tier 3:", remarks)
        

    # Tier 4: fallback
    if not remarks and event_name:
        remarks = f"This report covers {event_name.lower()}."

    # Combine for better temporal extraction
    text_for_dates = full_text + "\n" + remarks


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
            rf'(?:On|last)\s+(\d{{1,2}})\s+({MONTHS})\s+(\d{{4}})',
            text_for_dates, re.IGNORECASE
        )

        if on_match:
            start_date = parse_dmy(*on_match.groups())
        else:
            # Handle: "On September 26, 2025"
            on_match_alt = re.search(
                rf'(?:On|last)\s+({MONTHS})\s+(\d{{1,2}}),\s*(\d{{4}})',
                text_for_dates, re.IGNORECASE
            )
            if on_match_alt:
                month, day, year = on_match_alt.groups()
                start_date = parse_dmy(day, month, year)

    # ── Final fallback ──────────────────────────────────────────────────────
    if not start_date:
        start_date = report_date
        end_date = report_date

    if not end_date and start_date:
        end_date = start_date

    return DromicEvent(
        eventName    = event_name if event_name else make_folder_name(manifest_entry["filename"] if manifest_entry else pdf_path.name),
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
        page         = manifest_entry["page"]         if manifest_entry and "page" in manifest_entry else None,
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

def safe_filename(name: str) -> str:
    return re.sub(r'[^\w\-. ]', '', name)

def is_bold(fontname: str) -> bool:
    return "bold" in fontname.lower()

def is_italic(fontname: str) -> bool:
    return any(x in fontname.lower() for x in ["italic", "oblique"])

EXPECTED_SIZE_MIN = 7.5
EXPECTED_SIZE_MAX = 13.5  # excludes the size=16 heading bleed

def classify_level(words: list[dict[Any, Any]]) -> str | None:
    if not words:
        return None

    words = [w for w in words if EXPECTED_SIZE_MIN <= w["size"] <= EXPECTED_SIZE_MAX]
    if not words:
        return None

    text = " ".join(w["text"] for w in words).strip()
    x0 = min(w["x0"] for w in words)  # ← indentation signal
    bold_count   = sum(1 for w in words if is_bold(w["fontname"]))
    italic_count = sum(1 for w in words if is_italic(w["fontname"]))
    total        = len(words)
    majority_bold   = bold_count   > total / 2
    majority_italic = italic_count > total / 2
    all_caps = text == text.upper() and any(c.isalpha() for c in text)

    # ── DEBUG ──
    # print(f"    classify → text={text!r} bold={bold_count}/{total} italic={italic_count}/{total} all_caps={all_caps} fonts={[w['fontname'] for w in words]}")

    if any(k in text.lower() for k in ["region", "luzon", "mindanao", "visayas"]):
        return "region"
    
    if text.lower() in ["caraga", "ncr", "armm", "barmm", "car", "mimaropa", "calabarzon"]:
        return "region"

    if any(x in text.lower() for x in ["city", "municipality", "mun."]):
        return "municipality"

    if majority_bold and all_caps:
        return "region"

    if majority_bold:
        return "province"

    # fallback: indentation-based
    if majority_italic or x0 > 50 :   # tune threshold
        return "municipality"

    return None

def find_location_column(df: pd.DataFrame) -> int | None:
    """
    Handles variants like 'municipality' and 'municipalities'.
    Priority:
    1. region + province + municipality
    2. province + municipality
    3. any single keyword
    """

    keyword_groups = {
        "region": {"region", "regions"},
        "province": {"province", "provinces"},
        "municipality": {"municipality", "municipalities", "city", "cities"}
    }

    priority_sets = [
        {"region", "province", "municipality"},
        {"province", "municipality"},
    ]

    def match_score(text: str, keys: set[str]) -> int:
        text = text.lower()
        score = 0
        for key in keys:
            if any(alias in text for alias in keyword_groups[key]):
                score += 1
        return score

    combined_header = " ".join(str(c) for c in df.columns).lower()

    # Priority matching
    for key_set in priority_sets:
        if all(any(alias in combined_header for alias in keyword_groups[k]) for k in key_set):
            best_col, best_score = None, 0
            for i, col in enumerate(df.columns):
                score = match_score(str(col), key_set)
                if score > best_score:
                    best_col, best_score = i, score
            if best_col is not None:
                return best_col

    # Final fallback: any keyword
    best_col, best_score = None, 0
    for i, col in enumerate(df.columns):
        score = match_score(str(col), set(keyword_groups.keys()))
        if score > best_score:
            best_col, best_score = i, score

    return best_col if best_score > 0 else None

def extract_cell_words_on_page(plumber_page: Page, cell_bbox: tuple[float, float, float, float]):
    l, t, r, b = cell_bbox

    x0, x1 = min(l, r), max(l, r)
    y0, y1 = min(t, b), max(t, b)
    
    cell_height = y1 - y0
    
    # For very short cells, use a proportional vertical inset
    # to avoid bleeding into adjacent rows
    v_inset = max(1.5, cell_height * 0.15)  # 15% of height, min 1.5pt
    h_inset = 1.5
    
    try:
        cropped = plumber_page.crop((
            x0 + h_inset,
            y0 + v_inset,
            x1 - h_inset,
            y1 - v_inset,
        ))
        return cropped.extract_words(extra_attrs=["fontname", "size"])
    
    except ValueError:
        # Fallback if the cell is too small for the insets
        return plumber_page.within_bbox((x0, y0, x1, y1)).extract_words(extra_attrs=["fontname", "size"])

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
    col_text = " ".join(str(c) for c in df.columns).upper()

    if any(k in caption_lower for k in ["page", "note"]):
        return ""
    
    inferred_caption = infer_table_name([caption], 0)
    if inferred_caption != caption: # if it matched any key words
        # Verify the inferred name is consistent with the actual column content.
        # A caption may mention e.g. "assistance" while the table is really about
        # damaged houses — the column check catches that mismatch.
        inferred_lower = inferred_caption.replace('_', ' ')
        for cap_kw, col_kw in CAPTION_CONTENT_KEYWORDS:
            if cap_kw in inferred_lower:
                if col_kw not in col_text:
                    print(f"  [caption mismatch] Inferred '{inferred_caption}' from caption "
                          f"but no '{col_kw}' in columns — clearing caption")
                    return ""
                break
        return inferred_caption

    for cap_kw, col_kw in CAPTION_CONTENT_KEYWORDS:
        if cap_kw in caption_lower:
            if col_kw not in col_text:
                print(f"  [caption mismatch] Caption mentions '{cap_kw}' but columns have no '{col_kw}' — clearing caption")
                return ""
            break  # caption matches columns, keep it

    return caption

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

def is_location_col(col: str) -> bool:
    """True if this column is the raw location column (not the derived region/province/municipality)."""
    col_lower = col.lower()
    return (
        col_lower in LOCATION_COLS  # derived cols
        or all(k in col_lower for k in ["region", "province", "municipality"])  # raw header
        or (  # partial match for corrupted continuation headers like "MUNICIPALITY.Loboc"
            any(k in col_lower for k in ["region", "province", "municipality"])
            and col_lower not in LOCATION_COLS
        )
    )

def col_similarity(df1: pd.DataFrame, df2: pd.DataFrame) -> float:
    cols1 = [normalize_col(c) for c in df1.columns if not is_location_col(c)]
    cols2 = [normalize_col(c) for c in df2.columns if not is_location_col(c)]
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

def make_folder_name(filename: str, report_date: str = "") -> str:
    name = os.path.splitext(filename)[0]
    year_m = re.search(r'\b(20\d{2})\b', name)
    year   = year_m.group(1) if year_m else ""

    desc = re.sub(r'^DSWD[\s\-]DROMIC[\s\-].*?[\s\-]on[\s\-](?:the[\s\-])?', '', name, flags=re.IGNORECASE)
    desc = re.sub(r'[\s\-]*as[\s\-]of[\s\-].*$', '', desc, flags=re.IGNORECASE)
    desc = re.sub(r'[\s\-]*\d{1,2}[\s\-][A-Za-z]+[\s\-]20\d{2}.*$', '', desc, flags=re.IGNORECASE)
    desc = re.sub(r'[\s\-]+[1-9A-Z]$', '', desc)
    desc = re.sub(r'[#<>:"/\\|?*,]', '', desc)
    desc = desc.replace('-', ' ').strip()
    desc = re.sub(r'\s+', ' ', desc).strip()

    # Use report_date as disambiguator if provided (YYYY-MM-DD → YYYYMMDD)
    date_suffix = report_date.replace("-", "") if report_date else ""

    parts = [p for p in [desc, date_suffix] if p]
    folder = " ".join(parts)
    if year and year not in folder:
        folder = f"{folder} {year}".strip()

    return folder[:80].strip()

# Each rule: (required, excluded, name)
# All keywords in `required` must be present, none in `excluded`
TABLE_NAME_RULES: list[tuple[set[str], set[str], str]] = [
    # Damaged houses
    ({"damaged"},                       set(),                          "number_of_damaged_houses"),
    # Cost of assistance
    ({"assistance"},                    set(),                          "cost_of_assistance"),
    # Relief / FFPs
    ({"ffps"},                          set(),                          "relief_provided"),
    ({"packs"},                          set(),                          "relief_provided"),
    ({"standby"},                          set(),                          "standby_funds"),
    # Affected (no displacement context)
    ({"affected"},                      {"evacuation", "displaced"},    "number_of_affected_population"),
    # Inside EC — any combination that implies inside
    ({"inside"},                        set(),                          "inside_ec_displaced_families_persons"),
    ({"affected", "evacuation"},        set(),                          "inside_ec_displaced_families_persons"),
    ({"served", "inside"},              set(),                          "inside_ec_displaced_families_persons"),
    ({"displaced", "inside"},           set(),                          "inside_ec_displaced_families_persons"),
    ({"evacuation", "inside"},          set(),                          "inside_ec_displaced_families_persons"),
    # Outside EC
    ({"outside"},                       set(),                          "outside_ec_displaced_families_persons"),
    ({"served", "outside"},             set(),                          "outside_ec_displaced_families_persons"),
    ({"displaced", "outside"},          set(),                          "outside_ec_displaced_families_persons"),
    ({"evacuation", "outside"},         set(),                          "outside_ec_displaced_families_persons"),
    # Total displaced
    ({"displaced", "total"},            set(),                          "total_displaced_families_persons"),
    ({"served", "total"},               set(),                          "total_displaced_families_persons"),
    # General displaced / served / evacuee fallback
    ({"served"},                        set(),                          "total_ec_displaced_families_persons"),
    ({"displaced"},                     set(),                          "total_ec_displaced_families_persons"),
    ({"evacuee"},                       set(),                          "total_ec_displaced_families_persons"),
    ({"evacuation", "served"},          set(),                          "total_ec_displaced_families_persons"),
]

def infer_table_name(sample_cols: list[str], id: int) -> str:
    cols = " ".join(sample_cols).lower()

    for required, excluded, name in TABLE_NAME_RULES:
        if (all(kw in cols for kw in required) and
                not any(kw in cols for kw in excluded)):
            return name

    return cols[:60] if cols else f"table_{id}"


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse newlines and extra whitespace within column names.

    Docling joins multi-line header cell text with '\\n', so the same column
    can appear as 'DISPLACED\\nFAMILIES' on one page and 'DISPLACED FAMILIES'
    on the next.  Normalising here keeps col_similarity stable.
    """
    df.columns = [re.sub(r'\s+', ' ', str(c)).strip() for c in df.columns]
    return df


def drop_repeated_header_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove leading rows that echo the column headers.

    Pass 1 — column-name match: drops rows whose values fuzzy-match the current
    column labels (handles repeated headers on continuation pages).

    Pass 2 — orphan sub-header detection: after pass 1, drops any remaining
    leading rows where *every* non-location, non-empty data cell contains only
    alphabetic text (no digits).  In DROMIC tables all real data cells are
    numbers or dashes, so an all-alpha data row must be a sub-header label row
    (e.g. 'Partially | Totally') that Docling didn't flag as column_header=True.
    """
    if df.empty:
        return df

    # ── Pass 1: fuzzy match against column names ──────────────────────────────
    cols_norm = [normalize_col(str(c)) for c in df.columns]
    drop_up_to = 0

    for idx in range(min(3, len(df))):
        row_vals = [normalize_col(str(v)) for v in df.iloc[idx].values]
        pairs = [
            (c, r) for c, r in zip(cols_norm, row_vals)
            if c and r not in ('', 'NAN', 'NONE')
        ]
        if not pairs:
            continue
        match_count = sum(1 for c, r in pairs if fuzz.token_set_ratio(c, r) >= 75)
        if match_count / len(pairs) >= 0.5:
            drop_up_to = idx + 1
        else:
            break

    if drop_up_to:
        print(f"  [continuation] Dropped {drop_up_to} repeated header row(s)")
        df = df.iloc[drop_up_to:].reset_index(drop=True)

    # ── Pass 2: orphan sub-header rows (all-alpha data cells) ─────────────────
    drop_up_to2 = 0
    for idx in range(min(3, len(df))):
        row = df.iloc[idx]
        data_vals = [
            str(v).strip()
            for col, v in row.items()
            if not is_location_col(str(col))
            and str(v).strip().upper() not in ('', 'NAN', 'NONE')
        ]
        if len(data_vals) < 2:
            continue  # too few non-location values to judge reliably
        if all(re.match(r'^[A-Za-z][A-Za-z0-9 /\(\)\.]*$', v) and not re.search(r'\d', v)
               for v in data_vals):
            drop_up_to2 = idx + 1
        else:
            break  # first row with numeric data — stop

    if drop_up_to2:
        print(f"  [sub-header]  Dropped {drop_up_to2} orphan sub-header row(s)")
        df = df.iloc[drop_up_to2:].reset_index(drop=True)

    return df


def strip_absorbed_data_from_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip numeric data values that Docling absorbed into column names.

    When a table has 3 header rows, Docling flattens them with '.' as separator.
    If it misidentifies the GRAND TOTAL row as a third header row the result is:
      'NO. OF DAMAGED HOUSES.Total.115,840'  →  'NO. OF DAMAGED HOUSES.Total'
      'REGION / PROVINCE / MUNICIPALITY..GRAND TOTAL'  →  'REGION / PROVINCE / MUNICIPALITY'
    The fix strips any trailing segment that is a bare number (with optional commas)
    or the literal text 'GRAND TOTAL'.
    """
    new_cols = []
    for col in df.columns:
        cleaned = re.sub(
            r'[\.\s]+(GRAND\s+TOTAL|\d[\d,]*)$',
            '', str(col), flags=re.IGNORECASE
        ).strip('.')
        new_cols.append(cleaned.strip() if cleaned.strip() else str(col))
    if new_cols != list(df.columns):
        print(f"  [col-clean] Stripped absorbed data from column names")
    df.columns = new_cols
    return df


def split_merged_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Split rows where Docling fused adjacent PDF rows due to small row height.

    Fused rows are detected by cells containing two or more values separated
    by 2+ consecutive spaces (e.g. '9  1', 'City of Balanga  Dinalupihan').
    'region' and 'province' are always propagated to every split row since
    they come from forward-filled higher-level rows and are never merged.
    All other columns — including 'municipality' — are split normally, so
    'City of Balanga (capital)  Dinalupihan' becomes two correct rows.
    For columns with only a single value in a merged row, the value is kept
    on the first split row and left empty on the rest (information was lost
    at the PDF-parsing stage and cannot be recovered).
    """
    PROPAGATE = {"region", "province"}

    new_rows: list[dict] = []
    split_count = 0

    for _, row in df.iterrows():
        n_splits = 1
        for col, val in row.items():
            if col in PROPAGATE:
                continue
            s = str(val).strip()
            if re.search(r'  +', s):
                parts = [p for p in re.split(r'  +', s) if p.strip() or p == '']
                n_splits = max(n_splits, len(parts))

        if n_splits <= 1:
            new_rows.append(row.to_dict())
            continue

        split_count += 1
        for k in range(n_splits):
            new_row: dict = {}
            for col, val in row.items():
                if col in PROPAGATE:
                    new_row[col] = val
                    continue
                s = str(val).strip()
                if re.search(r'  +', s):
                    parts = [p.strip() for p in re.split(r'  +', s)]
                    new_row[col] = parts[k] if k < len(parts) else ''
                else:
                    new_row[col] = val if k == 0 else ''
            new_rows.append(new_row)

    if split_count:
        print(f"  [row-split] Split {split_count} merged row(s)")

    return pd.DataFrame(new_rows, columns=df.columns)


# Main loop ──────────────────────────────────────────────────────────────

device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using device: {device}")  # should print 'cuda'

pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = False
pipeline_options.do_table_structure = True

pipeline_options.table_structure_options = TableStructureOptions(
    do_cell_matching=True,
    mode=TableFormerMode.ACCURATE  # can afford ACCURATE on GPU
)

pipeline_options.generate_page_images = False
pipeline_options.generate_picture_images = False
pipeline_options.images_scale = 1.0

converter = DocumentConverter(
    format_options={
        InputFormat.PDF: PdfFormatOption(
            pipeline_options=pipeline_options,
            backend=PyPdfiumDocumentBackend)
    }
)

def process_file(pdf_path: Path, output_dir: Path):

    result = converter.convert(pdf_path)
    doc = result.document

    manifest_path = pdf_path.parent / "manifest.json"  # or per-file json
    event = extract_report_metadata(doc, pdf_path, manifest_path)

    folder_name = make_folder_name(os.path.basename(pdf_path), event.lastUpdateDate) or event.eventName
    output_dir  = Path(os.path.join(output_dir, folder_name))
    os.makedirs(output_dir, exist_ok=True)

    generate_json(event, output_dir)

    print(f"Tables found: {len(doc.tables)}\n")

    processed_tables: list[tuple[str, pd.DataFrame]] = []

    with pdfplumber.open(pdf_path) as pdf:

        for i, table in enumerate(doc.tables):
            caption    = table.caption_text(doc)
            prov_pages = [p.page_no for p in table.prov]
            # print(f"=== Table {i+1} | pages={prov_pages} | caption={caption!r} ===")

            df = table.export_to_dataframe(doc)
            df = clean_column_names(df)
            df = strip_absorbed_data_from_columns(df)
            df = drop_repeated_header_rows(df)
            print(df.columns.tolist())
            caption = validate_caption(caption, df)  # validate before processing

            # ── Find the location column ──────────────────────────────────────────
            loc_col_idx = find_location_column(df)
            used_fallback = False

            if loc_col_idx is None:
                # Headerless continuation pages have a data value (e.g. "Surigao del Norte")
                # as the column name, so no keyword match is possible.  Fall back to col 0 —
                # in DROMIC tables the location column is always first.  If the assumption is
                # wrong the level-classification gate further below will still skip the table.
                if df.shape[1] > 1:
                    loc_col_idx = 0
                    used_fallback = True
                    print(f"  [fallback] No named location column; trying col 0 ({df.columns[0]!r})")
                else:
                    print("  [skip] No location column detected.\n")
                    df.to_csv(f"./dump/{i}.csv", index=False)
                    continue

            loc_col_name = df.columns[loc_col_idx]
            # print(f"  Location column: index={loc_col_idx}, name={loc_col_name!r}")

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
            row_bbox_map: dict[int, tuple[int, tuple[float, float, float, float]]] = {}

            SKIP_TEXTS = {"GRAND TOTAL", "GRAND  TOTAL"}  # handle double-space variants

            for cell in table.data.table_cells:
                if cell.column_header:
                    continue
                if cell.start_col_offset_idx != loc_col_idx:
                    continue
                if cell.text.strip() == str(loc_col_name).strip():
                    continue
                if cell.text.strip().upper() in SKIP_TEXTS:          
                    continue

                row_idx = cell.start_row_offset_idx
                bbox    = cell.bbox

                if bbox is None:
                    continue 

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
                if plumber_page is None:
                    continue
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

            # When using the col-0 fallback, guard against narrative tables:
            # if no rows were classified as any location level the first column
            # is almost certainly not a location column (e.g. a DATE column).
            if used_fallback and not any(v is not None for v in level_per_df_row.values()):
                print("  [skip] No location hierarchy detected after fallback — likely narrative table.\n")
                df.to_csv(f"./dump/{i}.csv", index=False)
                continue

            # ── Forward-fill region / province / municipality ─────────────────────
            region_col: list[str | None]      = []
            province_col: list[str | None]      = []
            municipality_col: list[str | None]  = []

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

            df = split_merged_rows(df)

            processed_tables.append((caption or "", df))
            # print(df.to_string())

    processed_tables = fix_caption_sequence(processed_tables)

    # ── grouping logic: merge by signature OR by high similarity to adjacent table ─

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
            # Continuation pages are always immediately adjacent; allow a
            # slightly lower threshold to absorb minor column-name variations
            # (e.g. abbreviation differences) that survive normalisation.
            is_adjacent = pos - group_last_idx[gid] == 1
            threshold = 0.85 if is_adjacent else SIMILARITY_THRESHOLD
            if sim >= threshold:
                groups[gid].append((caption, df))
                group_last_idx[gid] = pos
                merged = True
                break

        if not merged:
            groups[group_id] = [(caption, df)]
            group_representatives[group_id] = df
            group_last_idx[group_id] = pos
            group_id += 1

    merged_results: list[dict[str, Any]] = []

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
            # caption = "_".join(sample_cols) if sample_cols else f"table_{gid}"
            caption = infer_table_name(sample_cols, gid)
            # print(caption)

        filename = slugify(caption) + ".csv"
        

        # Dedup base_df too before using as reference
        base_df = items[0][1].loc[:, ~items[0][1].columns.duplicated()]
        aligned = [base_df] + [align_columns(base_df, df) for _, df in items[1:]]
        base_cols = list(base_df.columns)
        aligned = [df.reindex(columns=base_cols) for df in aligned]
        merged_df = pd.concat(aligned, ignore_index=True)
        
        merged_results.append({
            "caption": caption,
            "df": merged_df,
            "filename": filename
        })

    final_to_save: list[dict[str, Any]] = []
    for i, table_i in enumerate(merged_results):
        is_summary = False
        df_i = table_i["df"]
        
        for j, table_j in enumerate(merged_results):
            if i == j: continue
            df_j = table_j["df"]
            
            # Only deduplicate when table_j is substantially larger — requiring
            # at least 2× the rows prevents page-continuation segments (30 rows
            # vs 32 rows) from being wrongly classified as summary vs detail.
            if col_similarity(df_i, df_j) > 0.95:
                if len(df_j) >= max(len(df_i) * 2, len(df_i) + 10):
                    print(f"  [dedupe] Skipping summary '{table_i['caption']}' ({len(df_i)} rows) "
                        f"in favor of detailed '{table_j['caption']}' ({len(df_j)} rows)")
                    is_summary = True
                    break
        
        if not is_summary:
            final_to_save.append(table_i)

    # Final Save
    for item in final_to_save:
        df = item["df"]
        filename = item["filename"]
        
        # Final safety check for duplicate filenames in the same folder
        counter = 1
        original_stem = Path(filename).stem
        while (output_dir / filename).exists():
            filename = f"{original_stem}_{counter}.csv"
            counter += 1
            
        df.to_csv(output_dir / filename, index=False)
        print(f"Saved: {filename} ({len(df)} rows)")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_dir = Path(f"../data/raw/dromic-new/{args.year}-pdf-mini")
    output_dir =  Path(f"../data/parsed/dromic-new/{args.year}")

    files = list(input_dir.glob("*"))


    for file in files:
        if file.suffix != ".pdf":
            continue

        process_file(file, output_dir)
        # try:
        #     process_file(file, output_dir)
        # except Exception as e:
        #     print(f"[ERROR] {file.name}: {e}")


if __name__ == "__main__":
    main()
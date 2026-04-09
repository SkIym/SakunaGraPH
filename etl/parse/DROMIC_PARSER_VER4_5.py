import os
import tempfile
import pdfplumber
import pandas as pd
import re
import json
from datetime import datetime
from dataclasses import dataclass, asdict
from concurrent.futures import ProcessPoolExecutor, as_completed

# --------------------------
# CONFIGURATION
# --------------------------

INPUT_FOLDER  = "DROMIC/DROMIC/2022"
OUTPUT_FOLDER = "DROMIC_PARSED_V4.5_2022"

HEADER_SEARCH_DISTANCE = 100   # px above a table to look for its label


# --------------------------
# DATA MODEL
# --------------------------

@dataclass
class DromicEvent:
    eventName      : str = ""      # e.g. "Fire Incident – Brgy. 45-Sta.Cruz, Tacloban City"
    eventType      : str = ""      # e.g. "Fire Incident", "Typhoon", "Volcanic Activity", …
    location       : str = ""      # raw location string from filename / title
    reportNumber   : int = 0
    reportDate     : str = ""      # "as of" date from the title
    startDate      : str = ""
    endDate        : str = ""
    lastUpdateDate : str = ""
    reportName     : str = ""      # original filename
    recordedBy     : str = "DROMIC"
    obtainedDate   : str = ""
    reportLink     : str = ""
    remarks        : str = ""      # Section I narrative


# --------------------------
# EVENT-TYPE KEYWORDS
# --------------------------

EVENT_TYPE_KEYWORDS = [
    "Super Typhoon", "Typhoon", "Tropical Storm", "Tropical Depression",
    "Low Pressure Area", "Southwest Monsoon", "Northeast Monsoon", "Monsoon",
    "Flood", "Flooding", "Flash Flood", "Storm Surge", "Tsunami",
    "Landslide", "Mudslide", "Earthquake", "Liquefaction",
    "Volcanic", "Eruption",
    "Fire",
    "Armed Conflict", "Demolition", "Explosion", "Oil Spill", "Collapsed",
    "Thunderstorm", "Whirlwind", "Tornado",
    "Drought", "El Niño", "La Niña",
    "Preparedness",
]

_EVENT_TYPE_CANONICAL = {
    "Super Typhoon"      : "Typhoon",
    "Tropical Depression": "Tropical Storm",
    "Flooding"           : "Flood",
    "Flash Flood"        : "Flood",
    "Mudslide"           : "Landslide",
    "Eruption"           : "Volcanic",
    "Collapsed"          : "Collapsed Structure",
    "Monsoon"            : "Southwest Monsoon",
}

_TERMINAL_DATE_RE = re.compile(
    r'(\d{1,2}[- ]\w+[- ]\d{4})(?:[- ]\d{1,2}(?:AM|PM))?$',
    re.IGNORECASE,
)


# -----------------------------------------------------------------------
# DOCX → PDF CONVERSION  (uses Microsoft Word via docx2pdf)
# -----------------------------------------------------------------------
# Requires:  pip install docx2pdf
#
# On Windows, docx2pdf drives Microsoft Word via COM automation.
# Word must be installed. The conversion is high-fidelity because it uses
# the exact same engine as Word's own File → Export → PDF feature.

try:
    from docx2pdf import convert as _docx2pdf_convert
    _DOCX2PDF_AVAILABLE = True
except ImportError:
    _DOCX2PDF_AVAILABLE = False


def docx_to_pdf(docx_path: str) -> str | None:
    """
    Convert a .docx file to a temporary PDF using Microsoft Word (via docx2pdf).
    Returns the path to the created PDF in a temp directory.
    The caller must clean up the file using _cleanup_tmp().

    Raises RuntimeError with a clear message on failure.
    """
    if not _DOCX2PDF_AVAILABLE:
        raise RuntimeError(
            "docx2pdf is not installed. Run:  pip install docx2pdf\n"
            "Microsoft Word must also be installed (it drives the conversion on Windows)."
        )

    tmp_dir  = tempfile.mkdtemp(prefix="dromic_docx_")
    stem     = os.path.splitext(os.path.basename(docx_path))[0]
    pdf_path = os.path.join(tmp_dir, f"{stem}.pdf")

    try:
        _docx2pdf_convert(docx_path, pdf_path)
    except Exception as e:
        raise RuntimeError(f"docx2pdf conversion failed for '{docx_path}': {e}") from e

    if not os.path.exists(pdf_path):
        raise RuntimeError(
            f"docx2pdf ran without error but no PDF was created at {pdf_path}. "
            "Make sure Microsoft Word is installed and not open in a broken state."
        )

    return pdf_path


def _cleanup_tmp(tmp_pdf: str | None):
    """Remove the temp PDF and its parent directory created by docx_to_pdf."""
    if tmp_pdf and os.path.exists(tmp_pdf):
        try:
            os.remove(tmp_pdf)
            parent = os.path.dirname(tmp_pdf)
            if parent and os.path.isdir(parent):
                os.rmdir(parent)
        except OSError:
            pass


# -----------------------------------------------------------------------
# FOLDER NAME GENERATOR
# -----------------------------------------------------------------------

def make_folder_name(filename: str) -> str:
    name = os.path.splitext(filename)[0]
    name = re.sub(r'\.docx$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'-[1-9A-Z]$', '', name)

    year_m = re.search(r'\b(20\d{2})\b', name)
    year   = year_m.group(1) if year_m else ""

    desc = re.sub(r'^DSWD-DROMIC-.*?-on-(?:the-)?', '', name, flags=re.IGNORECASE)
    desc = re.sub(r'(?:-as-of)?-?\d{1,2}-[A-Za-z]+-20\d{2}.*$', '', desc, flags=re.IGNORECASE)
    desc = desc.replace('-', ' ').strip()
    desc = re.sub(r'\s+', ' ', desc)
    desc = re.sub(r'\b([A-Z][a-z]{0,4})_\b', r'\1. ', desc)
    desc = desc.replace('_', ' ')
    desc = re.sub(r'\s+', ' ', desc).strip()

    return f"{desc} {year}".strip() if year else desc


# -----------------------------------------------------------------------
# FILENAME PARSER
# -----------------------------------------------------------------------

def parse_dromic_filename(filename: str) -> dict:
    result = {
        "report_number" : 0,
        "event_type"    : "",
        "storm_name"    : "",
        "location_raw"  : "",
        "as_of_date_str": "",
        "is_terminal"   : False,
        "desc_text"     : "",
    }

    name    = os.path.splitext(filename)[0]
    name_sp = name.replace("_", " ").replace("-", " ")
    core = re.sub(r'^DSWD\s+DROMIC\s+', '', name_sp, flags=re.IGNORECASE).strip()

    is_terminal = bool(re.match(r'Terminal\s+Report', core, re.IGNORECASE))
    result["is_terminal"] = is_terminal

    m = re.search(r'\bReport\s+#?(\d+)\b', name_sp, re.IGNORECASE)
    if m:
        result["report_number"] = int(m.group(1))

    if is_terminal:
        m_date = _TERMINAL_DATE_RE.search(name_sp.rstrip())
        if m_date:
            result["as_of_date_str"] = m_date.group(1).replace("-", " ").strip()
        desc_text = re.sub(r'^Terminal\s+Report\s+on\s+(?:the\s+)?', '', core, flags=re.IGNORECASE).strip()
        desc_text = re.sub(r'\s+\d{1,2}\s+\w+\s+\d{4}(?:\s+\d{1,2}(?:AM|PM))?\s*$', '', desc_text, flags=re.IGNORECASE).strip()
    else:
        m_date = re.search(r'as\s+of\s+(\d{1,2}\s+\w+\s+\d{4})', name_sp, re.IGNORECASE)
        if m_date:
            result["as_of_date_str"] = m_date.group(1).strip()
        desc_text = re.sub(
            r'^(?:Preparedness\s+for\s+Response\s+)?Report\s+\d+\s+on\s+'
            r'(?:the\s+)?(?:Effects\s+of\s+)?(?:Localized\s+)?',
            '', core, flags=re.IGNORECASE
        ).strip()
        desc_text = re.sub(
            r'^Preparedness\s+for\s+Response\s+Report\s+\d+\s+on\s+(?:the\s+)?',
            '', desc_text, flags=re.IGNORECASE
        ).strip()
        desc_text = re.sub(r'\s+as\s+of\s+.*$', '', desc_text, flags=re.IGNORECASE).strip()

    matched_keyword = ""
    search_text = name_sp + " " + desc_text
    for keyword in sorted(EVENT_TYPE_KEYWORDS, key=len, reverse=True):
        if re.search(re.escape(keyword), search_text, re.IGNORECASE):
            matched_keyword = keyword
            break

    canonical = _EVENT_TYPE_CANONICAL.get(matched_keyword, matched_keyword)
    result["event_type"] = canonical

    if matched_keyword in ("Super Typhoon", "Typhoon", "Tropical Storm", "Tropical Depression"):
        m_name = re.search(
            re.escape(matched_keyword) + r'\s+["\u201c]?([A-Z][A-Za-z]+)["\u201d]?',
            desc_text, re.IGNORECASE
        )
        if m_name:
            result["storm_name"] = m_name.group(1).strip()

    loc_m = re.search(r'\bin\b\s+(.*)', desc_text, re.IGNORECASE)
    if loc_m:
        raw_loc = loc_m.group(1).strip()
        if matched_keyword:
            raw_loc = re.sub(re.escape(matched_keyword), '', raw_loc, flags=re.IGNORECASE)
        raw_loc = re.sub(r'\s+', ' ', raw_loc).strip(" -")
        result["location_raw"] = raw_loc

    result["desc_text"] = desc_text
    return result


# -----------------------------------------------------------------------
# DATE HELPERS
# -----------------------------------------------------------------------

def parse_flexible_date(date_str: str):
    date_str = date_str.strip().replace(',', '')
    formats = [
        "%d %B %Y %I%p", "%d %B %Y %I:%M%p",
        "%d %B %Y %H:%M", "%d %B %Y",
        "%B %d %Y %H:%M", "%B %d %Y",
        "%d %b %Y %H:%M", "%d %b %Y",
        "%b %d %Y %H:%M", "%b %d %Y",
        "%d-%m-%Y", "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def extract_report_date_from_title(pdf_path: str):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = pdf.pages[0].extract_text() or ""
        m = re.search(r'as\s+of\s+(\d{1,2}\s+\w+\s+\d{4})', text, re.IGNORECASE)
        if m:
            return parse_flexible_date(m.group(1).strip())
    except Exception:
        pass
    return None


# -----------------------------------------------------------------------
# LOCATION EXTRACTION FROM PDF TITLE
# -----------------------------------------------------------------------

# Generic/national event types that don't have a specific location in
# their title (e.g. "Super Typhoon 'Karding'" covers multiple regions).
_GENERIC_EVENT_TITLE_RE = re.compile(
    r'Super\s+Typhoon|Typhoon|Tropical\s+Storm|Tropical\s+Depression|' 
    r'Low\s+Pressure\s+Area|Southwest\s+Monsoon|Northeast\s+Monsoon|' 
    r'Preparedness|Volcanic\s+Eruption|Eruption\s+of',
    re.IGNORECASE,
)

def extract_location_from_title(pdf_path: str) -> str | None:
    """
    Extract a clean, comma-separated location string from the PDF title block.

    Returns a string like "Brgy. Tapodoc, Aleosan, Cotabato" for localised
    events, or None for generic/national events (typhoons, monsoons, etc.)
    where the title contains no specific place.

    Falls back to None on any error so the caller can use the filename-
    derived location instead.
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = pdf.pages[0].extract_text() or ""
    except Exception:
        return None

    lines = [l.strip() for l in text.split('\n') if l.strip()]

    # Collect title lines — stop at the date line or first section heading
    title_lines = []
    for line in lines:
        if re.search(r'Page\s+\d+\s+of\s+\d+', line): continue   # page header
        if re.match(r'^[_\-=]{5,}$', line): continue               # decorators
        if re.match(r'\d{1,2}\s+\w+\s+\d{4}', line): break        # date line
        if re.match(r'as\s+of\s+\d', line, re.IGNORECASE): break   # 'as of DD Month...'
        if re.match(r'^I{1,3}V?\.\s', line): break                  # section I
        title_lines.append(line)

    full_title = re.sub(r'\s+', ' ', ' '.join(title_lines)).strip()

    # Strip the DSWD DROMIC boilerplate prefix
    core = re.sub(
        r'^DSWD\s+DROMIC\s+(?:Terminal\s+)?Report\s+on\s+(?:the\s+)?',
        '', full_title, flags=re.IGNORECASE,
    ).strip()

    # Generic/national events → no specific location in title
    if _GENERIC_EVENT_TITLE_RE.match(core):
        return None

    # Extract location after "in" or "at"
    # e.g. "Flooding Incident in Midsayap, North Cotabato"
    #      "Armed Conflict in Brgy. Tapodoc, Aleosan, Cotabato"
    #      "Capsized Motorized Banca at Pioduran Port, Albay"
    m = re.search(r'\b(?:in|at)\s+(.+)$', core, re.IGNORECASE)
    if m:
        loc = m.group(1).strip()
        # Strip any date that bled into the location string
        loc = re.sub(r',?\s*\d{1,2}\s+\w+\s+\d{4}.*$', '', loc).strip()
        loc = re.sub(r',?\s*\d{1,2}(?:AM|PM).*$', '', loc, flags=re.IGNORECASE).strip()
        if loc:
            return loc

    return None


# -----------------------------------------------------------------------
# INCIDENT DATE EXTRACTION
# -----------------------------------------------------------------------

_END_SIGNAL_RE = re.compile(
    r'exit(?:ed)?\s+(?:the\s+)?PAR|'
    r'left\s+(?:the\s+)?PAR|'
    r'declar(?:ed)?\s+fire\s+out|'
    r'fully\s+dissipat|'
    r'no\s+longer\s+a\s+threat|'
    r'returned\s+to\s+normal|'
    r'operat(?:ions?|ional)\s+(?:have\s+)?resume',
    re.IGNORECASE,
)

_MULTI_DAY_TYPES = {
    "Typhoon", "Tropical Storm", "Low Pressure Area",
    "Southwest Monsoon", "Flood",
}

def extract_incident_dates(narrative: str, event_type: str) -> tuple:
    sentence_re = re.compile(r'On\s+(\d{1,2}[\s,]+\w+[\s,]+\d{4})', re.IGNORECASE)
    dated_sentences = []
    for m in sentence_re.finditer(narrative):
        raw = m.group(1).replace(',', '').strip()
        dt = parse_flexible_date(raw)
        if dt:
            snippet = narrative[m.start(): m.start() + 200]
            dated_sentences.append((dt, snippet))

    if not dated_sentences:
        return "", ""

    start_dt = dated_sentences[0][0]
    is_multi_day = event_type in _MULTI_DAY_TYPES and len(dated_sentences) > 1

    if is_multi_day:
        end_dt = None
        for dt, snippet in dated_sentences:
            if _END_SIGNAL_RE.search(snippet):
                end_dt = dt
                break
        if end_dt is None:
            end_dt = max(dt for dt, _ in dated_sentences)
    else:
        end_dt = start_dt

    return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")


# -----------------------------------------------------------------------
# SECTION I EXTRACTION
# -----------------------------------------------------------------------

SECTION_STOP_PATTERNS = [
    r'^I{1,3}V?\.\s',
    r'^Annex\s+[A-Z]',
    r'REGION\s*/\s*PROVINCE',
    r'NUMBER\s+OF\s+AFFECTED',
    r'NUMBER\s+OF\s+DISPLACED',
    r'NO\.\s+OF\s+DAMAGED',
    r'COST\s+OF\s+ASSISTANCE',
    r'STANDBY\s+FUNDS',
    r'OFFICE\s+STANDBY',
]

def extract_situation_overview(pdf_path: str) -> str:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            parts = []
            for page in pdf.pages[:3]:
                text = page.extract_text() or ""
                lines = text.split('\n')
                in_section = False
                for line in lines:
                    stripped = line.strip()
                    if re.search(r'Situation\s+Overview', stripped, re.IGNORECASE):
                        in_section = True
                        continue
                    if in_section:
                        if any(re.search(p, stripped, re.IGNORECASE) for p in SECTION_STOP_PATTERNS):
                            return '\n'.join(parts).strip()
                        if stripped:
                            parts.append(stripped)
            return '\n'.join(parts).strip()
    except Exception as e:
        print(f"   ⚠ Error extracting narrative: {e}")
        return ""


# -----------------------------------------------------------------------
# TABLE LABELLING
# -----------------------------------------------------------------------

ANNEX_LABEL_RE   = re.compile(r'Annex\s+([A-Z])\.\s*(.+)', re.IGNORECASE)
SECTION_LABEL_RE = re.compile(r'^(?:[IVX]+\.|[a-z]\\.|\d+\.)\s+(.+)')
STANDBY_TABLE_RE = re.compile(r'standby\s+funds?\s+and\s+prepositioned', re.IGNORECASE)

_SKIP_LINE_RE = re.compile(
    r'DSWD\s+DROMIC\s+Terminal\s+Report|'
    r'Page\s+\d+\s+of\s+\d+|'
    r'^Source:|^Note:',
    re.IGNORECASE,
)

def _extract_label_from_lines(lines: list) -> str:
    """Shared label extraction logic — scans lines bottom-up for a table label."""
    for line in lines:
        # Skip page headers/footers and source/note lines
        if _SKIP_LINE_RE.search(line):
            continue
        m = ANNEX_LABEL_RE.search(line)
        if m:
            return f"annex_{m.group(1).upper()}_{_slugify(m.group(2).strip())}"
        if STANDBY_TABLE_RE.search(line):
            return "standby_funds_stockpile"
        m2 = SECTION_LABEL_RE.match(line)
        if m2:
            return _slugify(m2.group(1))
        # "Table N." pattern: Table 3. Cost of Assistance ...
        m3 = re.match(r'Table\s+\d+\.?\s+(.+)', line, re.IGNORECASE)
        if m3:
            return _slugify(m3.group(1))
        # Last resort: only accept lines that look like actual labels.
        # A label starts with a letter and contains NO digits — this
        # rejects data rows like "Pilar 6 78 267" or "Sual 4 - 157 - 587"
        # that bleed in from the bottom of the previous page.
        if (len(line) > 5
                and not re.match(r'^Page \d', line)
                and re.match(r'[A-Za-z]', line)
                and not re.search(r'\d', line)):
            return _slugify(line)
    return "unknown_table" 


def get_table_label(page, table_bbox, prev_page=None) -> str:
    """
    Look for a label above the table on the current page.
    If nothing found AND the table starts near the top of the page (top < 200px),
    also scan the bottom of the previous page — handles cross-page labels.
    """
    x0, top, x1, _ = table_bbox
    search_top = max(0, top - HEADER_SEARCH_DISTANCE)
    try:
        crop = page.crop((0, search_top, page.width, top))
        header_text = crop.extract_text() or ""
    except Exception:
        header_text = ""

    lines = [l.strip() for l in reversed(header_text.split('\n')) if l.strip()]
    label = _extract_label_from_lines(lines)

    # If no label found and the table is near the top, check the previous page bottom
    if label == "unknown_table" and prev_page is not None and top < 200:
        try:
            scan_height = 150   # px from bottom of previous page
            prev_crop = prev_page.crop((0, prev_page.height - scan_height,
                                        prev_page.width, prev_page.height))
            prev_text = prev_crop.extract_text() or ""
        except Exception:
            prev_text = ""
        prev_lines = [l.strip() for l in reversed(prev_text.split('\n')) if l.strip()]
        label = _extract_label_from_lines(prev_lines)

    return label


def _slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', '_', text.strip())
    return text[:60]


# -----------------------------------------------------------------------
# TABLE HELPERS
# -----------------------------------------------------------------------

# -----------------------------------------------------------------------
# PHILIPPINE GEOGRAPHIC LOOKUP TABLES
# -----------------------------------------------------------------------
# All 17 regions (by name variants used in DROMIC reports).
# Lookup is done via normalized uppercase matching.

_PH_REGIONS = {
    # Standard numbered regions
    "REGION I", "ILOCOS REGION",
    "REGION II", "CAGAYAN VALLEY",
    "REGION III", "CENTRAL LUZON",
    "REGION IV-A", "REGION IVA", "CALABARZON",
    "REGION IV-B", "REGION IVB", "MIMAROPA",
    "REGION V", "BICOL REGION",
    "REGION VI", "WESTERN VISAYAS",
    "REGION VII", "CENTRAL VISAYAS",
    "REGION VIII", "EASTERN VISAYAS",
    "REGION IX", "ZAMBOANGA PENINSULA",
    "REGION X", "NORTHERN MINDANAO",
    "REGION XI", "DAVAO REGION",
    "REGION XII", "SOCCSKSARGEN",
    "REGION XIII", "CARAGA",
    # Special regions
    "NCR", "NATIONAL CAPITAL REGION",
    "CAR", "CORDILLERA ADMINISTRATIVE REGION",
    "BARMM", "BANGSAMORO",
    "ARMM",
}

# All 82 Philippine provinces (uppercase).
_PH_PROVINCES = {
    # Region I
    "ILOCOS NORTE", "ILOCOS SUR", "LA UNION", "PANGASINAN",
    # Region II
    "BATANES", "CAGAYAN", "ISABELA", "NUEVA VIZCAYA", "QUIRINO",
    # Region III
    "AURORA", "BATAAN", "BULACAN", "NUEVA ECIJA", "PAMPANGA",
    "TARLAC", "ZAMBALES",
    # Region IV-A
    "BATANGAS", "CAVITE", "LAGUNA", "QUEZON", "RIZAL",
    # Region IV-B
    "MARINDUQUE", "OCCIDENTAL MINDORO", "ORIENTAL MINDORO",
    "PALAWAN", "ROMBLON",
    # Region V
    "ALBAY", "CAMARINES NORTE", "CAMARINES SUR", "CATANDUANES",
    "MASBATE", "SORSOGON",
    # Region VI
    "AKLAN", "ANTIQUE", "CAPIZ", "GUIMARAS", "ILOILO", "NEGROS OCCIDENTAL",
    # Region VII
    "BOHOL", "CEBU", "NEGROS ORIENTAL", "SIQUIJOR",
    # Region VIII
    "BILIRAN", "EASTERN SAMAR", "LEYTE", "NORTHERN SAMAR",
    "SAMAR", "WESTERN SAMAR", "SOUTHERN LEYTE",
    # Region IX
    "ZAMBOANGA DEL NORTE", "ZAMBOANGA DEL SUR", "ZAMBOANGA SIBUGAY",
    # Region X
    "BUKIDNON", "CAMIGUIN", "LANAO DEL NORTE", "MISAMIS OCCIDENTAL",
    "MISAMIS ORIENTAL",
    # Region XI
    "COMPOSTELA VALLEY", "DAVAO DE ORO", "DAVAO DEL NORTE", "DAVAO DEL SUR",
    "DAVAO OCCIDENTAL", "DAVAO ORIENTAL",
    # Region XII
    "COTABATO", "NORTH COTABATO", "SARANGANI", "SOUTH COTABATO", "SULTAN KUDARAT",
    # Region XIII / CARAGA
    "AGUSAN DEL NORTE", "AGUSAN DEL SUR", "DINAGAT ISLANDS",
    "SURIGAO DEL NORTE", "SURIGAO DEL SUR",
    # CAR
    "ABRA", "APAYAO", "BENGUET", "IFUGAO", "KALINGA", "MOUNTAIN PROVINCE",
    # BARMM
    "BASILAN", "LANAO DEL SUR", "MAGUINDANAO", "MAGUINDANAO DEL NORTE",
    "MAGUINDANAO DEL SUR", "SULU", "TAWI-TAWI",
}


def _classify_location(text: str) -> str | None:
    """
    Return 'region', 'province', or None (= muni/city) based on lookup tables.
    Matching is done on normalized uppercase text, tolerating minor variations.
    """
    up = text.upper().strip()

    # Exact region match
    if up in _PH_REGIONS:
        return "region"

    # Starts with "REGION" followed by a number or Roman numeral
    if re.match(r'REGION\s+(\d+|[IVX]+)\b', up):
        return "region"

    # Exact province match
    if up in _PH_PROVINCES:
        return "province"

    # Partial province match (handles "Province of X" or truncated names)
    for prov in _PH_PROVINCES:
        if prov in up or up in prov:
            # Guard against short false positives (e.g. "SUR" matching "LANAO DEL SUR")
            if len(up) >= 5:
                return "province"

    return None   # unknown → caller decides (muni or font fallback)


def _read_cell_words(page, cell_bbox):
    """Extract (text, fonts) from a cell bbox. Returns ("", set()) on failure."""
    if not cell_bbox:
        return "", set()
    try:
        crop = page.crop(cell_bbox)
        words = crop.extract_words(extra_attrs=["fontname"])
        text  = " ".join(w["text"] for w in words).strip()
        fonts = {w["fontname"] for w in words}
        return text, fonts
    except (ValueError, TypeError):
        return "", set()


def get_location_level(page, cell_bbox, fallback_bbox=None):
    """
    Determine the geographic hierarchy level of a location cell.

    Classification priority:
      1. Summary keywords  → "summary"
      2. Header keywords   → "skip"
      3. Known region name → "region"   (lookup table, font-independent)
      4. Known province    → "province" (lookup table, font-independent)
      5. Italic font       → "muni"     (tiebreaker for unknown names)
      6. Default           → "muni"

    Also handles the two-column Annex layout where col 0 holds a single
    decorative letter and col 1 holds the actual location text.
    """
    text, fonts = _read_cell_words(page, cell_bbox)

    # Two-column layout: col 0 is blank or a single decorative letter
    if (not text or len(text) <= 2) and fallback_bbox:
        fb_text, fb_fonts = _read_cell_words(page, fallback_bbox)
        if fb_text:
            text, fonts = fb_text, fb_fonts

    if not text:
        return None, ""

    # Always-skip patterns
    if re.search(r'REGION.{0,5}PROVINCE|PROVINCE.{0,5}MUNICIPALITY', text, re.IGNORECASE):
        return "skip", text
    if re.match(r'^\s*(Note:|Source:)', text, re.IGNORECASE):
        return "skip", text
    if is_summary_row(text):
        return "summary", text

    # Lookup-based classification (font-independent)
    level = _classify_location(text)
    if level:
        return level, text

    # Font tiebreaker for unknown names (municipality / city / barangay)
    is_italic = any("Oblique" in f or "Italic" in f for f in fonts)
    return "muni", text


def is_summary_row(text: str) -> bool:
    if not text:
        return False
    up = text.upper().strip()
    return any(p in up for p in ['GRAND TOTAL', 'TOTAL', 'SUB-TOTAL', 'SUBTOTAL',
                                  'SUB TOTAL', 'OVERALL', 'SUMMARY'])


_NUMERIC_RE = re.compile(r'^[\d,.\-\s]+$')
_GEO_WORDS  = re.compile(
    r'REGION|PROVINCE|MUNICIPALITY|CITY|TOTAL|NATIONAL|CORDILLERA|NEGROS|CEBU|'
    r'MANILA|LUZON|VISAYAS|MINDANAO|OCCIDENTAL|ORIENTAL|NORTE|SUR|NCR|CAR|BARMM',
    re.IGNORECASE,
)

def _col_is_location(rows, col_idx) -> bool:
    """
    Return True if the given column in data rows contains geographic text
    (not purely numeric values). Requires at least one cell with alpha chars.
    """
    for r in rows:
        if col_idx >= len(r) or not r[col_idx]:
            continue
        cell = str(r[col_idx]).strip()
        if not cell or len(cell) <= 1:
            continue
        if _NUMERIC_RE.match(cell):
            return False   # purely numeric → not a location column
        if _GEO_WORDS.search(cell) or re.search(r'[A-Za-z]{3,}', cell):
            return True
    return False


def find_location_col_idx(extracted_rows):
    """
    Return the column index containing the location hierarchy.

    Two-column Annex layout: col 0 has a decorative single letter ("R"/"N")
    and col 1 has the real text (REGION VI, Negros Occidental, …).
    Single-column standard layout: col 0 has everything.

    We detect the two-column layout by checking:
      - col 0 data rows are all blank or single characters
      - col 1 data rows contain geographic text (not numbers)
    """
    header_col = 0
    for row in extracted_rows[:6]:
        for ci, cell in enumerate(row or []):
            if cell and re.search(r'REGION|PROVINCE|MUNICIPALITY', str(cell), re.IGNORECASE):
                header_col = ci
                break

    data_rows = [r for r in extracted_rows[2:8] if r]
    if not data_rows:
        return header_col

    # Check if col 0 (header_col) values are all blank/single-char in data rows
    col0_empty = all(
        header_col >= len(r) or not r[header_col] or len(str(r[header_col]).strip()) <= 1
        for r in data_rows
    )
    # Check if col 1 (header_col+1) contains geographic location text (not numbers)
    col1_is_geo = _col_is_location(data_rows, header_col + 1)

    if col0_empty and col1_is_geo:
        return header_col + 1

    return header_col


def detect_and_merge_headers(extracted_rows):
    if not extracted_rows:
        return None, 0

    header_keywords = [
        'families', 'persons', 'barangay', 'brgys', 'cum', 'now',
        'type', 'date', 'description', 'affected', 'evacuation',
        'centers', 'inside', 'outside', 'status', 'region', 'province',
        'municipality', 'city', 'total', 'incident', 'dswd', 'lgu',
        'ngos', 'others', 'office', 'standby', 'stockpile', 'quantity',
        'cost', 'food', 'non-food', 'relief', 'items', 'damaged',
        'totally', 'partially',
    ]

    # Pattern that identifies a data row's location cell — if ANY cell in a
    # row matches this, the row is definitely data, not a header.
    _DATA_ROW_RE = re.compile(
        r'^(GRAND\s+TOTAL|REGION\s+(\d+|[IVX]+)|CALABARZON|MIMAROPA|CARAGA|'
        r'NCR|CAR|BARMM|ARMM|NATIONAL\s+CAPITAL)',
        re.IGNORECASE,
    )

    last_header_idx = -1
    for idx in range(min(8, len(extracted_rows))):
        row = extracted_rows[idx]
        if not row:
            continue

        # Check ALL cells (not just col 0) for data-row signals.
        # This handles tables where col 0 is always None (location in col 1).
        if any(
            cell and _DATA_ROW_RE.match(str(cell).strip())
            for cell in row
        ):
            break

        header_cnt = data_cnt = 0
        for cell in row:
            if not cell:
                continue
            s = str(cell).lower().strip()
            if any(k in s for k in header_keywords):
                header_cnt += 1
            elif s.replace(',', '').replace('.', '').isdigit():
                data_cnt += 1
            elif len(s) > 50:
                data_cnt += 1
        if data_cnt > header_cnt and data_cnt > 0:
            break
        last_header_idx = idx

    if last_header_idx < 0:
        return None, 0

    data_start  = last_header_idx + 1
    header_rows = extracted_rows[:data_start]
    num_cols    = max(len(r) for r in extracted_rows)

    filled = []
    for row in header_rows:
        new_row, last = [], None
        for cell in row:
            v = str(cell).strip() if cell else ""
            if v:
                last = v
            new_row.append(last or "")
        while len(new_row) < num_cols:
            new_row.append("")
        filled.append(new_row)

    merged = []
    for col in range(num_cols):
        parts = []
        for row in filled:
            v = row[col] if col < len(row) else ""
            if v and v not in parts:
                parts.append(v)
        raw   = "_".join(parts)
        clean = re.sub(r'[^A-Za-z0-9_]', '_', raw)
        clean = re.sub(r'_+', '_', clean).strip('_')
        merged.append(clean or f"Column_{col}")

    return merged, data_start


# -----------------------------------------------------------------------
# MAIN PDF PROCESSOR
# -----------------------------------------------------------------------

def process_pdf(event: DromicEvent, file_counter: int, pdf_path: str):
    print(f"\n📄 [{file_counter}] Processing: {pdf_path}")

    narrative = extract_situation_overview(pdf_path)
    if narrative:
        event.remarks = narrative
        print(f"   → Narrative: {len(narrative)} chars")

    # --- Location from PDF title (overrides filename-derived location) ---
    pdf_location = extract_location_from_title(pdf_path)
    if pdf_location:
        event.location = pdf_location
        print(f"   → Location (from title): {pdf_location}")
    elif event.location:
        print(f"   → Location (from filename): {event.location}")

    report_dt = extract_report_date_from_title(pdf_path)
    if report_dt:
        event.reportDate     = report_dt.strftime("%Y-%m-%d")
        event.lastUpdateDate = report_dt.strftime("%Y-%m-%d")
        print(f"   → Report date: {event.reportDate}")
    elif event.reportDate:
        event.lastUpdateDate = event.reportDate
        print(f"   → Report date (from filename): {event.reportDate}")

    start_iso, end_iso = extract_incident_dates(narrative, event.eventType)
    if start_iso:
        event.startDate = start_iso
        event.endDate   = end_iso
        print(f"   → Incident dates: {start_iso} → {end_iso}")

    pdf_name    = os.path.splitext(os.path.basename(pdf_path))[0]
    folder_name = make_folder_name(os.path.basename(pdf_path)) or (event.eventName or pdf_name)
    output_dir  = os.path.join(OUTPUT_FOLDER, folder_name)
    os.makedirs(output_dir, exist_ok=True)

    all_tables_buffer  = {}   # label → list[dict]
    table_headers_used = {}   # label → (col_headers, data_start, loc_col_idx)
    headers_to_label   = {}   # tuple(col_headers) → label  (merged-header-key match)
    rowsig_to_label    = {}   # tuple(raw_row_sig)  → label  (raw-row-signature match)
    colcount_to_label  = {}   # int(num_cols)       → label  (last-resort fallback)

    def _row_signature(rows) -> tuple:
        """
        Produce a stable fingerprint from the first 2 non-empty rows of a table.
        Uses the normalised text of each cell, ignoring None/empty cells.
        This is much more discriminating than column count alone because it
        captures the actual header text (e.g. "NUMBER OF AFFECTED" vs
        "NUMBER OF DISPLACED INSIDE ECs") even when both tables have 4 cols.
        """
        sig_parts = []
        for row in rows[:2]:
            if not row:
                continue
            cells = tuple(
                re.sub(r'\s+', ' ', str(c).strip().upper())
                for c in row if c and str(c).strip()
            )
            if cells:
                sig_parts.append(cells)
        return tuple(sig_parts)

    with pdfplumber.open(pdf_path) as pdf:
        pages = pdf.pages
        for page_index, page in enumerate(pages, start=1):
            prev_page = pages[page_index - 2] if page_index >= 2 else None
            tables_found = page.find_tables({
                "vertical_strategy"  : "lines",
                "horizontal_strategy": "lines",
                "snap_tolerance"     : 5,
            })
            if not tables_found:
                continue

            for table_obj in tables_found:
                label          = get_table_label(page, table_obj.bbox, prev_page=prev_page)
                extracted_rows = table_obj.extract()
                row_geometries = table_obj.rows

                if label == "unknown_table":
                    # ── Strategy 1: merged-header key ─────────────────
                    candidate_headers, _ = detect_and_merge_headers(extracted_rows)
                    hkey = tuple(candidate_headers or [])
                    if hkey and hkey in headers_to_label:
                        label = headers_to_label[hkey]
                        print(f"   → Page {page_index}: table '{label}' (continuation, header match)")

                    # ── Strategy 2: raw-row signature ──────────────────
                    # Most reliable for multi-page tables: matches the actual
                    # header text (e.g. "NUMBER OF AFFECTED") even when the
                    # merged header key differs slightly across pages.
                    else:
                        rsig = _row_signature(extracted_rows)
                        if rsig and rsig in rowsig_to_label:
                            label = rowsig_to_label[rsig]
                            print(f"   → Page {page_index}: table '{label}' (continuation, row-sig)")

                        # ── Strategy 3: column-count last resort ───────
                        # Only used when the continuation page has NO header
                        # rows at all (pure data fragment, e.g. page 2 of a
                        # table whose header was entirely on the prior page).
                        elif not rsig:
                            num_cols = max((len(r) for r in extracted_rows if r), default=0)
                            if num_cols and num_cols in colcount_to_label:
                                label = colcount_to_label[num_cols]
                                print(f"   → Page {page_index}: table '{label}' (continuation, col-count={num_cols})")
                            else:
                                print(f"   → Page {page_index}: table '{label}'")
                        else:
                            print(f"   → Page {page_index}: table '{label}'")
                else:
                    print(f"   → Page {page_index}: table '{label}'")

                if label not in all_tables_buffer:
                    all_tables_buffer[label] = []

                if label not in table_headers_used:
                    col_headers, data_start = detect_and_merge_headers(extracted_rows)
                    loc_col_idx = find_location_col_idx(extracted_rows)
                    table_headers_used[label] = (col_headers, data_start, loc_col_idx)
                    if label != "unknown_table":
                        # Register all three lookup keys
                        hkey = tuple(col_headers or [])
                        if hkey:
                            headers_to_label[hkey] = label
                        rsig = _row_signature(extracted_rows)
                        if rsig:
                            rowsig_to_label[rsig] = label
                        num_cols = len(col_headers) if col_headers else 0
                        if num_cols:
                            colcount_to_label[num_cols] = label
                else:
                    col_headers, data_start, loc_col_idx = table_headers_used[label]
                    # If this is a data-only continuation (no headers on this
                    # page fragment), the inherited data_start from the header-
                    # only stub on the prior page would skip every row here.
                    if data_start > 0 and extracted_rows:
                        first_row_text = " ".join(
                            str(c) for c in (extracted_rows[0] or []) if c
                        ).lower()
                        header_kws = {"families","persons","region","province",
                                      "municipality","barangay","dswd","lgu",
                                      "ngos","others","cost","affected","displaced"}
                        row_looks_like_header = any(k in first_row_text for k in header_kws)
                        if not row_looks_like_header:
                            data_start = 0   # treat all rows as data

                current_region = current_province = current_muni = None

                for row_idx, (row_obj, row_text) in enumerate(
                        zip(row_geometries, extracted_rows)):
                    if data_start > 0 and row_idx < data_start:
                        continue
                    if not row_obj.cells:
                        continue

                    loc_bbox = (row_obj.cells[loc_col_idx]
                                if loc_col_idx < len(row_obj.cells) else None)
                    if loc_bbox is None:
                        for ci in range(len(row_obj.cells)):
                            if row_obj.cells[ci] is not None:
                                loc_bbox = row_obj.cells[ci]
                                break

                    # Pass the adjacent cell (loc_col_idx + 1) as fallback for
                    # two-column location layouts where col 0 has a decorative
                    # single letter and col 1 has the real location text.
                    fallback_idx  = loc_col_idx + 1
                    fallback_bbox = (row_obj.cells[fallback_idx]
                                     if fallback_idx < len(row_obj.cells) else None)
                    level, text = get_location_level(page, loc_bbox, fallback_bbox=fallback_bbox)

                    if level == "skip":
                        continue
                    if text and re.search(
                            r'(Note|Source)\s*[:.] |ongoing assessment|validation being|'
                            r'Hence,\s+ongoing|submitted by DSWD',
                            text, re.IGNORECASE):
                        continue

                    is_summary = (level == "summary")

                    if level == "region":
                        current_region   = text
                        current_province = None
                        current_muni     = None
                    elif level == "province":
                        current_province = text
                        current_muni     = None
                    elif level == "muni":
                        current_muni = text

                    rd = {
                        "Page"        : page_index,
                        "TableLabel"  : label,
                        "Region"      : current_region   if not is_summary else None,
                        "Province"    : current_province if not is_summary else None,
                        "City_Muni"   : current_muni     if not is_summary else None,
                        "Summary_Type": text              if is_summary     else None,
                    }

                    for col_idx, cell in enumerate(row_text):
                        if col_idx == loc_col_idx:
                            continue
                        val = (cell or "").replace("\n", " ").strip()
                        if col_headers and col_idx < len(col_headers):
                            hdr = col_headers[col_idx]
                        else:
                            hdr = f"Column_{col_idx}"
                        if hdr.startswith("Column_"):
                            continue
                        rd[hdr] = val

                    data_vals = [v for k, v in rd.items()
                                 if k not in ("Page", "TableLabel", "Region", "Province",
                                              "City_Muni", "Summary_Type")]
                    if not any(data_vals):
                        continue

                    all_tables_buffer[label].append(rd)

    for label, rows in all_tables_buffer.items():
        if not rows:
            continue
        df = pd.DataFrame(rows)
        csv_path = os.path.join(output_dir, f"{label}.csv")
        df.to_csv(csv_path, index=False)
        print(f"   ✓ Saved: {csv_path}  ({len(rows)} rows)")

    _generate_json(event, output_dir)
    print(f"   ✓ Metadata saved for: {event.eventName}")


def _generate_json(event: DromicEvent, output_dir: str):
    event_dict = asdict(event)
    metadata = {
        "eventName"    : event.eventName,
        "eventType"    : event.eventType,
        "location"     : event.location,
        "reportNumber" : event.reportNumber,
        "reportDate"   : event.reportDate,
        "startDate"    : event.startDate,
        "endDate"      : event.endDate,
        "remarks"      : event.remarks,
    }
    source = {k: v for k, v in event_dict.items() if k not in metadata}
    with open(os.path.join(output_dir, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=4)
    with open(os.path.join(output_dir, "source.json"), "w") as f:
        json.dump(source, f, indent=4)


# -----------------------------------------------------------------------
# ENTRY POINTS
# -----------------------------------------------------------------------

def build_event_from_filename(filename: str) -> DromicEvent:
    parsed      = parse_dromic_filename(filename)
    event_type  = parsed["event_type"]
    location    = parsed["location_raw"]
    is_terminal = parsed["is_terminal"]

    folder     = make_folder_name(filename)
    event_name = re.sub(r'\s+20\d{2}$', '', folder).strip()

    if event_type == "Preparedness" and not event_name.lower().startswith("preparedness"):
        event_name = f"Preparedness – {event_name}"
    if is_terminal and event_name:
        event_name = f"{event_name} (Terminal)"

    event = DromicEvent(
        reportName   = filename,
        eventType    = event_type,
        location     = location,
        eventName    = event_name,
        reportNumber = parsed["report_number"],
    )
    as_of_str = parsed["as_of_date_str"]
    if as_of_str:
        dt = parse_flexible_date(as_of_str)
        if dt:
            event.reportDate = dt.strftime("%Y-%m-%d")
    return event


def process_all_pdfs(parallel: bool = False):
    # ── Pre-flight check for docx2pdf ─────────────────────────────────
    has_docx = any(
        f.lower().endswith(".docx")
        for f in os.listdir(INPUT_FOLDER)
        if os.path.isfile(os.path.join(INPUT_FOLDER, f))
    )
    if has_docx and not _DOCX2PDF_AVAILABLE:
        print(
            "⚠️  WARNING: DOCX files detected but docx2pdf is not installed.\n"
            "   Run:  pip install docx2pdf\n"
            "   Microsoft Word must also be installed on this machine.\n"
            "   DOCX files will be SKIPPED until docx2pdf is available."
        )
    elif has_docx:
        print("✅ docx2pdf ready — DOCX files will be converted via Microsoft Word.")

    print("🔎 Scanning folder for PDF and DOCX files…")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    files = [
        f for f in os.listdir(INPUT_FOLDER)
        if f.lower().endswith((".pdf", ".docx"))
    ]

    if not files:
        print("❌ No files found.")
        return

    def _process_one(f: str, idx: int):
        original_path = os.path.join(INPUT_FOLDER, f)
        tmp_pdf = None

        if f.lower().endswith(".docx"):
            print(f"\n🔄 Converting DOCX → PDF: {f}")
            try:
                tmp_pdf  = docx_to_pdf(original_path)
                pdf_path = tmp_pdf
            except RuntimeError as e:
                print(f"   ❌ Skipping {f}: {e}")
                return
        else:
            pdf_path = original_path

        try:
            process_pdf(build_event_from_filename(f), idx, pdf_path)
        finally:
            _cleanup_tmp(tmp_pdf)

    if parallel:
        with ProcessPoolExecutor() as executor:
            futures = {
                executor.submit(_process_one, f, idx + 1): f
                for idx, f in enumerate(files)
            }
            for future in as_completed(futures):
                fname = futures[future]
                try:
                    future.result()
                    print(f"✓ Finished {fname}")
                except Exception as e:
                    print(f"❌ Error processing {fname}: {e}")
    else:
        for idx, f in enumerate(files, start=1):
            try:
                _process_one(f, idx)
            except Exception as e:
                print(f"❌ Error processing {f}: {e}")

    print(f"\n🎉 Done! Processed {len(files)} file(s).")


if __name__ == "__main__":
    process_all_pdfs(parallel=False)

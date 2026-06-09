import os
import pdfplumber
import pandas as pd
import numpy as np
import re
import json
import argparse
from datetime import datetime
from dataclasses import dataclass, asdict
from concurrent.futures import ProcessPoolExecutor, as_completed

# --------------------------
# CONFIGURATION
# --------------------------

INPUT_FOLDER = "../data/raw/ndrrmc_new"
OUTPUT_FOLDER = "../data/parsed/ndrrmc"
FOLDER_LENGTH = 0
HEADER_SEARCH_DISTANCE = 80
ALIGNMENT_TOLERANCE = 5
OCR_MIN_CHARS = 20          # min non-whitespace chars before falling back to OCR
OCR_RESOLUTION = 300        # DPI for page rasterisation
OCR_CONF_THRESHOLD = 30     # ignore Tesseract words with confidence below this
OCR_Y_CLUSTER_TOLERANCE = 8  # pixels: words within this vertical distance → same line
OCR_X_COL_TOLERANCE = 15    # pixels: gap larger than this between words → column break


# -----------------------------------------------------------------------
# LAYOUT-AWARE OCR  (Surya-style bounding-box reconstruction)
# -----------------------------------------------------------------------

class LayoutOCR:
    """
    Drop-in replacement for plain pytesseract.image_to_string() that uses
    Tesseract's TSV output (word-level bounding boxes) to:

      1. Reconstruct reading order and paragraph text  (for narrative blocks)
      2. Detect table structure by clustering words into rows and columns
         purely from their X/Y positions                (for table cells)

    This mimics what Surya does with its layout-analysis + OCR pipeline,
    using only Tesseract 5 + pytesseract which are already available.
    """

    def __init__(self, resolution: int = OCR_RESOLUTION,
                 conf_threshold: int = OCR_CONF_THRESHOLD,
                 y_tol: int = OCR_Y_CLUSTER_TOLERANCE,
                 x_col_tol: int = OCR_X_COL_TOLERANCE):
        try:
            import pytesseract
            pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        except ImportError:
            raise ImportError(
                "pytesseract is required. Install with: pip install pytesseract\n"
                "Also install Tesseract OCR: https://github.com/tesseract-ocr/tesseract"
            )
        self.pytesseract = pytesseract
        self.resolution = resolution
        self.conf_threshold = conf_threshold
        self.y_tol = y_tol
        self.x_col_tol = x_col_tol

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _rasterise(self, pdfplumber_page):
        """Convert a pdfplumber page to a PIL Image."""
        return pdfplumber_page.to_image(resolution=self.resolution).original

    def _get_words_df(self, pil_image) -> pd.DataFrame:
        """Run Tesseract and return a filtered DataFrame of words with geometry."""
        df = self.pytesseract.image_to_data(
            pil_image,
            output_type=self.pytesseract.Output.DATAFRAME,
            config="--oem 3 --psm 11",   # sparse text — good for mixed layout pages
        )
        df = df[df["conf"] >= self.conf_threshold].copy()
        df = df[df["text"].notna() & (df["text"].str.strip() != "")]
        df = df.reset_index(drop=True)
        # centre-y for clustering
        df["cy"] = df["top"] + df["height"] / 2
        return df

    def _cluster_lines(self, words_df: pd.DataFrame) -> list[list[dict]]:
        """
        Group words into lines by proximity of their centre-Y coordinate.
        Returns list of lines; each line is a list of word dicts sorted by x.
        """
        if words_df.empty:
            return []

        lines: list[list[dict]] = []
        current_line: list[dict] = []
        prev_cy = None

        for _, row in words_df.sort_values("cy").iterrows():
            word = row.to_dict()
            if prev_cy is None or abs(word["cy"] - prev_cy) <= self.y_tol:
                current_line.append(word)
            else:
                if current_line:
                    lines.append(sorted(current_line, key=lambda w: w["left"]))
                current_line = [word]
            prev_cy = word["cy"]

        if current_line:
            lines.append(sorted(current_line, key=lambda w: w["left"]))

        # Re-sort lines top-to-bottom by median cy
        lines.sort(key=lambda ln: np.median([w["cy"] for w in ln]))
        return lines

    def _line_to_text(self, line: list[dict]) -> str:
        return " ".join(w["text"] for w in line)

    # ------------------------------------------------------------------
    # Public: narrative text extraction
    # ------------------------------------------------------------------

    def extract_text(self, pdfplumber_page) -> str:
        """
        Extract narrative/plain text from a scanned page preserving reading order.
        Equivalent to the old pytesseract.image_to_string() but layout-aware.
        """
        img = self._rasterise(pdfplumber_page)
        words_df = self._get_words_df(img)
        lines = self._cluster_lines(words_df)
        return "\n".join(self._line_to_text(ln) for ln in lines)

    # ------------------------------------------------------------------
    # Public: table extraction
    # ------------------------------------------------------------------

    def _detect_column_boundaries(self, all_lines: list[list[dict]]) -> list[tuple[float, float]]:
        """
        Infer column x-ranges from the word positions across all lines.
        Uses a gap-finding approach: collect all word left edges, cluster them.
        Returns list of (x_start, x_end) tuples sorted left-to-right.
        """
        if not all_lines:
            return []

        # Collect all (left, right) spans
        spans = [(w["left"], w["left"] + w["width"]) for ln in all_lines for w in ln]
        if not spans:
            return []

        # Find gaps: sort all x positions and look for spaces > x_col_tol
        all_lefts = sorted(set(s[0] for s in spans))
        if len(all_lefts) < 2:
            return [(min(s[0] for s in spans), max(s[1] for s in spans))]

        columns = []
        col_start = all_lefts[0]
        col_end = all_lefts[0]

        for x in all_lefts[1:]:
            if x - col_end > self.x_col_tol:
                columns.append((col_start, col_end + self.x_col_tol))
                col_start = x
            col_end = x

        columns.append((col_start, col_end + self.x_col_tol))
        return columns

    def _assign_word_to_col(self, word: dict,
                             columns: list[tuple[float, float]]) -> int:
        """Return index of the column whose range best overlaps with word's left edge."""
        wx = word["left"]
        best_col = 0
        best_dist = float("inf")
        for i, (cx0, cx1) in enumerate(columns):
            if cx0 <= wx <= cx1:
                return i
            dist = min(abs(wx - cx0), abs(wx - cx1))
            if dist < best_dist:
                best_dist = dist
                best_col = i
        return best_col

    def extract_table(self, pdfplumber_page,
                       table_bbox: tuple | None = None) -> list[list[str]]:
        """
        Extract a table from a (possibly scanned) page using layout analysis.

        Parameters
        ----------
        pdfplumber_page : pdfplumber Page object
        table_bbox      : (x0, top, x1, bottom) in PDF points, or None for full page.
                          If given, only the region inside this bbox is analysed.

        Returns
        -------
        list of rows, each row is a list of cell strings (may be empty strings).
        """
        img = self._rasterise(pdfplumber_page)

        if table_bbox is not None:
            # Convert PDF-point bbox → pixel bbox
            pw = pdfplumber_page.width
            ph = pdfplumber_page.height
            iw, ih = img.size
            sx = iw / pw
            sy = ih / ph
            x0, top, x1, bottom = table_bbox
            px0, ptop, px1, pbottom = int(x0*sx), int(top*sy), int(x1*sx), int(bottom*sy)
            # add small margin
            margin = int(4 * sx)
            px0 = max(0, px0 - margin)
            ptop = max(0, ptop - margin)
            px1 = min(iw, px1 + margin)
            pbottom = min(ih, pbottom + margin)
            img = img.crop((px0, ptop, px1, pbottom))

        words_df = self._get_words_df(img)
        if words_df.empty:
            return []

        lines = self._cluster_lines(words_df)
        if not lines:
            return []

        columns = self._detect_column_boundaries(lines)
        if not columns:
            return [[self._line_to_text(ln)] for ln in lines]

        n_cols = len(columns)
        rows: list[list[str]] = []

        for line in lines:
            cells: list[list[str]] = [[] for _ in range(n_cols)]
            for word in line:
                col_idx = self._assign_word_to_col(word, columns)
                cells[col_idx].append(word["text"])
            rows.append([" ".join(c) for c in cells])

        return rows

    # ------------------------------------------------------------------
    # Public: full page analysis — returns both text and table regions
    # ------------------------------------------------------------------

    def extract_page_layout(self, pdfplumber_page) -> dict:
        """
        Full layout analysis of a page. Returns:
          {
            'text':   str   — narrative text (non-table regions),
            'tables': list of list[list[str]]  — each detected table as row/cell grid,
            'table_bboxes': list of (x0, top, x1, bottom)
          }
        Uses pdfplumber's line-detection to find table bounding boxes first;
        the remaining area is treated as narrative text.
        """
        page = pdfplumber_page

        # Attempt to find table regions via drawn lines (works even on scanned pages
        # if the scanner preserved the lines in the PDF layer)
        table_objs = page.find_tables({
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "snap_tolerance": 5,
        })
        table_bboxes = [t.bbox for t in table_objs]

        tables = []
        for bbox in table_bboxes:
            rows = self.extract_table(page, table_bbox=bbox)
            tables.append(rows)

        # If no drawn lines found, try text-clustering to detect table-like regions
        if not table_bboxes:
            img = self._rasterise(page)
            words_df = self._get_words_df(img)
            lines = self._cluster_lines(words_df)
            # Heuristic: if ≥3 words per line on average and ≥3 lines → treat whole page as table
            if lines:
                avg_words = np.mean([len(ln) for ln in lines])
                if avg_words >= 2.5 and len(lines) >= 3:
                    columns = self._detect_column_boundaries(lines)
                    if len(columns) >= 2:
                        n_cols = len(columns)
                        tbl: list[list[str]] = []
                        for line in lines:
                            cells: list[list[str]] = [[] for _ in range(n_cols)]
                            for word in line:
                                ci = self._assign_word_to_col(word, columns)
                                cells[ci].append(word["text"])
                            tbl.append([" ".join(c) for c in cells])
                        tables.append(tbl)
                        # synthetic bbox covering full page
                        table_bboxes.append((0, 0, page.width, page.height))

        # Narrative text = page text minus table regions
        narrative_text = self.extract_text(page)

        return {
            "text": narrative_text,
            "tables": tables,
            "table_bboxes": table_bboxes,
        }


# -----------------------------------------------------------------------
# PAGE TEXT HELPER
# -----------------------------------------------------------------------

_layout_ocr: LayoutOCR | None = None  # module-level singleton (avoid re-init overhead)

def _get_ocr() -> LayoutOCR:
    global _layout_ocr
    if _layout_ocr is None:
        _layout_ocr = LayoutOCR()
    return _layout_ocr


def get_page_text(page, use_ocr: bool = False) -> str:
    """
    Extract text from a pdfplumber page.
    Falls back to layout-aware OCR when native extraction yields little content.
    """
    text = page.extract_text() or ""
    if use_ocr and len(text.replace(" ", "").replace("\n", "")) < OCR_MIN_CHARS:
        print(f"   → Native extraction yielded {len(text.strip())} chars, "
              "falling back to layout-aware OCR...")
        text = _get_ocr().extract_text(page)
    return text


# -----------------------------------------------------------------------
# EVENT DATACLASS
# -----------------------------------------------------------------------

@dataclass
class Event:
    eventName: str = ""
    startDate: str = ""
    endDate: str = ""
    lastUpdateDate: str = ""
    reportName: str = ""
    recordedBy: str = "NDRRMC"
    obtainedDate: str = ""
    reportLink: str = ""
    remarks: str = ""


# -----------------------------------------------------------------------
# MULTI-PAGE NARRATIVE EXTRACTION
# -----------------------------------------------------------------------

def extract_multi_page_narrative(pdf_path: str, event_name: str,
                                  use_ocr: bool = False) -> str:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if len(pdf.pages) == 0:
                return ""

            narrative_parts = []

            table_start_patterns = [
                r'REGION\s*\|\s*PROVINCE',
                r'AFFECTED POPULATION',
                r'RELATED INCIDENTS',
                r'CASUALTIES',
                r'^\s*REGION\s*$',
                r'^\s*PROVINCE\s*$',
                r'Page \d+\s*/\s*\d+',
            ]

            max_narrative_pages = min(5, len(pdf.pages))

            for page_num in range(max_narrative_pages):
                page = pdf.pages[page_num]
                page_text = get_page_text(page, use_ocr=use_ocr)

                if page_num == 0:
                    lines = page_text.split('\n')
                    cleaned_lines = []
                    for i, line in enumerate(lines):
                        line = line.strip()
                        if i < 3:
                            continue
                        if 'Telefax:' in line or 'Email:' in line or 'Websites:' in line:
                            continue
                        if len(line) < 10 and i < 10:
                            continue
                        if line:
                            cleaned_lines.append(line)
                    page_text = '\n'.join(cleaned_lines)

                found_table_start = False
                for pattern in table_start_patterns:
                    if re.search(pattern, page_text, re.IGNORECASE | re.MULTILINE):
                        match = re.search(pattern, page_text, re.IGNORECASE | re.MULTILINE)
                        narrative_parts.append(page_text[:match.start()].strip())
                        found_table_start = True
                        break

                if found_table_start:
                    break
                else:
                    narrative_parts.append(page_text.strip())

            return '\n\n'.join(narrative_parts).strip()

    except Exception as e:
        print(f"Error extracting multi-page narrative: {e}")
        return ""


# -----------------------------------------------------------------------
# DATE EXTRACTION
# -----------------------------------------------------------------------

def extract_dates_from_text(text: str) -> list:
    patterns = [
        r'\b(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}(?:\s+\d{1,2}:\d{2}(?:\s*(?:am|pm|AM|PM))?)?)\b',
        r'\b((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}(?:\s+\d{1,2}:\d{2}(?:\s*(?:am|pm|AM|PM))?)?)\b',
        r'\b(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}(?:\s+\d{1,2}:\d{2}(?:\s*(?:am|pm|AM|PM))?)?)\b',
        r'\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}(?:\s+\d{1,2}:\d{2}(?:\s*(?:am|pm|AM|PM))?)?)\b',
        r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{4})\b',
        r'\((\d{1,2}\s+(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+\d{4})\s*-\s*(\d{1,2}\s+(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+\d{4})\)',
    ]

    found_dates = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            if len(match.groups()) == 2 and match.groups()[1] is not None:
                s = parse_flexible_date(match.group(1))
                e = parse_flexible_date(match.group(2))
                if s:
                    found_dates.append(('range_start', match.group(1), s, match.start()))
                if e:
                    found_dates.append(('range_end', match.group(2), e, match.start()))
            else:
                date_str = match.group(1) if match.groups() else match.group(0)
                dt = parse_flexible_date(date_str)
                if dt:
                    found_dates.append(('single', date_str, dt, match.start()))

    found_dates.sort(key=lambda x: x[3])
    return found_dates


def parse_flexible_date(date_str: str) -> datetime | None:
    date_str = date_str.strip().replace(',', '')
    formats = [
        "%d %B %Y %H:%M %p", "%d %B %Y %H:%M", "%d %B %Y",
        "%B %d %Y %H:%M %p", "%B %d %Y %H:%M", "%B %d %Y",
        "%d %b %Y %H:%M %p", "%d %b %Y %H:%M", "%d %b %Y",
        "%b %d %Y %H:%M %p", "%b %d %Y %H:%M", "%b %d %Y",
        "%d-%m-%Y", "%d/%m/%Y", "%m-%d-%Y", "%m/%d/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def extract_narrative_dates(pdf_path: str, event_name: str,
                              use_ocr: bool = False):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if len(pdf.pages) == 0:
                return None, None, "", None

            first_page_text = get_page_text(pdf.pages[0], use_ocr=use_ocr)
            narrative_text = extract_multi_page_narrative(pdf_path, event_name, use_ocr=use_ocr)
            print(f"   → Extracted narrative: {len(narrative_text)} characters")

            event_lower = event_name.lower()
            is_sitrep = 'sitrep' in event_lower or 'situational report' in narrative_text.lower()
            is_election = 'election' in event_lower or 'bske' in event_lower
            is_monitoring = is_sitrep or 'monitoring' in event_lower or 'cases' in event_lower

            # "As of" date
            as_of_date = None
            for page_num in range(min(2, len(pdf.pages))):
                page_text = get_page_text(pdf.pages[page_num], use_ocr=use_ocr)
                m = re.search(r'as\s+of\s*[(\[]?\s*([^)\]]+?)\s*[)\]]?(?:\s|$)',
                               page_text, re.IGNORECASE)
                if m:
                    dt = parse_flexible_date(m.group(1).strip())
                    if dt:
                        as_of_date = dt
                        print(f"   → Found 'as of' date: {m.group(1).strip()}")
                        break

            # Report date (timestamp near top of page)
            report_date = None
            for dtype, dstr, dobj, pos in extract_dates_from_text(first_page_text[:300]):
                if ':' in dstr and pos < 200:
                    report_date = dobj
                    print(f"   → Found report date: {dstr}")
                    break

            all_narrative_dates = extract_dates_from_text(narrative_text)
            print(f"   → Found {len(all_narrative_dates)} dates in narrative")

            narrative_dates = [
                d for d in all_narrative_dates
                if not (d[3] < 200 and ':' in d[1])
            ]

            if is_monitoring and not narrative_dates:
                print("   → Detected monitoring report")
                if as_of_date:
                    return as_of_date, as_of_date, narrative_text, report_date
                if report_date:
                    return report_date, report_date, narrative_text, report_date
                return None, None, narrative_text, None

            if is_election:
                print("   → Detected election event")
                for pattern in [
                    r'scheduled\s+for\s+(\d{1,2}\s+\w+\s+\d{4})',
                    r'election.*?(\d{1,2}\s+\w+\s+\d{4})',
                    r'(\d{1,2}\s+\w+\s+\d{4}).*?election',
                ]:
                    m = re.search(pattern, narrative_text, re.IGNORECASE)
                    if m:
                        dt = parse_flexible_date(m.group(1))
                        if dt:
                            print(f"   → Found election date: {m.group(1)}")
                            return dt, dt, narrative_text, report_date
                if narrative_dates:
                    return narrative_dates[0][2], narrative_dates[0][2], narrative_text, report_date

            if not narrative_dates:
                print("   → No narrative dates found")
                if as_of_date:
                    return as_of_date, as_of_date, narrative_text, report_date
                if report_date:
                    return report_date, report_date, narrative_text, report_date
                return None, None, narrative_text, None

            range_starts = [d for d in narrative_dates if d[0] == 'range_start']
            range_ends   = [d for d in narrative_dates if d[0] == 'range_end']

            if range_starts and range_ends:
                print("   → Using explicit date range")
                return range_starts[0][2], range_ends[0][2], narrative_text, report_date

            date_objs = sorted(d[2] for d in narrative_dates)
            print(f"   → Using chronological range from {len(date_objs)} dates")
            return date_objs[0], date_objs[-1], narrative_text, report_date

    except Exception as e:
        import traceback
        print(f"Error extracting dates: {e}")
        traceback.print_exc()
        return None, None, "", None


# -----------------------------------------------------------------------
# HELPER FUNCTIONS
# -----------------------------------------------------------------------

def get_lastUpdateDateTime(event: Event, lastUpdateDateTime: str):
    formats = [
        "%b %d, %Y", "%B %d %Y", "%B %d, %Y %H:%M", "%b %d, %Y %H:%M"
    ]
    for fmt in formats:
        try:
            event.lastUpdateDate = datetime.strptime(lastUpdateDateTime, fmt).strftime("%Y-%m-%d %H:%M:%S")
            return
        except ValueError:
            continue
    event.lastUpdateDate = lastUpdateDateTime


def clean_tablename(event: Event, table_title: str) -> str:
    if not table_title:
        return "Unknown_Section"
    if "as of" in table_title.lower():
        parts = table_title.split("as of")
        table_title = parts[0]
        raw_date = parts[1].replace("(", "").replace(")", "").strip()
        get_lastUpdateDateTime(event, raw_date)
    table_title = table_title.strip().replace(" ", "_").lower()
    return "".join(c for c in table_title if c.isalnum() or c in "._-")


abbrev_map = {
    "Tropical Storm": "TS", "Typhoon": "TY", "Tropical Cyclone": "TC",
    "Situational Report": "SitRep", "Southwest Monsoon": "SWM",
    "Low Pressure Area": "LPA", "Terminal Report": "TR", "Final Report": "FR",
}

def normalize_subject(text: str) -> str:
    for full, abbr in abbrev_map.items():
        text = re.sub(full, abbr, text, flags=re.IGNORECASE)
    return text

def clean_filename(filename: str) -> str:
    name = filename.replace(".pdf", "")
    name = re.sub(r"(Breakdown|Final_Report|SitRep|Situational_Report|Terminal_Report|Table)",
                  "", name, flags=re.IGNORECASE)
    name = name.replace("_", " ")
    m = re.search(r"for (.+)", name, flags=re.IGNORECASE)
    subject = m.group(1).strip() if m else name.strip()
    subject = re.sub(r"(Breakdown.*)$", "", subject, flags=re.IGNORECASE).strip()
    subject = subject.replace("the", "").replace(" -", "").lstrip().replace("(", "").replace(")", "")
    return normalize_subject(subject)

def generate_json(event: Event, output_dir: str):
    event_dict = asdict(event)
    metadata = {k: event_dict[k] for k in ("eventName", "startDate", "endDate", "remarks")}
    source = {k: v for k, v in event_dict.items() if k not in metadata}

    for path, data in [
        (os.path.join(output_dir, "metadata.json"), metadata),
        (os.path.join(output_dir, "source.json"), source),
    ]:
        with open(path, "w") as f:
            json.dump(data, f, indent=4)
        print(f"✓ Saved: {path}")


def get_text_alignment_and_case(page, cell_bbox):
    if not cell_bbox:
        return None, None, ""
    try:
        words = page.crop(cell_bbox).extract_words()
    except ValueError:
        return None, None, ""
    if not words:
        return None, None, ""

    text = " ".join(w["text"] for w in words).strip()
    cell_x0, _, cell_x1, _ = cell_bbox
    text_x0 = min(w["x0"] for w in words)
    text_x1 = max(w["x1"] for w in words)
    left_margin  = text_x0 - cell_x0
    right_margin = cell_x1 - text_x1

    if abs(left_margin - right_margin) < ALIGNMENT_TOLERANCE:
        alignment = "CENTER"
    elif left_margin < ALIGNMENT_TOLERANCE * 2:
        alignment = "LEFT"
    elif right_margin < ALIGNMENT_TOLERANCE * 2:
        alignment = "RIGHT"
    else:
        alignment = "UNKNOWN"

    case_type = "UPPER" if text.isupper() else ("TITLE" if text.istitle() else "MIXED")
    return alignment, case_type, text


def is_summary_row(text: str) -> bool:
    if not text:
        return False
    upper = text.upper().strip()
    return any(p in upper for p in [
        'GRAND TOTAL', 'TOTAL', 'SUB-TOTAL', 'SUB TOTAL', 'SUBTOTAL',
        'INJURED/ILL', 'INJURED', 'ILL', 'DEAD', 'MISSING',
        'OVERALL', 'SUMMARY', 'CASUALTIES',
    ])


# -----------------------------------------------------------------------
# HEADER DETECTION
# -----------------------------------------------------------------------

def detect_and_merge_headers_with_spanning(extracted_rows):
    if not extracted_rows:
        return None, 0

    header_keywords = [
        'families', 'persons', 'barangay', 'brgys', 'cum', 'now',
        'type', 'date', 'time', 'description', 'actions', 'remarks',
        'affected', 'evacuation', 'centers', 'inside', 'outside',
        'status', 'region', 'province', 'municipality', 'city',
        'total', 'served', 'current', 'incident', 'occurrence',
    ]

    last_header_idx = -1
    for idx in range(min(8, len(extracted_rows))):
        row = extracted_rows[idx]
        if not row:
            continue
        first_cell = str(row[0]).strip() if row[0] else ""

        if (re.match(r'^REGION\s+(\d+|[IVX]+)', first_cell, re.IGNORECASE) or
                re.match(r'^GRAND\s+TOTAL', first_cell, re.IGNORECASE)):
            break
        if re.match(r'^(DEAD|INJURED|MISSING|CASUALTIES)', first_cell, re.IGNORECASE):
            break

        header_like = data_like = 0
        for cell in row:
            if cell is None or str(cell).strip() == '':
                continue
            cs = str(cell).lower().strip()
            if any(kw in cs for kw in header_keywords):
                header_like += 1
            elif cs.replace(',', '').replace('.', '').isdigit():
                data_like += 1
            elif len(cs) > 50:
                data_like += 1

        if data_like > header_like and data_like > 0:
            break
        last_header_idx = idx

    if last_header_idx < 0:
        return None, 0

    data_start_idx = last_header_idx + 1
    header_rows = extracted_rows[:data_start_idx]
    num_columns = len(extracted_rows[0])

    # Forward-fill within each header row
    filled = []
    for row in header_rows:
        new_row, last = [], None
        for cell in row:
            val = str(cell).strip() if cell else ""
            if val:
                last = val
                new_row.append(val)
            else:
                new_row.append(last or "")
        filled.append(new_row)

    # Vertical merge
    merged_headers = []
    for col_idx in range(num_columns):
        parts = []
        for row in filled:
            val = row[col_idx] if col_idx < len(row) else ""
            if val and val not in parts:
                parts.append(val)
        if not parts:
            merged_headers.append(f"Column_{col_idx}")
        else:
            raw = "_".join(parts)
            clean = raw.replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '').replace('.', '')
            merged_headers.append(re.sub(r'_+', '_', clean))

    print(f"   → Detected {data_start_idx} header rows")
    return merged_headers, data_start_idx


# -----------------------------------------------------------------------
# CROSS-PAGE TITLE TRACKING
# -----------------------------------------------------------------------

class SimpleTitleTracker:
    def __init__(self):
        self.pending_title = None
        self.pending_title_page = None

    def check_page_bottom(self, page, page_num):
        page_height = page.height
        bottom_crop = page.crop((0, page_height - 150, page.width, page_height))
        bottom_text = bottom_crop.extract_text() or ""
        lines = [l.strip() for l in bottom_text.split("\n") if l.strip()]

        potential_title = None
        for line in reversed(lines):
            if len(line) < 5 or re.match(r'^\d+$', line) or 'Page' in line:
                continue
            if line.isupper() or 'as of' in line.lower() or 'AFFECTED POPULATION' in line.upper():
                potential_title = line
                break

        if potential_title:
            tables = page.find_tables({
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "snap_tolerance": 5,
            })
            title_y = page_height - 50
            inside = any(t.bbox[1] < title_y < t.bbox[3] for t in tables)
            if not inside:
                self.pending_title = potential_title
                self.pending_title_page = page_num
                print(f"   → Potential carry-over title (page {page_num}): {potential_title}")
                return potential_title
        return None

    def get_title_for_table(self, page, page_num, table_bbox):
        if (self.pending_title and
                self.pending_title_page == page_num - 1 and
                table_bbox[1] < page.height * 0.3):
            title = self.pending_title
            self.pending_title = None
            print(f"   → Using carry-over title: {title}")
            return title

        x0, top, x1, _ = table_bbox
        try:
            header_text = page.crop(
                (0, max(0, top - HEADER_SEARCH_DISTANCE), page.width, top)
            ).extract_text() or ""
            lines = [l.strip() for l in header_text.split("\n") if l.strip()]
            if lines:
                pt = lines[-1]
                if pt.isupper() or len(pt) < 100:
                    return pt
        except Exception:
            pass
        return None


# -----------------------------------------------------------------------
# OCR TABLE EXTRACTION — used when native pdfplumber finds no drawn lines
# -----------------------------------------------------------------------

def _ocr_table_rows_from_layout(page, table_bbox,
                                  column_headers, data_start_idx):
    """
    Use LayoutOCR to extract table rows from a scanned/image-based table region.
    Returns a list of row dicts compatible with the main row-building loop.
    """
    ocr = _get_ocr()
    rows = ocr.extract_table(page, table_bbox=table_bbox)
    if not rows:
        return []

    # If we don't have headers yet, detect them from the OCR rows
    if column_headers is None:
        column_headers, data_start_idx = detect_and_merge_headers_with_spanning(rows)

    results = []
    for row_idx, row in enumerate(rows):
        if data_start_idx and row_idx < data_start_idx:
            continue
        rd = {"Page": None, "Region": None, "Province": None,
              "City_Muni": None, "Barangay": None, "Summary_Type": None}
        for col_idx, cell in enumerate(row):
            if col_idx == 0:
                continue
            header = (column_headers[col_idx]
                      if column_headers and col_idx < len(column_headers)
                      else f"Column_{col_idx}")
            rd[header] = cell.replace("\n", " ").strip()
        results.append(rd)

    return results, column_headers, data_start_idx


# -----------------------------------------------------------------------
# MAIN PROCESSOR
# -----------------------------------------------------------------------

def process_pdf(pdf_event: Event, file_counter: int,
                pdf_path: str, use_ocr: bool = False):
    print(f"\n📄{file_counter} Processing PDF: {pdf_path}")
    if use_ocr:
        print("   → Layout-aware OCR mode enabled (bounding-box reconstruction)")

    start_date, end_date, narrative_text, report_date = \
        extract_narrative_dates(pdf_path, pdf_event.eventName, use_ocr=use_ocr)

    if narrative_text:
        pdf_event.remarks = narrative_text
        print(f"   → Extracted narrative ({len(narrative_text)} chars)")
    if start_date:
        pdf_event.startDate = start_date.strftime("%Y-%m-%d")
        print(f"   → Start Date: {pdf_event.startDate}")
    if end_date:
        pdf_event.endDate = end_date.strftime("%Y-%m-%d")
        print(f"   → End Date: {pdf_event.endDate}")

    output_dir = os.path.join(OUTPUT_FOLDER, pdf_event.eventName)
    os.makedirs(output_dir, exist_ok=True)

    current_title = "Unknown_Section"
    all_tables_buffer: dict[str, list] = {}
    table_headers_used: dict[str, tuple] = {}
    title_tracker = SimpleTitleTracker()

    with pdfplumber.open(pdf_path) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            title_tracker.check_page_bottom(page, page_index)

            native_text = page.extract_text() or ""
            page_is_scanned = (
                use_ocr and
                len(native_text.replace(" ", "").replace("\n", "")) < OCR_MIN_CHARS
            )

            tables_found = page.find_tables({
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "snap_tolerance": 5,
            })

            if not tables_found:
                if page_is_scanned:
                    # --- OCR full-page layout analysis for scanned pages ---
                    print(f"   ⚙ Page {page_index}: scanned, running layout OCR for tables…")
                    layout = _get_ocr().extract_page_layout(page)

                    for tbl_idx, (tbl_rows, tbl_bbox) in enumerate(
                            zip(layout["tables"], layout["table_bboxes"])):

                        potential_title = title_tracker.get_title_for_table(
                            page, page_index, tbl_bbox)
                        if potential_title:
                            current_title = clean_tablename(pdf_event, potential_title)

                        if current_title not in all_tables_buffer:
                            all_tables_buffer[current_title] = []

                        col_headers, ds_idx = table_headers_used.get(
                            current_title, (None, 0))
                        if col_headers is None:
                            col_headers, ds_idx = detect_and_merge_headers_with_spanning(tbl_rows)
                            table_headers_used[current_title] = (col_headers, ds_idx)

                        for row_idx, row in enumerate(tbl_rows):
                            if ds_idx and row_idx < ds_idx:
                                continue
                            rd = {
                                "Page": page_index,
                                "Region": None, "Province": None,
                                "City_Muni": None, "Barangay": None,
                                "Summary_Type": None,
                            }
                            for col_idx, cell in enumerate(row):
                                if col_idx == 0:
                                    continue
                                header = (col_headers[col_idx]
                                          if col_headers and col_idx < len(col_headers)
                                          else f"Column_{col_idx}")
                                rd[header] = (cell or "").replace("\n", " ").strip()
                            all_tables_buffer[current_title].append(rd)
                else:
                    # no tables, no OCR needed — skip
                    continue

            # --- Native table extraction (drawn-line tables) ---
            for table_obj in tables_found:
                potential_title = title_tracker.get_title_for_table(
                    page, page_index, table_obj.bbox)
                if potential_title:
                    current_title = clean_tablename(pdf_event, potential_title)

                if current_title not in all_tables_buffer:
                    all_tables_buffer[current_title] = []

                extracted_rows = table_obj.extract()

                if current_title not in table_headers_used:
                    col_headers, ds_idx = detect_and_merge_headers_with_spanning(extracted_rows)
                    table_headers_used[current_title] = (col_headers, ds_idx)
                else:
                    col_headers, ds_idx = table_headers_used[current_title]

                # Check if the native extraction is empty/poor and page is scanned
                native_cells = sum(
                    1 for row in extracted_rows
                    for cell in row if cell and str(cell).strip()
                )
                if page_is_scanned and native_cells < 5:
                    print(f"   ⚙ Page {page_index}: native table sparse ({native_cells} cells), "
                          "using OCR for this table…")
                    layout_rows = _get_ocr().extract_table(
                        page, table_bbox=table_obj.bbox)
                    if layout_rows:
                        extracted_rows = layout_rows
                        if current_title not in table_headers_used or \
                                table_headers_used[current_title][0] is None:
                            col_headers, ds_idx = \
                                detect_and_merge_headers_with_spanning(extracted_rows)
                            table_headers_used[current_title] = (col_headers, ds_idx)

                row_geometries = table_obj.rows
                current_region = current_province = current_muni = current_barangay = None

                for row_idx, (row_obj, row_text) in enumerate(
                        zip(row_geometries, extracted_rows)):
                    if ds_idx and row_idx < ds_idx:
                        continue
                    if not row_obj.cells:
                        continue

                    loc_bbox = row_obj.cells[0]
                    align, casing, text = get_text_alignment_and_case(page, loc_bbox)

                    if text and "REGION" in text and "PROVINCE" in text:
                        continue

                    is_summary = is_summary_row(text)
                    summary_type = None

                    if text and not is_summary:
                        if align == "CENTER" and casing == "UPPER":
                            current_region = text
                            current_province = current_muni = current_barangay = None
                        elif align == "LEFT" and casing == "UPPER":
                            current_province = text
                            current_muni = current_barangay = None
                        elif align == "CENTER" and casing != "UPPER":
                            current_muni = text
                            current_barangay = None
                        elif align == "RIGHT":
                            current_barangay = text
                    elif is_summary:
                        summary_type = text

                    rd = {
                        "Page": page_index,
                        "Region": current_region if not is_summary else None,
                        "Province": current_province if not is_summary else None,
                        "City_Muni": current_muni if not is_summary else None,
                        "Barangay": current_barangay if not is_summary else None,
                        "Summary_Type": summary_type,
                    }
                    for col_idx, cell in enumerate(row_text):
                        if col_idx == 0:
                            continue
                        header = (col_headers[col_idx]
                                  if col_headers and col_idx < len(col_headers)
                                  else f"Column_{col_idx}")
                        rd[header] = (cell or "").replace("\n", " ").strip()

                    all_tables_buffer[current_title].append(rd)

    for title, rows in all_tables_buffer.items():
        if not rows:
            continue
        df = pd.DataFrame(rows)
        csv_path = os.path.join(output_dir, f"{title}.csv")
        df.to_csv(csv_path, index=False)
        print(f"   ✓ Saved table: {csv_path} ({len(rows)} rows)")

    generate_json(pdf_event, output_dir)


# -----------------------------------------------------------------------
# BATCH RUNNERS
# -----------------------------------------------------------------------

def process_all_pdfs(use_ocr: bool = False):
    print("🔎 Scanning folder for PDFs...")
    files = os.listdir(INPUT_FOLDER)
    pdf_files = [f for f in files if f.lower().endswith(".pdf")]
    for idx, filename in enumerate(pdf_files, start=1):
        event = Event(reportName=filename, eventName=clean_filename(filename))
        process_pdf(event, idx, os.path.join(INPUT_FOLDER, filename), use_ocr=use_ocr)
    print(f"\n🎉 Finished parsing all PDFs! {len(pdf_files)}/{len(files)}")


def process_all_pdfs_parallel(use_ocr: bool = False):
    print("🔎 Scanning folder for PDFs...")
    files = os.listdir(INPUT_FOLDER)
    pdf_files = [f for f in files if f.lower().endswith(".pdf")]
    done = 0

    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(
                process_pdf,
                Event(reportName=fn, eventName=clean_filename(fn)),
                idx + 1,
                os.path.join(INPUT_FOLDER, fn),
                use_ocr,
            ): fn
            for idx, fn in enumerate(pdf_files)
        }
        for future in as_completed(futures):
            fn = futures[future]
            try:
                future.result()
                done += 1
                print(f"✓ Finished {fn}")
            except Exception as e:
                print(f"❌ Error processing {fn}: {e}")

    print(f"\n🎉 Finished parsing all PDFs! {done}/{len(pdf_files)}")


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse NDRRMC situational report PDFs with layout-aware OCR")
    parser.add_argument(
        "--ocr", action="store_true",
        help="Enable layout-aware OCR fallback for scanned/image-based PDFs "
             "(uses Tesseract 5 bounding-box reconstruction — no Surya required)")
    parser.add_argument(
        "--sequential", action="store_true",
        help="Process PDFs sequentially instead of in parallel")
    args = parser.parse_args()

    if args.sequential:
        process_all_pdfs(use_ocr=args.ocr)
    else:
        process_all_pdfs_parallel(use_ocr=args.ocr)
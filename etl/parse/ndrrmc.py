import os
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

INPUT_FOLDER = "../data/raw/ndrrmc_new"            # folder that contains PDFs
OUTPUT_FOLDER = "../data/parsed/ndrrmc"   # NEW OUTPUT FOLDER
FOLDER_LENGTH = 0
HEADER_SEARCH_DISTANCE = 80
ALIGNMENT_TOLERANCE = 5

@dataclass
class Event:
    eventName : str = ""
    startDate : str = ""
    endDate : str = ""
    lastUpdateDate : str = ""
    reportName : str = ""
    recordedBy : str = "NDRRMC"
    obtainedDate : str = ""
    reportLink : str = ""
    remarks : str = ""

# -----------------------------------------------------------------------
# IMPROVED: MULTI-PAGE NARRATIVE EXTRACTION
# -----------------------------------------------------------------------

def extract_multi_page_narrative(pdf_path, event_name):
    """Extract narrative text that may span multiple pages."""
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
                page_text = page.extract_text() or ""
                
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
            
            full_narrative = '\n\n'.join(narrative_parts).strip()
            return full_narrative
            
    except Exception as e:
        print(f"Error extracting multi-page narrative: {e}")
        return ""


# -----------------------------------------------------------------------
# DATE EXTRACTION
# -----------------------------------------------------------------------

def extract_dates_from_text(text):
    """Extract all dates from text using various date patterns."""
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
                start_str = match.group(1)
                end_str = match.group(2)
                start_dt = parse_flexible_date(start_str)
                end_dt = parse_flexible_date(end_str)
                if start_dt:
                    found_dates.append(('range_start', start_str, start_dt, match.start()))
                if end_dt:
                    found_dates.append(('range_end', end_str, end_dt, match.start()))
            else:
                date_str = match.group(1) if match.groups() else match.group(0)
                date_obj = parse_flexible_date(date_str)
                if date_obj:
                    found_dates.append(('single', date_str, date_obj, match.start()))
    
    found_dates.sort(key=lambda x: x[3])
    return found_dates


def parse_flexible_date(date_str):
    """Parse a date string using multiple formats."""
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


def extract_narrative_dates(pdf_path, event_name):
    """Extract start and end dates from multi-page narrative."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if len(pdf.pages) == 0:
                return None, None, "", None
            
            first_page = pdf.pages[0]
            first_page_text = first_page.extract_text() or ""
            
            narrative_text = extract_multi_page_narrative(pdf_path, event_name)
            print(f"   → Extracted narrative: {len(narrative_text)} characters across pages")
            
            event_lower = event_name.lower()
            is_sitrep = 'sitrep' in event_lower or 'situational report' in narrative_text.lower()
            is_election = 'election' in event_lower or 'bske' in event_lower.lower()
            is_monitoring = is_sitrep or 'monitoring' in event_lower or 'cases' in event_lower
            
            as_of_date = None
            for page_num in range(min(2, len(pdf.pages))):
                page_text = pdf.pages[page_num].extract_text() or ""
                as_of_pattern = r'as\s+of\s*[(\[]?\s*([^)\]]+?)\s*[)\]]?(?:\s|$)'
                as_of_match = re.search(as_of_pattern, page_text, re.IGNORECASE)
                if as_of_match:
                    as_of_str = as_of_match.group(1).strip()
                    as_of_date = parse_flexible_date(as_of_str)
                    if as_of_date:
                        print(f"   → Found 'as of' date: {as_of_str}")
                        break
            
            all_dates_first_page = extract_dates_from_text(first_page_text[:300])
            report_date = None
            for date_type, date_str, date_obj, position in all_dates_first_page:
                if ':' in date_str and position < 200:
                    report_date = date_obj
                    print(f"   → Found report date: {date_str}")
                    break
            
            all_narrative_dates = extract_dates_from_text(narrative_text)
            print(f"   → Found {len(all_narrative_dates)} dates in narrative")
            
            narrative_dates = []
            for date_type, date_str, date_obj, position in all_narrative_dates:
                if position < 200 and (':' in date_str):
                    continue
                narrative_dates.append((date_type, date_str, date_obj, position))
            
            if is_monitoring and not narrative_dates:
                print(f"   → Detected monitoring report")
                if as_of_date:
                    return as_of_date, as_of_date, narrative_text, report_date
                if report_date:
                    return report_date, report_date, narrative_text, report_date
                return None, None, narrative_text, None
            
            if is_election:
                print(f"   → Detected election event")
                election_patterns = [
                    r'scheduled\s+for\s+(\d{1,2}\s+\w+\s+\d{4})',
                    r'election.*?(\d{1,2}\s+\w+\s+\d{4})',
                    r'(\d{1,2}\s+\w+\s+\d{4}).*?election',
                ]
                for pattern in election_patterns:
                    match = re.search(pattern, narrative_text, re.IGNORECASE)
                    if match:
                        election_date_str = match.group(1)
                        election_date = parse_flexible_date(election_date_str)
                        if election_date:
                            print(f"   → Found election date: {election_date_str}")
                            return election_date, election_date, narrative_text, report_date
                if narrative_dates:
                    election_date = narrative_dates[0][2]
                    return election_date, election_date, narrative_text, report_date
            
            if not narrative_dates:
                print(f"   → No narrative dates found")
                if as_of_date:
                    return as_of_date, as_of_date, narrative_text, report_date
                if report_date:
                    return report_date, report_date, narrative_text, report_date
                return None, None, narrative_text, None
            
            range_starts = [d for d in narrative_dates if d[0] == 'range_start']
            range_ends = [d for d in narrative_dates if d[0] == 'range_end']
            
            if range_starts and range_ends:
                start_date = range_starts[0][2]
                end_date = range_ends[0][2]
                print(f"   → Using explicit date range")
            else:
                all_date_objects = [d[2] for d in narrative_dates]
                all_date_objects.sort()
                start_date = all_date_objects[0]
                end_date = all_date_objects[-1]
                print(f"   → Using chronological range from {len(all_date_objects)} dates")
            
            return start_date, end_date, narrative_text, report_date
            
    except Exception as e:
        print(f"Error extracting dates: {e}")
        import traceback
        traceback.print_exc()
        return None, None, "", None


# -----------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------
def get_lastUpdateDateTime(event:Event, lastUpdateDateTime):
    formats = [
        "%b %d, %Y",
        "%B %d %Y",
        "%B %d, %Y %H:%M",
        "%b %d, %Y %H:%M"
    ]
    parsed_date = None
    for fmt in formats:
        try:
            FormatDate = datetime.strptime(lastUpdateDateTime, fmt)
            event.lastUpdateDate = FormatDate.strftime("%Y-%m-%d %H:%M:%S")
            parsed_date = True
            break
        except ValueError:
            continue
    if not parsed_date:
        event.lastUpdateDate = lastUpdateDateTime


def clean_tablename(event: Event, table_title: str) -> str:
    if not table_title:
        return "Unknown_Section"
    
    if "as of" in table_title.lower():
        title_split = table_title.split("as of")
        table_title = title_split[0]
        table_title = table_title.strip().replace(" ", "_").lower()
        lastUpdateDate = title_split[1].replace("(","").replace(")","").strip()
        get_lastUpdateDateTime(event, lastUpdateDate)
    else:
        table_title = table_title.strip().replace(" ", "_").lower()
    
    return "".join(c for c in table_title if c.isalnum() or c in "._-")


abbrev_map = {
    "Tropical Storm": "TS",
    "Typhoon": "TY",
    "Tropical Cyclone": "TC",
    "Situational Report": "SitRep",
    "Southwest Monsoon": "SWM",
    "Low Pressure Area": "LPA",
    "Terminal Report": "TR",
    "Final Report": "FR"
}

def normalize_subject(text):
    for full, abbr in abbrev_map.items():
        text = re.sub(full, abbr, text, flags=re.IGNORECASE)
    return text

def clean_filename(filename):
    name = filename.replace(".pdf", "")
    name = re.sub(r"(Breakdown|Final_Report|SitRep|Situational_Report|Terminal_Report|Table)", "", name, flags=re.IGNORECASE)
    name = name.replace("_", " ")
    
    match = re.search(r"for (.+)", name, flags=re.IGNORECASE)
    if match:
        subject = match.group(1).strip()
    else:
        subject = name.strip()
    
    subject = re.sub(r"(Breakdown.*)$", "", subject, flags=re.IGNORECASE).strip().replace("the","").replace(" -", "").removeprefix(" ").replace("(","").replace(")","")
    subject = normalize_subject(subject)
    
    return subject

def generate_json(event, output_dir):
    event_dict = asdict(event)
    metadata = {
        "eventName": event.eventName,
        "startDate": event.startDate,
        "endDate": event.endDate,
        "remarks": event.remarks
    }
    source = {k: v for k, v in event_dict.items() if k not in metadata}

    metadata_path = os.path.join(output_dir, "metadata.json")
    source_path = os.path.join(output_dir, "source.json")

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=4)

    with open(source_path, "w") as f:
        json.dump(source, f, indent=4)

    print(f"✓ Saved metadata: {metadata_path}")
    print(f"✓ Saved source: {source_path}")


def get_text_alignment_and_case(page, cell_bbox):
    """Analyze text geometry and casing inside a table cell."""
    if not cell_bbox:
        return None, None, ""

    try:
        cell_crop = page.crop(cell_bbox)
        words = cell_crop.extract_words()
    except ValueError:
        return None, None, ""

    if not words:
        return None, None, ""

    text = " ".join(w["text"] for w in words).strip()

    cell_x0, _, cell_x1, _ = cell_bbox
    text_x0 = min(w["x0"] for w in words)
    text_x1 = max(w["x1"] for w in words)

    left_margin = text_x0 - cell_x0
    right_margin = cell_x1 - text_x1

    alignment = "UNKNOWN"
    if abs(left_margin - right_margin) < ALIGNMENT_TOLERANCE:
        alignment = "CENTER"
    elif left_margin < ALIGNMENT_TOLERANCE * 2:
        alignment = "LEFT"
    elif right_margin < ALIGNMENT_TOLERANCE * 2:
        alignment = "RIGHT"

    case_type = "MIXED"
    if text.isupper():
        case_type = "UPPER"
    elif text.istitle():
        case_type = "TITLE"

    return alignment, case_type, text


def is_summary_row(text):
    """
    Detect if a row is a summary row (GRAND TOTAL, INJURED/ILL, etc.)
    These should go in a separate Summary_Type column, not in Province.
    """
    if not text:
        return False
    
    text_upper = text.upper().strip()
    
    summary_patterns = [
        'GRAND TOTAL',
        'TOTAL',
        'SUB-TOTAL',
        'SUB TOTAL',
        'SUBTOTAL',
        'INJURED/ILL',
        'INJURED',
        'ILL',
        'DEAD',
        'MISSING',
        'OVERALL',
        'SUMMARY',
        'CASUALTIES',
    ]
    
    return any(pattern in text_upper for pattern in summary_patterns)


# -----------------------------------------------------------------------
# FIXED: ROBUST HEADER DETECTION WITH EXPANDED SAFETY BREAK
# -----------------------------------------------------------------------

def detect_and_merge_headers_with_spanning(extracted_rows):
    """
    ROBUST VERSION: Detects header rows and merges them vertically.
    Includes SAFETY STOP for 'REGION X', 'GRAND TOTAL', and CASUALTY HEADERS
    (DEAD/INJURED/MISSING) to prevent them from being swallowed as headers.
    """
    if not extracted_rows or len(extracted_rows) == 0:
        return None, 0
    
    # Check first 8 rows to find where data starts
    header_keywords = [
        'families', 'persons', 'barangay', 'brgys', 'cum', 'now',
        'type', 'date', 'time', 'description', 'actions', 'remarks',
        'affected', 'evacuation', 'centers', 'inside', 'outside',
        'status', 'region', 'province', 'municipality', 'city',
        'total', 'served', 'current', 'incident', 'occurrence'
    ]
    
    last_header_idx = -1
    for idx in range(min(8, len(extracted_rows))):
        row = extracted_rows[idx]
        if not row:
            continue

        # --- SAFETY CHECK: If row looks like data start, STOP ---
        first_cell = str(row[0]).strip() if row[0] else ""
        
        # 1. Check for REGION or GRAND TOTAL
        if re.match(r'^REGION\s+(\d+|[IVX]+)', first_cell, re.IGNORECASE) or \
           re.match(r'^GRAND\s+TOTAL', first_cell, re.IGNORECASE):
            break
            
        # 2. Check for CASUALTY SECTION HEADERS (Dead, Injured, Missing)
        # Only if they are at the START of the row (likely a section divider)
        if re.match(r'^(DEAD|INJURED|MISSING|CASUALTIES)', first_cell, re.IGNORECASE):
            break
        # --------------------------------------------------------
        
        header_like_count = 0
        data_like_count = 0
        
        for cell in row:
            if cell is None or str(cell).strip() == '':
                continue
            cell_str = str(cell).lower().strip()
            
            if any(keyword in cell_str for keyword in header_keywords):
                header_like_count += 1
            elif cell_str.replace(',', '').replace('.', '').isdigit():
                data_like_count += 1
            elif len(cell_str) > 50:
                data_like_count += 1
        
        if data_like_count > header_like_count and data_like_count > 0:
            break
        
        last_header_idx = idx
    
    if last_header_idx < 0:
        return None, 0
    
    data_start_idx = last_header_idx + 1
    
    # --- Forward Fill and Vertical Merge ---
    
    header_rows = extracted_rows[:data_start_idx]
    num_columns = len(extracted_rows[0])
    
    filled_header_matrix = []
    
    for row in header_rows:
        new_row = []
        last_valid_val = None
        
        for cell in row:
            cell_val = str(cell).strip() if cell else ""
            
            if cell_val:
                last_valid_val = cell_val
                new_row.append(cell_val)
            else:
                new_row.append(last_valid_val if last_valid_val else "")
                
        filled_header_matrix.append(new_row)
    
    merged_headers = []
    
    for col_idx in range(num_columns):
        parts = []
        for row_idx in range(len(filled_header_matrix)):
            val = filled_header_matrix[row_idx][col_idx]
            if val and val not in parts: 
                parts.append(val)
        
        if not parts:
            merged_headers.append(f"Column_{col_idx}")
        else:
            raw_name = "_".join(parts)
            clean_name = raw_name.replace(' ', '_').replace('/', '_').replace('(', '').replace(')', '').replace('.', '')
            clean_name = re.sub(r'_+', '_', clean_name)
            merged_headers.append(clean_name)

    print(f"   → Detected {data_start_idx} header rows (Fixed Logic)")
    
    return merged_headers, data_start_idx


# -----------------------------------------------------------------------
# Cross-page title tracking
# -----------------------------------------------------------------------
class SimpleTitleTracker:
    def __init__(self):
        self.pending_title = None
        self.pending_title_page = None
    
    def check_page_bottom(self, page, page_num):
        """Look for potential title at bottom of page. IMPROVED to handle titles even when tables are present."""
        page_height = page.height
        bottom_y = page_height - 150  # INCREASED from 100 to 150 pixels to catch more titles
        
        # Extract ALL text from bottom section first (regardless of tables)
        bottom_crop = page.crop((0, bottom_y, page.width, page_height))
        bottom_text = bottom_crop.extract_text() or ""
        
        lines = [l.strip() for l in bottom_text.split("\n") if l.strip()]
        
        # Look for title-like lines (check BEFORE tables check)
        potential_title = None
        for line in reversed(lines):
            if len(line) < 5 or re.match(r'^\d+$', line) or 'Page' in line:
                continue
            
            # CRITICAL: Check for table section titles (AFFECTED POPULATION, CASUALTIES, etc.)
            if line.isupper() or 'as of' in line.lower() or 'AFFECTED POPULATION' in line.upper():
                potential_title = line
                break
        
        # If we found a potential title, verify it's not inside a table
        if potential_title:
            # Check if there's a table that would contain this title
            tables = page.find_tables({
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "snap_tolerance": 5,
            })
            
            # Only skip if the title is actually INSIDE a table cell
            title_y_position = page_height - 50  # Approximate position of title at bottom
            is_inside_table = False
            
            for table in tables:
                table_top = table.bbox[1]
                table_bottom = table.bbox[3]
                # If title is inside table boundaries, skip it
                if table_top < title_y_position < table_bottom:
                    is_inside_table = True
                    break
            
            if not is_inside_table:
                self.pending_title = potential_title
                self.pending_title_page = page_num
                print(f"   → Found potential title at bottom of page {page_num}: {potential_title}")
                return potential_title
        
        return None
    
    def get_title_for_table(self, page, page_num, table_bbox):
        """Get title for a table."""
        if self.pending_title and self.pending_title_page == page_num - 1:
            table_top = table_bbox[1]
            if table_top < page.height * 0.3:
                title = self.pending_title
                print(f"   → Using title from previous page: {title}")
                self.pending_title = None
                return title
        
        x0, top, x1, bottom = table_bbox
        try:
            header_text = page.crop(
                (0, max(0, top - HEADER_SEARCH_DISTANCE), page.width, top)
            ).extract_text() or ""
            
            lines = [l.strip() for l in header_text.split("\n") if l.strip()]
            if lines:
                potential_title = lines[-1]
                if potential_title.isupper() or len(potential_title) < 100:
                    return potential_title
        except Exception:
            pass
        
        return None


# -----------------------------------------------------------------------
# MAIN PROCESSOR
# -----------------------------------------------------------------------
def process_pdf(pdf_event = Event, file_counter = int, pdf_path = str):
    print(f"\n📄{file_counter} Processing PDF: {pdf_path}")

    start_date, end_date, narrative_text, report_date = extract_narrative_dates(pdf_path, pdf_event.eventName)
    
    if narrative_text:
        pdf_event.remarks = narrative_text
        print(f"   → Extracted narrative ({len(narrative_text)} characters)")
    
    if start_date:
        pdf_event.startDate = start_date.strftime("%Y-%m-%d")
        print(f"   → Start Date: {pdf_event.startDate}")
    
    if end_date:
        pdf_event.endDate = end_date.strftime("%Y-%m-%d")
        print(f"   → End Date: {pdf_event.endDate}")

    pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_dir = os.path.join(OUTPUT_FOLDER, pdf_event.eventName)
    os.makedirs(output_dir, exist_ok=True)

    current_title = "Unknown_Section"
    all_tables_buffer = {}
    table_headers_used = {}

    title_tracker = SimpleTitleTracker()

    with pdfplumber.open(pdf_path) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            
            title_tracker.check_page_bottom(page, page_index)

            tables_found = page.find_tables({
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "snap_tolerance": 5,
            })

            if not tables_found:
                continue

            for table_obj in tables_found:
                potential_title = title_tracker.get_title_for_table(page, page_index, table_obj.bbox)
                
                if potential_title:
                    try:
                        new_title = clean_tablename(pdf_event, potential_title)
                        current_title = new_title
                    except Exception:
                        pass

                if current_title not in all_tables_buffer:
                    all_tables_buffer[current_title] = []

                extracted_rows = table_obj.extract()
                row_geometries = table_obj.rows

                if current_title not in table_headers_used:
                    column_headers, data_start_idx = detect_and_merge_headers_with_spanning(extracted_rows)
                    table_headers_used[current_title] = (column_headers, data_start_idx)
                else:
                    column_headers, data_start_idx = table_headers_used[current_title]

                current_region = None
                current_province = None
                current_muni = None
                current_barangay = None

                for row_idx, (row_obj, row_text) in enumerate(zip(row_geometries, extracted_rows)):
                    if data_start_idx > 0 and row_idx < data_start_idx:
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
                            current_province = None
                            current_muni = None
                            current_barangay = None
                        elif align == "LEFT" and casing == "UPPER":
                            current_province = text
                            current_muni = None
                            current_barangay = None
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
                        
                        if column_headers and col_idx < len(column_headers):
                            header_name = column_headers[col_idx]
                        else:
                            header_name = f"Column_{col_idx}"
                        
                        rd[header_name] = (cell or "").replace("\n", " ").strip()

                    all_tables_buffer[current_title].append(rd)

    for title, rows in all_tables_buffer.items():
        if not rows:
            continue

        df = pd.DataFrame(rows)
        # No sorting to preserve PDF order

        csv_path = os.path.join(output_dir, f"{title}.csv")
        df.to_csv(csv_path, index=False)

        print(f"   ✓ Saved table: {csv_path} ({len(rows)} rows)")
    
    generate_json(pdf_event, output_dir)


def process_all_pdfs():
    print("🔎 Scanning folder for PDFs...")

    FILES =  os.listdir(INPUT_FOLDER)
    FOLDER_LENGTH = len(FILES)
    file_counter = 0
    for filename in FILES:
        if filename.lower().endswith(".pdf"):
            file_counter += 1
            fullpath = os.path.join(INPUT_FOLDER, filename)
            event = Event(reportName = filename, eventName = clean_filename(filename))
            process_pdf(event, file_counter, fullpath)

    print(f"\n🎉 Finished parsing all PDFs ! {file_counter}/{FOLDER_LENGTH}")

def process_all_pdfs_parallel():
    print("🔎 Scanning folder for PDFs...")

    FILES = os.listdir(INPUT_FOLDER)
    pdf_files = [f for f in FILES if f.lower().endswith(".pdf")]
    FOLDER_LENGTH = len(pdf_files)
    file_counter = 0
    
    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(
                process_pdf, 
                Event(reportName=filename, eventName=clean_filename(filename)), 
                idx+1, 
                os.path.join(INPUT_FOLDER, filename)
            ): filename
            for idx, filename in enumerate(pdf_files)
        }

        for future in as_completed(futures):
            filename = futures[future]
            try:
                future.result()
                file_counter += 1
                print(f"✓ Finished {filename}")
            except Exception as e:
                print(f"❌ Error processing {filename}: {e}")

    print(f"\n🎉 Finished parsing all PDFs ! {file_counter}/{FOLDER_LENGTH}")

if __name__ == "__main__":
    process_all_pdfs_parallel()
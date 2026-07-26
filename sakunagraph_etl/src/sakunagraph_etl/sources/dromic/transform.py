

from typing import Iterable, List, Optional, Tuple

from rdflib import URIRef
from sakunagraph_etl.transform.helpers import load_csv_df, to_int, to_million_php
from .rdf import AFF_POP_TOKENS, ASSISTANCE_TOKENS, HOUSING_TOKENS, ORG_MAPPING, AffectedPopulation, Assistance, Event, Housing, PEvac, Provenance, ReportVersion
from .identity import canonical_event_id, legacy_event_id
import os
import json
from datetime import datetime
import re
import polars as pl
from sakunagraph_etl.transform.impact import impact_entities as _impact_entities

def _fix_docling_numeric(val: str) -> str:
    """
    Fix Docling's numeric parsing artifacts where digits leak into
    separators, producing patterns like:
      '17, 7, 785'     → '17785'     (then reformat as needed)
      '1, 1, 562, 599' → '1562599'
      '28. 8. 23'      → '28.23'
    
    Strategy: detect the artifact pattern and reconstruct the number.
    """
    if not val or not isinstance(val, str):
        return val

    val = val.strip()

    # Pattern: digit groups separated by ", digit, " where the middle digit
    # is a duplicate of the leading digit of the next group
    # e.g. "1, 1, 562, 2, 599" → remove the spurious single-digit tokens

    # Step 1: fix decimal artifact "28. 8. 23" → "28.23"
    # Pattern: number DOT spurious_digit DOT real_decimal
    val = re.sub(
        r'(\d+)\.\s*\d+\.\s*(\d+)',
        lambda m: f"{m.group(1)}.{m.group(2)}",
        val
    )

    # Step 2: fix thousands separator artifact "1, 1, 562, 2, 599"
    # Remove single-digit tokens that appear between multi-digit groups
    # i.e. "X, D, YYY" where D is a single digit → "X, YYY"
    val = re.sub(r',\s*\d(?=\s*,)', '', val)

    val = re.sub(r'^\s*[.,]+\s*', '', val)  # strip leading dots/commas
    val = re.sub(r'\s*[.,]+\s*$', '', val)       # strip trailing dots/commas

    # Step 3: remove remaining commas/spaces to get clean number
    # then reformat if needed — but since we're storing as string, just clean
    val = re.sub(r',\s*', '', val).strip()

    return val

def _parse_date(value: str) -> Optional[datetime]:
    """Parse a date string in various formats, return None if unparseable."""
    if not value or not value.strip():
        return None
    value = value.strip()
    formats = [
        "%Y-%m-%d",           # 2020-09-29
        "%d %B %Y",           # 29 September 2020
        "%d %b %Y",           # 29 Sep 2020
        "%B %d, %Y",          # September 29, 2020
        "%b %d, %Y",          # Sep 29, 2020
        "%d-%m-%Y",           # 29-09-2020
        "%m/%d/%Y",           # 09/29/2020
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    print(f"  [WARN] Could not parse date: {value!r}")
    return None

def _event_id(
    event_name: str,
    start_date: str | None,
    report_link: str | None = None,
) -> str:
    """Return the canonical post-based ID with a legacy metadata fallback."""

    return canonical_event_id(
        report_link,
        event_name=event_name,
        start_date=start_date,
    )

def _extract_barangay(text: str) -> tuple[str | None, str]:
    """
    Extracts the barangay name and the remaining string from strings like:
      "Brgy. Proper, Calamba, Laguna"
      "Barangay Holy Spirit, Quezon City"
      "Bgy. Commonwealth, QC"
    
    Returns (barangay_name, remaining) e.g. ("Proper", "Calamba, Laguna"),
    or None if not found.
    """
    pattern = r'(?:Barangay|Brgy\.?|Bgy\.?)\s+([^,]+),?\s*(.*)'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return (None, text)


def load_event(file_path: str) -> Event:
    from sakunagraph_etl.enrichment.disaster_types import DISASTER_CLASSIFIER
    from sakunagraph_etl.enrichment.locations import LOCATION_MATCHER

    with open(file_path, "r", encoding="utf-8") as f:
        meta: dict[str, str] = json.load(f)

    source_path = Path(file_path).with_name("source.json")
    source: dict[str, str] = {}
    if source_path.is_file():
        with source_path.open("r", encoding="utf-8") as file:
            source = json.load(file)

    event_name = meta.get("eventName", "") 
    remarks = meta.get("remarks", "") 
    pred, _ = DISASTER_CLASSIFIER.classify(
        [event_name + remarks]
    )[0]

    location = meta.get("location", None)


    # if location is explicit add, if not, tag through impact (outside)
    hasLocation = ""
    hasBarangay = ""
    if location and location != "" and "," in location:
        (hasBarangay, location) = _extract_barangay(location)
        hasLocation = "|".join(LOCATION_MATCHER.match_cell(location))

    # if not meta["startDate"]:
    #     print('Missing dates in metadata.json: ', event_name)

    legacy_id = legacy_event_id(event_name, meta.get("startDate"))
    event_id = _event_id(
        event_name,
        meta.get("startDate"),
        source.get("reportLink"),
    )
    event = Event(
        id=event_id,
        eventName=event_name,
        startDate=_parse_date(meta.get("startDate", "")),
        endDate=_parse_date(meta.get("endDate", "")),
        remarks=remarks,
        hasDisasterType=pred,
        hasBarangay=hasBarangay if hasBarangay else None,
        hasLocation=URIRef(str(hasLocation)) if hasLocation else None,
        legacy_id=legacy_id if event_id != legacy_id else None,
    )

    return event

def load_provenance(file_path: str) -> Provenance:

    with open(file_path, "r", encoding="utf-8") as f:
        src: dict[str, object] = json.load(f)

    # if not src["lastUpdateDate"]: print("Missing last update date on file: ", src["reportName"])

    raw_versions = src.get("reportVersions", [])
    versions = tuple(
        ReportVersion(
            reportName=str(entry.get("filename") or ""),
            reportLink=str(entry.get("post_url") or "") or None,
            downloadUrl=str(entry.get("download_url") or "") or None,
            obtainedDate=str(entry.get("downloaded_at") or "") or None,
            postDate=str(entry.get("post_date") or "") or None,
            sha256=str(entry.get("sha256") or "") or None,
        )
        for entry in raw_versions
        if isinstance(entry, dict) and entry.get("filename")
    ) if isinstance(raw_versions, list) else ()

    return Provenance(
        lastUpdateDate=_parse_date(str(src.get("lastUpdateDate") or "")),
        reportLink=str(src.get("reportLink") or "") or None,
        reportName=str(src.get("reportName") or ""),
        obtainedDate=str(src.get("obtainedDate") or "") or None,
        downloadUrl=str(src.get("downloadUrl") or "") or None,
        postDate=str(src.get("postDate") or "") or None,
        sha256=str(src.get("sha256") or "") or None,
        versions=versions,
    )

def load_aff_pop(folder_path: str) -> Tuple[List[AffectedPopulation] | None, List[PEvac] | None]:

    src_paths: List[str] = []

    files = os.listdir(folder_path)

    # Check if a total displaced/served file exists anywhere
    has_total_displaced = any(
        "total" in f and any(k in f for k in ("displaced", "served")) and f.endswith(".csv")
        for f in files
    )

    for file in files:
        if "affected" in file and "number" in file and file.endswith(".csv"):
            src_paths.append(os.path.join(folder_path, file))
            continue

        if "total" in file and any(k in file for k in ("displaced", "served")) and file.endswith(".csv"):
            src_paths.append(os.path.join(folder_path, file))
        elif "displaced" in file and not has_total_displaced:
            src_paths.append(os.path.join(folder_path, file))

    
    if len(src_paths) == 0: return (None, None)

    
    dfs: Iterable[pl.DataFrame] = []
    index = 1
    for src_path in src_paths:
        # print("Loading aff pop for: ", src_path)
        df = load_csv_df(
            src_path,
            mapping_tokens=AFF_POP_TOKENS,
            target_cols=["region", "province"],
            collapse_key="municipality",
            match_location=True,
            correct_qty_barangay=False
        )

        df = to_int(df, ["affectedFamilies", "affectedPersons", "affectedBarangays", "displacedFamilies", "displacedPersons", "displacedFamiliesI", "displacedPersonsI", "displacedFamiliesO", "displacedPersonsO", "evacuationCenters"])

        dfs.append(df)
        index += 1

    # combine all dfs on hasLocation
    try:
        combined = dfs[0]
        for df in dfs[1:]:
            shared_cols = [c for c in df.columns if c in combined.columns and c != "hasLocation"]
            combined = combined.join(df.drop(shared_cols), on="hasLocation", how="full", coalesce=True)
    except pl.exceptions.SchemaError:
        return (None, None)
    

    # resolve O + I into displacedFamilies / displacedPersons only if no total displaced file / columns
    if not has_total_displaced:
        fam_cols = [c for c in ["displacedFamiliesO", "displacedFamiliesI"] if c in combined.columns]
        per_cols = [c for c in ["displacedPersonsO", "displacedPersonsI"] if c in combined.columns]
        drop_cols = fam_cols + per_cols

        exprs: List[pl.Expr] = []
        if fam_cols:
            exprs.append(pl.sum_horizontal([pl.col(c).fill_null(0) for c in fam_cols]).alias("displacedFamilies"))
        if per_cols:
            exprs.append(pl.sum_horizontal([pl.col(c).fill_null(0) for c in per_cols]).alias("displacedPersons"))
        
        if exprs:
            combined = combined.with_columns(exprs).drop(drop_cols)
    
    if len(combined) > 1: 
        combined = combined.with_row_index("id", 1)

    return (
        _impact_entities(combined, AffectedPopulation),
        _impact_entities(combined, PEvac),
    )

def load_housing(folder_path: str) -> List[Housing] | None:

    src_path = next(
        (
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if "house" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path:
        return None
    
    # print("Loading housing for: ", src_path)

    df = load_csv_df(
        src_path,
        mapping_tokens=HOUSING_TOKENS,
        target_cols=["region", "province"],
        collapse_key="municipality",
        match_location=True,
        correct_qty_barangay=False,
    )

    df = to_int(df, ["totallyDamagedHouses", "partiallyDamagedHouses"])

    if len(df) > 1:
        df = df.with_row_index("id", 1)

    return _impact_entities(df, Housing)


def load_assistance(
    folder_path: str,
    *,
    debug_dir: str | os.PathLike[str] | None = None,
) -> List[Assistance] | None:

    src_path = next(
        (
            os.path.join(folder_path, f)
            for f in os.listdir(folder_path)
            if "assistance" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path:
        return None
    

    # print("Loading assistance for: ", src_path)

    df = load_csv_df(
        src_path,
        target_cols=["region", "province"],
        mapping_tokens=ASSISTANCE_TOKENS,
        collapse_key="municipality",
        match_location=True,
        correct_qty_barangay=False,
        split_assistance=True,
    )

    cost_cols = [c for c in df.columns if any(
    kw in c.lower() for kw in ["dswd", "lgu", "ngo", "others", "nga"]
    )]

    df = df.with_columns([
        pl.col(c).cast(pl.Utf8).map_elements(_fix_docling_numeric, return_dtype=pl.Utf8)
        for c in cost_cols
    ])

    if debug_dir is not None:
        debug_path = os.path.join(
            os.fspath(debug_dir),
            "dromic",
            os.path.basename(os.path.normpath(folder_path)),
            "assistance.csv",
        )
        os.makedirs(os.path.dirname(debug_path), exist_ok=True)
        df.write_csv(debug_path)

    df = to_million_php(df, ["dswd", "lgu", "others", "ngo", "nga"])

    df = df.rename(mapping=ORG_MAPPING, strict=False)

    # pivot: hasLocation, contributingOrg, contributionAmount

    existing_org_cols = [c for c in ORG_MAPPING.values() if c in df.columns]

    df = df.unpivot(
        on=existing_org_cols,
        index="hasLocation",
        variable_name="contributingOrg",
        value_name="contributionAmount",
    ).filter(
            pl.col("contributionAmount").is_not_null()
            & (pl.col("contributionAmount") != 0)
        )


    if len(df) > 1:
        df = df.with_row_index("id", 1)


    return _impact_entities(df, Assistance)

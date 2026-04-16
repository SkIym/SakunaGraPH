

from typing import Iterable, List

from rdflib import URIRef
from transform.helpers import df_to_entities, normalize_columns, to_int, load_csv_df, to_million_php
from semantic_processing.location_matcher_v2 import LOCATION_MATCHER
from mappings.iris import DROMIC_EVENT_NS
from mappings.dromic import AFF_POP_COL_MAP, ASSISTANCE_TOKENS, HOUSES_MAPPING, AffectedPopulation, Assistance, Event, Housing, Provenance
import os
import json
from semantic_processing.disaster_classifier import DISASTER_CLASSIFIER
import uuid
from datetime import datetime
import re
import polars as pl

def _event_id(event_name: str, start_date: str | None) -> str:
    """Deterministic hex ID from event name + start date."""
    key = f"{event_name.strip().lower()}:{start_date or ''}"
    return uuid.uuid5(DROMIC_EVENT_NS, key).hex

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

    with open(file_path, "r", encoding="utf-8") as f:
        meta: dict[str, str] = json.load(f)

    event_name = meta.get("eventName", "") 
    remarks = meta.get("remarks", "") 
    pred, _ = DISASTER_CLASSIFIER.classify(
        [event_name + remarks]
    )[0]

    location = meta.get("location", None)


    # if location is explicit add, if not, tag through impact (outside)
    hasLocation = ""
    hasBarangay = ""
    if location and location != "":
        (hasBarangay, location) = _extract_barangay(location)
        hasLocation = "|".join(LOCATION_MATCHER.match_cell(location))

    # if not meta["startDate"]:
    #     print('Missing dates in metadata.json: ', event_name)

    event = Event(
        id=_event_id(event_name, meta.get("startDate")),
        eventName=event_name,
        startDate=datetime.fromisoformat(meta["startDate"]) if meta["startDate"] else None,
        endDate=datetime.fromisoformat(meta["endDate"]) if meta["endDate"] else None,
        remarks=remarks,
        hasDisasterType=pred,
        hasBarangay=hasBarangay if hasBarangay else None,
        hasLocation=URIRef(str(hasLocation)) if hasLocation else None
    )

    return event

def load_provenance(file_path: str) -> Provenance:

    with open(file_path, "r", encoding="utf-8") as f:
        src: dict[str, str] = json.load(f)

    # if not src["lastUpdateDate"]: print("Missing last update date on file: ", src["reportName"])
    
    return Provenance(
        lastUpdateDate=datetime.fromisoformat(src['lastUpdateDate']) if src['lastUpdateDate'] else None,
        reportLink=src.get("reportLink"),
        reportName=src.get("reportName", ""),
        obtainedDate=src.get("obtainedDate"),
    )

def load_aff_pop(folder_path: str) -> List[AffectedPopulation] | None:

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

    
    if len(src_paths) == 0: return None

    
    dfs: Iterable[pl.DataFrame] = []
    index = 1
    for src_path in src_paths:
        print("Loading aff pop for: ", src_path)
        df = load_csv_df(
            src_path,
            mapping=AFF_POP_COL_MAP,
            target_cols=["Region", "Province"],
            collapse_on="Summary_Type",
            collapse_key="City_Muni",
            match_location=True,
            correct_QTY_Barangay=False
        )

        df = to_int(df, ["affectedFamilies", "affectedPersons", "affectedBarangays", "displacedFamilies", "displacedPersons", "displacedFamiliesI", "displacedPersonsI", "displacedFamiliesO", "displacedPersonsO"])

        dfs.append(df)
        index += 1

    # combine all dfs on hasLocation
    try:
        combined = dfs[0]
        for df in dfs[1:]:
            shared_cols = [c for c in df.columns if c in combined.columns and c != "hasLocation"]
            combined = combined.join(df.drop(shared_cols), on="hasLocation", how="full", coalesce=True)
    except pl.exceptions.SchemaError:
        return None

    # resolve O + I into displacedFamilies / displacedPersons only if no total displaced file
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

    combined.write_csv(f"./dump/combined.csv")

    return df_to_entities(combined, AffectedPopulation)

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
    
    df = load_csv_df(
        src_path,
        mapping=HOUSES_MAPPING,
        target_cols=["Region", "Province"],
        collapse_on="Summary_Type",
        collapse_key="City_Muni",
        match_location=True,
        correct_QTY_Barangay=False,
    )

    df = to_int(df, ["totallyDamagedHouses", "partiallyDamagedHouses"])

    if len(df) > 1:
        df = df.with_row_index("id", 1)

    return df_to_entities(df, Housing)

def load_assistance(folder_path: str) -> List[Assistance] | None:

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
    


    df = load_csv_df(
        src_path,
        target_cols=["Region", "Province"],
        collapse_on="Summary_Type",
        collapse_key="City_Muni",
        match_location=True,
        correct_QTY_Barangay=False,
    )

    df = normalize_columns(df, ASSISTANCE_TOKENS)

    df = to_million_php(df, ["dswd", "lgu", "others", "ngo"])

    df = df.rename({
        "dswd": "https://sakuna.ph/org/DSWD",
        "lgu": "https://sakuna.ph/org/LGU",
        "ngo": "https://sakuna.ph/org/NGO",
        "others": "https://sakuna.ph/org/Unspecified",
    })

    # pivot: hasLocation, contributingOrg, contributionAmount

    df = df.unpivot(
        on=["https://sakuna.ph/org/DSWD", "https://sakuna.ph/org/LGU", "https://sakuna.ph/org/Unspecified", "https://sakuna.ph/org/NGO"],
        index="hasLocation",
        variable_name="contributingOrg",
        value_name="contributionAmount",
    ).filter(
        pl.col("contributionAmount").is_not_null()
        & (pl.col("contributionAmount") != 0)
    )


    if len(df) > 1:
        df = df.with_row_index("id", 1)

    df.write_csv(f"dump/assistance.csv")

    return df_to_entities(df, Assistance)

if __name__ == "__main__":
    load_assistance("../data/parsed/dromic/2022/Mw 7.0 Earthquake Incident in Tayum Abra 2023")
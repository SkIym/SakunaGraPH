import argparse
import math
import re
import uuid
from pathlib import Path
import numpy as np
import pandas as pd
from dateutil.parser import parse
from mappings.iris import GDA_NS
from semantic_processing.location_matcher_v2 import LOCATION_MATCHER

from mappings.gda_mapping import (
    Assistance, AffectedPopulation, Casualties, CommunicationLineDisruption,
    DeclarationOfCalamity, DamageGeneral, Evacuation, Event, HousingDamage,
    Incident, InfrastructureDamage, PowerDisruption, Preparedness, Recovery, Relief,
    Rescue, RoadAndBridgesDamage, SeaportDisruption,
    WaterDisruption
)
from typing import Any
pd.set_option("future.no_silent_downcasting", True)
from dataclasses import fields

COLUMN_MAPPING = {
    "M or I": "eventClass",
    "Main Event Disaster Type": "hasType",
    "Disaster Name": "eventName",
    "Date/Period": "date",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Main Area/s Affected / Location": "hasLocation",
    "Additional Perils/Disaster Sub-Type Occurences (Compound Disaster, e.g. Typhoon Haiyan = rain + wind + storm surge)": "hasSubtype",
    "PREPAREDNESS_Announcements_Local Agencies / Government Units Concerned": "agencyLGUsPresentPreparedness",
    "PREPAREDNESS_Announcements_Warnings Released / Status Alert or Alert/ State of Calamity": "declarationOfCalamity",
    "PREPAREDNESS_Evacuation_LGU Evacuation Plan": "evacuationPlan",
    "PREPAREDNESS_Evacuation_No. of Evacuation Centers": "evacuationCenters",
    "PREPAREDNESS_Rescue_Rescue Operating Unit/Team": "rescueUnit",
    "PREPAREDNESS_Rescue_Available Rescue Equipment": "rescueEquipment",
    "IMPACT_Number of Affected Areas_Barangays": "affectedBarangays",
    "IMPACT_Casualties_Dead_Total": "dead",
    "IMPACT_Casualties_Injured_Total": "injured",
    "IMPACT_Casualties_Missing_Total": "missing",
    "IMPACT_Affected_Families": "affectedFamilies",
    "IMPACT_Affected_Persons": "affectedPersons",
    "IMPACT_Evacuated_Families": "displacedFamilies",
    "IMPACT_Evacuated_Persons": "displacedPersons",
    "IMPACT_Damages to Properties_Houses_Fully": "totallyDamagedHouses",
    "IMPACT_Damages to Properties_Houses_Partially": "partiallyDamagedHouses",
    "IMPACT_Damages to Properties_Infrastructure (in Millions)": "infraDamageAmount",
    "IMPACT_Damages to Properties_Agriculture (in Millions)": "agricultureDamageAmount",
    "IMPACT_Damages to Properties_Private/Commercial (in Millions)": "commercialDamageAmount",
    "IMPACT_Damages to Properties_Social (in Millions)": "socialDamageAmount",
    "IMPACT_Damages to Properties_Cross sectoral (in Millions)": "crossSectoralDamageAmount",
    "IMPACT_Damages to Properties_Total cost (in Millions)": "generalDamageAmount",
    "IMPACT_Status of Lifelines_Electricity or Power Supply": "powerAffected",
    "IMPACT_Status of Lifelines_Communication Lines": "communicationAffected",
    "IMPACT_Status of Lifelines_Transportation_Roads and Bridges": "roadBridgeAffected",
    "IMPACT_Status of Lifelines_Transportation_Seaports": "seaportsAffected",
    "IMPACT_Status of Lifelines_Transportation_Airports": "airportsAffected",
    "IMPACT_Status of Lifelines_Water_Dams and other Reservoirs": "areDamsAffected",
    "IMPACT_Status of Lifelines_Water_Tap": "isTapAffected",
    "RESPONSE AND RECOVERY_Allocated Funds for the Affected Area/s": "allocatedFunds",
    "RESPONSE AND RECOVERY_NGO-LGU Support Units Present": "agencyLGUsPresentAssistance",
    "RESPONSE AND RECOVERY_International Organizations Present": "internationalOrgsPresent",
    "RESPONSE AND RECOVERY_Amount of Donation from International Organizations (including local NGOs)": "amountNGOs",
    "RESPONSE AND RECOVERY_Supply of Relief Goods_Canned Goods, Rice, etc._Cost": "itemCostGoods",
    "RESPONSE AND RECOVERY_Supply of Relief Goods_Canned Goods, Rice, etc._Quantity": "itemQtyGoods",
    "RESPONSE AND RECOVERY_Supply of Relief Goods_Water_Cost": "itemCostWater",
    "RESPONSE AND RECOVERY_Supply of Relief Goods_Water_Quantity": "itemQtyWater",
    "RESPONSE AND RECOVERY_Supply of Relief Goods_Clothing_Cost": "itemCostClothing",
    "RESPONSE AND RECOVERY_Supply of Relief Goods_Clothing_Quantity": "itemQtyClothing",
    "RESPONSE AND RECOVERY_Supply of Relief Goods_Medicine_Cost": "itemCostMedicine",
    "RESPONSE AND RECOVERY_Supply of Relief Goods_Medicine_Quantity": "itemQtyMedicine",
    "RESPONSE AND RECOVERY_Supply of Relief Goods_Items Not Specified (Cost)": "itemCostOthers1",
    "RESPONSE AND RECOVERY_Supply of Relief Goods_Total Cost": "itemCostOthers2",
    "RESPONSE AND RECOVERY_Search, Rescue and Retrieval": "srrDone",
    "RESPONSE AND RECOVERY_City-Municipal Policy Changes": "policyChanges",
    "RESPONSE AND RECOVERY_Cost of Structure Built post-disaster": "postStructureCost",
    "RESPONSE AND RECOVERY_Post-Disaster Training": "postTraining",
    "REFERENCES (Authors. Year. Title. Journal/Book/Newspaper. Publisher, Place published. Pages. Website, Date Accessed)": "reference",
    "Detailed Description of Disaster Event": "otherDescription",
    "Comments/Notes": "remarks",
}

EXPORT_SPECS: dict[str, Any] = {
    "gda_prep.csv": {
        "cols": ["id", "agencyLGUsPresentPreparedness", "declarationOfCalamity"],
        "dropna": {
            "subset": ["agencyLGUsPresentPreparedness", "declarationOfCalamity"],
            "how": "all",
        },
        "contains": {
            "column": "declarationOfCalamity",
            "pattern": "Calamity",
            "case": False,
            "exclude": True,
        },
        "rename": {"declarationOfCalamity": "announcementsReleased"},
    },
    "gda_evac.csv": {
        "cols": ["id", "evacuationPlan", "evacuationCenters"],
        "dropna": {"subset": ["evacuationPlan", "evacuationCenters"], "how": "all"},
        "astype": {"evacuationCenters": "Int64"}
    },
    "gda_rescue.csv": {
        "cols": ["id", "rescueEquipment", "rescueUnit"],
        "dropna": {"subset": ["rescueEquipment", "rescueUnit"], "how": "all"},
    },
    "gda_calamity.csv": {
        "cols": ["id", "declarationOfCalamity"],
        "dropna": {"subset": ["declarationOfCalamity"], "how": "any"},
        "contains": {
            "column": "declarationOfCalamity",
            "pattern": "Calamity",
            "case": False,
            "exclude": False,
        },
    },
    "gda_aff_pop.csv": {
        "cols": [
            "id",
            "affectedBarangays",
            "affectedFamilies",
            "affectedPersons",
            "displacedFamilies",
            "displacedPersons",
        ],
        "dropna": {
            "subset": [
                "affectedBarangays",
                "affectedFamilies",
                "affectedPersons",
                "displacedFamilies",
                "displacedPersons",
            ],
            "how": "all",
        },
        "float_format": "%.0f",
    },
    "gda_casualties.csv": {
        "cols": ["id", "dead", "injured", "missing"],
        "dropna": {"subset": ["dead", "injured", "missing"], "how": "all"},
        "float_format": "%.0f",
    },
    "gda_housing.csv": {
        "cols": ["id", "totallyDamagedHouses", "partiallyDamagedHouses"],
        "dropna": {
            "subset": ["totallyDamagedHouses", "partiallyDamagedHouses"],
            "how": "all",
        }
    },
    "gda_infra.csv": {
        "cols": [
            "id",
            "infraDamageAmount",
            "commercialDamageAmount",
            "socialDamageAmount",
            "crossSectoralDamageAmount",
        ],
        "dropna": {
            "subset": [
                "infraDamageAmount",
                "commercialDamageAmount",
                "socialDamageAmount",
                "crossSectoralDamageAmount",
            ],
            "how": "all",
        },
    },
    "gda_dmg_general.csv": {
        "cols": ["id", "generalDamageAmount"],
        "dropna": {"subset": ["generalDamageAmount"], "how": "any"},
        "onlyIfMissing": [
            "infraDamageAmount",
            "agricultureDamageAmount",
            "commercialDamageAmount",
            "socialDamageAmount",
            "crossSectoralDamageAmount",
        ],
    },
    "gda_power.csv": {
        "cols": ["id", "powerAffected"],
        "dropna": {"subset": ["powerAffected"], "how": "any"},
    },
    "gda_comms.csv": {
        "cols": ["id", "communicationAffected"],
        "dropna": {"subset": ["communicationAffected"], "how": "any"},
    },
    "gda_rnb.csv": {
        "cols": ["id", "roadBridgeAffected"],
        "dropna": {"subset": ["roadBridgeAffected"], "how": "any"},
    },
    "gda_seaports.csv": {
        "cols": ["id", "seaportsAffected"],
        "dropna": {"subset": ["seaportsAffected"], "how": "any"},
    },
    "gda_airports.csv": {
        "cols": ["id", "airportsAffected"],
        "dropna": {"subset": ["airportsAffected"], "how": "any"},
    },
    "gda_water.csv": {
        "cols": ["id", "areDamsAffected", "isTapAffected"],
        "dropna": {"subset": ["areDamsAffected", "isTapAffected"], "how": "all"},
    },
    "gda_assistance.csv": {
        "cols": [
            "id",
            "allocatedFunds",
            "agencyLGUsPresentAssistance",
            "internationalOrgsPresent",
            "amountNGOs",
        ],
        "dropna": {
            "subset": [
                "allocatedFunds",
                "agencyLGUsPresentAssistance",
                "internationalOrgsPresent",
                "amountNGOs",
            ],
            "how": "all",
        },
        "float_format": "%.0f",
    },
    "gda_relief_goods.csv": {
        "cols": ["id", "itemCostGoods", "itemQtyGoods"],
        "dropna": {"subset": ["itemCostGoods", "itemQtyGoods"], "how": "all"},
        "float_format": "%.0f",
    },
    "gda_relief_water.csv": {
        "cols": ["id", "itemCostWater", "itemQtyWater"],
        "dropna": {"subset": ["itemCostWater", "itemQtyWater"], "how": "all"},
        "float_format": "%.0f",
    },
    "gda_relief_clothing.csv": {
        "cols": ["id", "itemCostClothing", "itemQtyClothing"],
        "dropna": {"subset": ["itemCostClothing", "itemQtyClothing"], "how": "all"},
        "float_format": "%.0f",
    },
    "gda_relief_med.csv": {
        "cols": ["id", "itemCostMedicine", "itemQtyMedicine"],
        "dropna": {"subset": ["itemCostMedicine", "itemQtyMedicine"], "how": "all"},
        "float_format": "%.0f",
    },
    "gda_relief_unspecified.csv": {
        "cols": ["id", "itemCostOthers1"],
        "dropna": {"subset": ["itemCostOthers1"], "how": "any"},
        "float_format": "%.0f",
    },
    "gda_relief_general.csv": {
        "cols": ["id", "itemCostOthers2"],
        "dropna": {"subset": ["itemCostOthers2"], "how": "any"},
        "float_format": "%.0f",
        "onlyIfMissing": [
            "itemCostOthers1",
            "itemCostClothing",
            "itemCostMedicine",
            "itemCostGoods",
            "itemCostWater",
        ],
    },
    "gda_recovery.csv": {
        "cols": ["id", "srrDone", "policyChanges", "postStructureCost", "postTraining"],
        "dropna": {
            "subset": ["srrDone", "policyChanges", "postStructureCost", "postTraining"],
            "how": "all",
        },
        "float_format": "%.0f",
    },
}

# Matches hyphen, en dash, and em dash
DASH = r"[-–—]"

# Null-like sentinel values to replace with NaN
NULL_VALUES = ["","-", "Not applicable", "n.a.", "Not indicated", np.nan, pd.NA]


def _event_id(row: pd.Series) -> str:
    key = "|".join([
        str(row.get("eventName") or ""),
        str(row.get("startDate")  or ""),
        str(row.get("hasLocation") or ""),
        str(row.name)
    ]).lower().strip()
    return uuid.uuid5(GDA_NS, key).hex

def normalize_one_date(text: str) -> str | None:
    """Parse a single date fragment and return 'YYYY-MM-DD', or None on failure."""
    for dayfirst in (False, True):
        try:
            return parse(text, dayfirst=dayfirst).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None

def clean_date_range(value: str) -> tuple[str | None, str | None]:
    """
    Normalise a raw date cell into a (startDate, endDate) pair.

    Handles: ISO dates, timestamps, year ranges, long date ranges,
    month-month-year ranges, month-year–month-year ranges,
    day-day month-year ranges, and single dates.
    """
    if pd.isna(value):
        return (None, None)

    text = str(value).strip()

    # Already a clean ISO date
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        return (text, text)

    # ISO timestamp — keep date part only
    if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$", text):
        return (text.split()[0], text.split()[0])

    # Strip timezone abbreviations and time components
    text = re.sub(r"\b[A-Z]{2,4}\b", "", text).strip()
    text = re.sub(r"\b\d{1,2}:\d{2}(:\d{2})?\s*(AM|PM|am|pm)?\b", "", text).strip()

    # Year range: "1918–1919"
    m = re.match(rf"^\s*(\d{{4}})\s*{DASH}\s*(\d{{4}})\s*$", text)
    if m:
        y1, y2 = m.groups()
        return (f"{y1}-01-01", f"{y2}-12-31")

    # Long date range: "August 10, 2008 – July 14, 2009"
    m = re.match(
        rf"^\s*([A-Za-z]+\s+\d{{1,2}},?\s*\d{{4}})\s*{DASH}\s*([A-Za-z]+\s+\d{{1,2}},?\s*\d{{4}})\s*$",
        text,
    )
    if m:
        left, right = m.groups()
        return (normalize_one_date(left), normalize_one_date(right))

    # Month–Month Year: "April–June 1957"
    m = re.match(rf"^\s*([A-Za-z]+)\s*{DASH}\s*([A-Za-z]+)\s+(\d{{4}})", text)
    if m:
        m1, m2, year = m.groups()
        start = normalize_one_date(f"1 {m1} {year}")
        end = normalize_one_date(f"1 {m2} {year}")
        if end:
            end = (pd.to_datetime(end) + pd.tseries.offsets.MonthEnd()).strftime("%Y-%m-%d")
        return (start, end)

    # Month Year–Month Year: "April 1965–June 1957"
    m = re.match(rf"^\s*([A-Za-z]+)\s*(\d{{4}})\s*{DASH}\s*([A-Za-z]+)\s+(\d{{4}})", text)
    if m:
        m1, y1, m2, y2 = m.groups()
        start = normalize_one_date(f"1 {m1} {y1}")
        end = normalize_one_date(f"1 {m2} {y2}")
        if end:
            end = (pd.to_datetime(end) + pd.tseries.offsets.MonthEnd()).strftime("%Y-%m-%d")
        return (start, end)

    # Day–Day Month Year: "2–7 July 2001"
    m = re.match(
        rf"^\s*(\d{{1,2}})\s*{DASH}\s*(\d{{1,2}})\s+([A-Za-z]+)\s*,?\s*(\d{{4}})\s*$",
        text,
    )
    if m:
        d1, d2, month, year = m.groups()
        return (
            normalize_one_date(f"{d1} {month} {year}"),
            normalize_one_date(f"{d2} {month} {year}"),
        )

    # Month Day–Day Year: "Nov 12–15 2003"
    m = re.match(
        rf"^\s*([A-Za-z]+)\s+(\d{{1,2}})\s*{DASH}\s*(\d{{1,2}})\s*,?\s*(\d{{4}})\s*$",
        text,
    )
    if m:
        month, d1, d2, year = m.groups()
        return (
            normalize_one_date(f"{d1} {month} {year}"),
            normalize_one_date(f"{d2} {month} {year}"),
        )

    # Month Day – Month Day Year: "August 31 – September 4, 1984"
    m = re.match(
        rf"^\s*([A-Za-z]+\s+\d{{1,2}})\s*{DASH}\s*([A-Za-z]+\s+\d{{1,2}})\s*,?\s*(\d{{4}})\s*$",
        text,
    )
    if m:
        left, right, year = m.groups()
        return (
            normalize_one_date(f"{left} {year}"),
            normalize_one_date(f"{right} {year}"),
        )

    # Year only: "1984"
    if re.match(r"^\d{4}$", text):
        return (f"{text}-01-01", f"{text}-12-31")

    # Single date fallback
    single = normalize_one_date(text)
    return (single, single) if single else (None, None)

def load_with_tiered_headers(path: str | Path) -> pd.DataFrame:
    """
    Load an XLSX file with 3–4 tier headers.

    Stops merging header levels when a level is unnamed (i.e. from merged cells).
    """
    df = pd.read_excel(path, header=[3, 4, 5, 6])

    new_cols = []
    for col_tuple in df.columns:
        parts = []
        for level in col_tuple:
            s = str(level).strip()
            if s.lower().startswith("unnamed") or s in ("", "nan"):
                break
            parts.append(s)
        new_cols.append("_".join(parts))

    df.columns = new_cols
    return df

def to_type_iri(dtype: str | None) -> list[str]:

    if dtype is None: return []

    # Subtype incidents are handled separately
    if "[" in dtype:
        return dtype

    types = dtype.split("|")
    cleaned_iris: list[str] = []

    for t in types:
        fixed_iri = (
            t.strip()
            .replace(" ", "") 
            .replace("(", "")
            .replace(")", "")
            .replace("Misc", "Miscellaneous")
            .replace("Flashflood", "FlashFlood")
            .replace("Earthquake", "")
        )

        if fixed_iri:
            cleaned_iris.append(fixed_iri)

    # print(cleaned_iris)
    return cleaned_iris

def export_slices(df: pd.DataFrame, specs: dict[str, Any], out_dir: str | Path = "../data/parsed/gda") -> None:
    """Write each slice defined in *specs* as a CSV file under *out_dir*."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for filename, spec in specs.items():
        if "onlyIfMissing" in spec:
            tmp = df[df[spec["onlyIfMissing"]].isna().all(axis=1)][spec["cols"]].copy()
        else:
            tmp = df[spec["cols"]].copy()

        if "dropna" in spec:
            tmp = tmp.dropna(
                subset=spec["dropna"]["subset"],
                how=spec["dropna"].get("how", "any"),
            )

        if "contains" in spec:
            c = spec["contains"]
            mask = tmp[c["column"]].str.contains(c["pattern"], case=c.get("case", True), na=False)
            if c.get("exclude", False):
                mask = ~mask
            tmp = tmp[mask]

        if "rename" in spec:
            tmp.rename(columns=spec["rename"], inplace=True)

        for col, dtype in spec.get("astype", {}).items():
            tmp[col] = tmp[col].astype(dtype)

        tmp.to_csv(out_dir / filename, index=False, float_format=spec.get("float_format"))

def _null(value: Any) -> bool:
    
    return (
        value is None
        or value is pd.NA
        or (isinstance(value, float) and math.isnan(value))
        or (isinstance(value, str) and value.strip().lower() in ("none", "nan", ""))
    )

def transform_gda(path: str) -> dict[type, list[Any]]:
    raw_path = Path("../data/raw/static/geog-archive-cleaned.xlsx")
    out_dir = Path("../data/parsed/gda")

    df = load_with_tiered_headers(raw_path)
    df = df.rename(columns=COLUMN_MAPPING)
    df = df[list(COLUMN_MAPPING.values())]

    # Drop entirely empty rows/columns and rows missing key fields
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    df = df.dropna(subset=["date", "hasLocation"])

    # Normalise sentinel null values
    for sentinel in NULL_VALUES:
        df = df.replace(sentinel, None).infer_objects(copy=False)

    # Parse dates
    df[["startDate", "endDate"]] = df["date"].apply(lambda v: pd.Series(clean_date_range(v)))

    df = df.dropna(subset=["startDate"])

    df["eventName"] = df["eventName"].str.replace('"', "", regex=False)
    # df["id"] = [uuid.uuid4().hex for _ in range(len(df))]

    df["hasType"] = df["hasType"].apply(
        lambda t: "|".join(to_type_iri(t))
    )

    df["hasSubtypeRaw"] = df["hasSubtype"]
    df["hasSubtype"] = df["hasSubtype"].apply(
        lambda t: "|".join(to_type_iri(t))
    )
    # match locations
    df["hasLocation"] = df["hasLocation"].astype(str).apply(
        lambda cell: "|".join(LOCATION_MATCHER.match_cell(cell))
    )

    df["totallyDamagedHouses"] = df["totallyDamagedHouses"].astype(float).astype("Int64")

    df["id"] = df.apply(_event_id, axis=1)

    # general damage amount values if no other were reported (exclude totals)
    cols = ["infraDamageAmount", "agricultureDamageAmount", "commercialDamageAmount", "socialDamageAmount", "crossSectoralDamageAmount"]
    df.loc[df[cols].notna().any(axis=1), "generalDamageAmount"] = None

    # Explode compound sub-type incidents into a separate table
    incidents: dict[str, list[Any]] = {f.name: [] for f in fields(Incident)}

    for _, row in df.iterrows():
        subtypes = row["hasSubtypeRaw"]
        if isinstance(subtypes, str) and "[" in subtypes:
            for cnt, instance in enumerate(subtypes.split(";"), start=1):
                has_type, has_location = (
                    instance.strip().replace("[", "").replace("]", "").split(":")
                )

                incidents["id"].append(row["id"])
                incidents["hasType"].append("|".join(to_type_iri(has_type)))
                incidents["hasLocation"].append("|".join(LOCATION_MATCHER.match_cell(has_location)))
                incidents["sub_id"].append(cnt)
                incidents["startDate"].append(row["startDate"])
                incidents["endDate"].append(row["endDate"])

    pd.DataFrame(incidents).to_csv(out_dir / "gda_incidents.csv")

    export_slices(df, EXPORT_SPECS, out_dir)

    clss: list[type] = [Assistance, AffectedPopulation, Casualties, CommunicationLineDisruption,
    DeclarationOfCalamity, DamageGeneral, Evacuation, Event, HousingDamage, InfrastructureDamage, PowerDisruption, Preparedness, Recovery,
    Rescue, RoadAndBridgesDamage, SeaportDisruption,
    WaterDisruption]

    entities: dict[type, list[Any]] = {}
    for cls in clss: entities[cls] = []

    df = df.loc[:, ~df.columns.duplicated()]

    # main csv to entities
    for row in df.to_dict(orient="records"):
    
        for cls in clss:
            data: dict[str, Any] = {}
            class_fields = fields(cls)

            for f in class_fields:
                value = row.get(f.name, None)

                if _null(value):
                    data[f.name] = None
                else:
                    data[f.name] = value
            # skip empty entities
            if any(v is not None and v != "" for k, v in data.items() if k != "id"):
                entities[cls].append(cls(**data))

    # incidents

    incidents_df = pd.DataFrame(incidents)
    entities[Incident] = []
    for row in incidents_df.to_dict(orient="records"):
        data: dict[str, Any] = {}

        for f in fields(Incident):
            value = row.get(f.name, None)

            if _null(value):
                data[f.name] = None
            else:
                # print(value)
                data[f.name] = value

        if any(v is not None and v != "" for k, v in data.items() if k != "id"):
            # print(Incident(**data))
            entities[Incident].append(Incident(**data))
    
    # relief — one Relief entity per type per row
    _RELIEF_COLS: list[tuple[str, str, str | None]] = [
        ("goods",       "itemCostGoods",    "itemQtyGoods"),
        ("water",       "itemCostWater",    "itemQtyWater"),
        ("clothing",    "itemCostClothing", "itemQtyClothing"),
        ("medicine",    "itemCostMedicine", "itemQtyMedicine"),
        ("unspecified", "itemCostOthers1",  None),
        ("general",     "itemCostOthers2",  None),
    ]

    _GENERAL_GUARD_COLS = [
        "itemCostOthers1", "itemCostClothing",
        "itemCostMedicine", "itemCostGoods", "itemCostWater",
    ]

    entities[Relief] = []
    for row in df.to_dict(orient="records"):
        for item_type, cost_col, qty_col in _RELIEF_COLS:
            cost = row.get(cost_col) 
            qty  = row.get(qty_col) if qty_col else None

            if item_type == "general" and any(not _null(row.get(c)) for c in _GENERAL_GUARD_COLS):
                continue

            if _null(cost) and _null(qty):
                continue

            entities[Relief].append(Relief(
                id=row["id"],
                itemType=item_type,
                itemCost=None if _null(cost) else cost,
                itemQty=None if _null(qty) else str(qty),
            ))

    df.to_csv(out_dir / "gda.csv")

    return entities


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transform EM-DAT XLSX to SakunaGraPH-conformant RDF."
    )
    parser.add_argument(
        "--input", "-i",
        default="../data/raw/static/geog-archive-cleaned.xlsx",
        help="Path to the GDA .xlsx file.",
    )
    args = parser.parse_args()

    transform_gda(args.input)
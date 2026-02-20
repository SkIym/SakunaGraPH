# NDRRMC CSVs AND JSONS MAPPER HERE
import os
from typing import List
import uuid
import json
from preprocessors.location_matcher import LOCATION_MATCHER
from preprocessors.disaster_classifier import DISASTER_CLASSIFIER
from mappings.ndrrmc_mappings import AFF_POP_COL_MAP, INCIDENT_COLUMN_MAPPINGS, AffectedPopulation, Casualties, Event, Provenance, Incident
from datetime import datetime
from preprocessors.ndrrmc_cleaner import concat_loc_levels, event_name_expander, forward_fill_and_collapse, normalize_datetime, to_int
import polars as pl


def load_uuids(folder_path: str):

    for folder in next(os.walk(folder_path))[1]:

        meta_path = os.path.join(folder_path, folder, "metadata.json")
        try:
            if os.path.exists(meta_path):
                with open(meta_path, "r", encoding="utf-8") as f:
                    try:
                        meta = json.load(f)
                    except json.JSONDecodeError:
                        meta = {}
            else:
                meta = {}

            if "id" not in meta:
                meta["id"] = uuid.uuid4().hex

            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)

            # print(f"Updated metadata: {meta_path}")

        except Exception as e:
            print(f"Failed to update {meta_path}: {e}")


    # start with mapping event metadata

def load_events(folder_path: str) -> list[Event]:
    events: list[Event] = []

    for folder in next(os.walk(folder_path))[1]:
        meta_path = os.path.join(folder_path, folder, "metadata.json")
        src_path = os.path.join(folder_path, folder, "source.json" )

        if not os.path.exists(meta_path):
            continue

        with open(meta_path, "r", encoding="utf-8") as f:
            meta: dict[str, str] = json.load(f)
        
        with open(src_path, "r", encoding="utf-8") as f:
            source: dict[str, str] = json.load(f)

        event_remarks = meta.get("remarks")
        event_name = event_name_expander(meta.get("eventName", folder)) 
        file_name = source.get("reportName", "")
        prediction = "MiscellaneousAccidentGeneral" # Deafult event type

        if event_remarks:
            
            first_d = event_remarks.split(". ")[0]
            # print(first_d)

            (prediction, _) = DISASTER_CLASSIFIER.classify([event_name + file_name + ": " + first_d])[0]
        else:
            (prediction, _) = DISASTER_CLASSIFIER.classify([event_name  + file_name])[0]


        ev = Event(
            id=meta.get("id", uuid.uuid4().hex),
            eventName=meta.get("eventName", folder),
            startDate=datetime.fromisoformat(meta["startDate"]) if meta["startDate"] else None,
            endDate=datetime.fromisoformat(meta["endDate"]) if meta["endDate"] else None,
            remarks=meta.get("remarks"),
            hasType=prediction,
        )

        events.append(ev)

    return events

def load_provenance(event_folder_path: str) -> Provenance | None:

    src_path = os.path.join(event_folder_path, "source.json")

    if not os.path.exists(src_path):
        return None

    with open(src_path, "r", encoding="utf-8") as f:
        src: dict[str, str] = json.load(f)

    p = Provenance(
        lastUpdateDate=datetime.fromisoformat(src["lastUpdateDate"]),
        reportLink=src.get("reportLink"),
        reportName=src.get("reportName", ""),
        obtainedDate=src.get("obtainedDate") if src["obtainedDate"] else None,

    )
    
    return p

def load_incidents(event_folder_path: str) -> List[Incident] | None:

    src_path = os.path.join(event_folder_path, "related_incidents.csv")

    if not os.path.exists(src_path):
        return None

    target_cols = ["Region", "Province", "City_Muni"]
    df = pl.read_csv(src_path)

    # fix column names
    df = df.rename(INCIDENT_COLUMN_MAPPINGS, strict=False)

    print("Forward filling...")

    # clean rows by retaining only the actual incident entry
    df = forward_fill_and_collapse(df, target_cols, "QTY", "TYPE_OF_INCIDENT")

    meta_path = os.path.join(event_folder_path, "metadata.json")

    with open(meta_path, "r", encoding="utf-8") as f:
            meta: dict[str, str] = json.load(f)
        
    event_name = meta.get("eventName", event_folder_path)

    df = df.with_columns([
        pl.lit("due to " + event_name_expander(event_name)).alias("event_name")
    ])

    print("Classifying disaster types...")
    # classify the type of the disaster based on incident type text and description
    type_texts = (
        df
        .select(
            pl.concat_str(
                ["TYPE_OF_INCIDENT", "STATUS_for_flooded\nareas","DESCRIPTION", "event_name"],
                separator=" — ",
                ignore_nulls=True
            ).alias("combined")
        )
        .to_series()
        .to_list()
    )


    predictions = DISASTER_CLASSIFIER.classify(type_texts)
    pred_classes: list[str] = []

    for _, (pred_class, _) in zip(type_texts, predictions):
        pred_classes.append(pred_class)

    # add column for predicted type
    df = df.with_columns([
        pl.Series("hasType", pred_classes),
    ])

    # lower case locations
    df = df.with_columns(Province=pl.col("Province").str.to_lowercase(),
                    City_Muni=pl.col("City_Muni").str.to_lowercase())



    # Match locations
    locations = concat_loc_levels(df, ["City_Muni", "Province", "Region"], ",")
    matched_locations = LOCATION_MATCHER.match(locations)
    df = df.with_columns([
        pl.Series("hasLocation", matched_locations),
    ])

    matched_locations = LOCATION_MATCHER.match(locations)
    df = df.with_columns([
        pl.Series("hasLocation", matched_locations),
    ])
    
    print("Normalizing datetime...")
    df = normalize_datetime(df,
                            "DATE_OF\nOCCURENCE","TIME_OF\nOCCURENCE", 
                            "%d %B %Y %I:%M %P", 
                            "%d %B %Y")

    # add column for index 
    df = df.with_row_index("incident_id", 1)

    print("Writing csv to " + event_folder_path)

    df.write_csv(event_folder_path + "/cleaned_related_incidents.csv")

    # Construct Incident objects
    incidents: List[Incident] = []
    for row in df.iter_rows(named=True):
        incident = Incident(
            id=row["incident_id"],
            incidentActionsTaken=row["ACTIONS_TAKEN"],
            incidentDescription=row["DESCRIPTION"],
            startDate=row["startDate"],
            endDate=row["startDate"],
            hasLocation=row["hasLocation"],
            hasBarangay=row["Barangay"],
            hasType=row["hasType"],
            remarks=row["REMARKS"]
        )

        incidents.append(incident)

    return incidents

def load_aff_pop(event_folder_path: str) -> List[AffectedPopulation] | None:
    src_path = os.path.join(event_folder_path, "affected_population.csv")

    if not os.path.exists(src_path):
        return None
    
    target_cols = ["Region", "Province", "City_Muni"]
    df = pl.read_csv(src_path)

    # fix column names
    df = df.rename(AFF_POP_COL_MAP, strict=False)


    # clean rows by retaining only the actual incident entry
    df = forward_fill_and_collapse(df, target_cols, "QTY", "affectedBarangays")

    # Match locations
    locations = concat_loc_levels(df, ["City_Muni", "Province", "Region"], ",")
    matched_locations = LOCATION_MATCHER.match(locations)
    df = df.with_columns([
        pl.Series("hasLocation", matched_locations),
    ])

    # downcast to int for int columns
    df = to_int(df, ["affectedFamilies", "affectedBarangays", "affectedPersons", "displacedFamilies", "displacedPersons"])

    df = df.with_row_index("id", 1)
    
    df.write_csv(event_folder_path + "/cleaned_affected_population.csv")
    affpops: List[AffectedPopulation] = []
    for row in df.iter_rows(named=True):
        affpop = AffectedPopulation(
            id=row["id"],
            affectedBarangays=row["affectedBarangays"],
            affectedFamilies=row["affectedFamilies"],
            affectedPersons=row["affectedPersons"],
            displacedFamilies=row["displacedFamilies"],
            displacedPersons=row["displacedPersons"],
            hasLocation=row["hasLocation"],
            hasBarangay=row["Barangay"]
        )

        affpops.append(affpop)
    
    return affpops

def load_casualties(event_folder_path: str) -> List[Casualties] | None:
    src_path = os.path.join(event_folder_path, "casualties.csv")

    if not os.path.exists(src_path):
        src_path = os.path.join(event_folder_path, "casualties_2.csv")
    
    if not os.path.exists(src_path): return None
    
    df = pl.read_csv(src_path)

    # forward fille and retain most granular entity
    target_cols = ["Region", "Province", "City_Muni", "Summary_Type"]
    df = forward_fill_and_collapse(df, target_cols, "QTY", "VALIDATED")

    # Match locations
    locations = concat_loc_levels(df, ["City_Muni", "Province", "Region"], ",")
    matched_locations = LOCATION_MATCHER.match(locations)
    df = df.with_columns([
        pl.Series("hasLocation", matched_locations),
    ])

    # Normalize casualty type
    df = df.with_columns(
        pl.when(pl.col("Summary_Type").str.contains(r"(?i)injured"))
            .then(pl.lit("INJURED"))
            .otherwise(pl.col("Summary_Type"))
            .alias("casualtyType")
    )

    df = df.with_row_index("id", 1)
    df.write_csv(f"{event_folder_path} + /cleaned_casualties.csv")

    casualties: List[Casualties] = []

    for row in df.iter_rows(named=True):
        cas = Casualties(
            id=row["id"],
            casualtyType=row["casualtyType"],
            casualtyCount=row["QTY"] if row["QTY"] else 1,
            hasLocation=row["hasLocation"],
            hasBarangay=row["Barangay"],
            casualtyDataSource=row["SOURCE_OF\nDATA"],
            casualtyCause=row["CAUSE"],
            remarks=row["REMARKS"]
        )

        casualties.append(cas)
    
    return casualties


if __name__ == "__main__":
    load_casualties("./data/ndrrmc_mini/Combined Effects of  Enhanced SWM and TCs FERDIE GENER and HELEN IGME 2024")
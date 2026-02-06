# NDRRMC CSVs AND JSONS MAPPER HERE
import os
import uuid
import json
from etl.prep.disaster_classifier import DISASTER_CLASSIFIER
from mappings.ndrrmc_mappings import INCIDENT_COLUMN_MAPPINGS, Event, Provenance, Incident
from datetime import datetime
from prep.ndrrmc_cleaner import forward_fill_and_collapse
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

            print(f"Updated metadata: {meta_path}")

        except Exception as e:
            print(f"Failed to update {meta_path}: {e}")


    # start with mapping event metadata

def load_events(folder_path: str) -> list[Event]:
    events: list[Event] = []

    for folder in next(os.walk(folder_path))[1]:
        meta_path = os.path.join(folder_path, folder, "metadata.json")

        if not os.path.exists(meta_path):
            continue

        with open(meta_path, "r", encoding="utf-8") as f:
            meta: dict[str, str] = json.load(f)

        ev = Event(
            id=meta.get("id", uuid.uuid4().hex),
            eventName=meta.get("eventName", folder),
            startDate=datetime.fromisoformat(meta["startDate"]) if meta["startDate"] else None,
            endDate=datetime.fromisoformat(meta["endDate"]) if meta["endDate"] else None,
            remarks=meta.get("remarks")
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

def load_incidents(event_folder_path: str) -> list[Incident] | None:

    src_path = os.path.join(event_folder_path, "related_incidents.csv")

    if not os.path.exists(src_path):
        return None

    target_cols = ["Region", "Province", "City_Muni"]
    df = pl.read_csv(src_path)

    # fix column names
    df = df.rename(INCIDENT_COLUMN_MAPPINGS)

    # clean rows by retaining only the actual incident entry
    df = forward_fill_and_collapse(df, target_cols, "Qty", "Type of Incident")

    # classify the type of the disaster
    type_texts = df.select("Type of Incident").to_series().to_list()
    predictions = DISASTER_CLASSIFIER.classify(type_texts)
    pred_classes: list[str] = []

    for _, (pred_class, _) in zip(type_texts, predictions):
        pred_classes.append(pred_class)

    # add column for predicted type
    df = df.with_columns([
        pl.Series("hasType", pred_classes),
    ])

    # add column for index 
    df = df.with_row_index("incident_id", 1)
    

    # Construct Incident objects
    incidents = []
    for row in df.iter_rows(named=True):
        incident = Incident(
            region=row["Region"],
            province=row["Province"],
            city=row["City_Muni"],
            incident_type=row["Type of Incident"],
            predicted_class=row["PredictedClass"],
            similarity_score=row["SimilarityScore"],
            qty=row.get("Qty")
        )
        incidents.append(incident)

    return incidents

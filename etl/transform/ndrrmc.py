# NDRRMC CSVs AND JSONS MAPPER HERE
import os
from re import S
from typing import Iterable, List
import uuid
import json
from dataclasses import fields
from semantic_processing.location_matcher import LOCATION_MATCHER
from semantic_processing.disaster_classifier import DISASTER_CLASSIFIER
from mappings.ndrrmc import AFF_POP_COL_MAP, AGRI_MAPPING, ASSISTANCE_PROVIDED_MAPPING, HOUSES_MAPPING, INCIDENT_COLUMN_MAPPINGS, INFRA_MAPPING, PEVAC_MAPPING, AffectedPopulation, Agriculture, Casualties, Event, Housing, Infrastructure, PEvacuation, Provenance, Incident, Relief
from datetime import datetime
from transform.ndrrmc_cleaner import concat_loc_levels, correct_QTY_column, event_name_expander, forward_fill_and_collapse, normalize_datetime, remove_summary_rows, replace_column_whitespace_with_underscore, to_float, to_int, to_million_php
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
    df = correct_QTY_column(df)
    df = df.rename(INCIDENT_COLUMN_MAPPINGS, strict=False)

    # print("Forward filling...")

    # clean rows by retaining only the actual incident entry
    df = forward_fill_and_collapse(df, target_cols, "QTY", "TYPE_OF_INCIDENT")

    meta_path = os.path.join(event_folder_path, "metadata.json")

    with open(meta_path, "r", encoding="utf-8") as f:
            meta: dict[str, str] = json.load(f)
        
    event_name = meta.get("eventName", event_folder_path)

    df = df.with_columns([
        pl.lit("due to " + event_name_expander(event_name)).alias("event_name")
    ])

    # print("Classifying disaster types...")
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
    
    # print("Normalizing datetime...")
    df = normalize_datetime(df,
                            "DATE_OF\nOCCURENCE","TIME_OF\nOCCURENCE", 
                            "%d %B %Y %I:%M %P", 
                            "%d %B %Y")

    # add column for index 
    df = df.with_row_index("incident_id", 1)

    # print("Writing csv to " + event_folder_path)

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
    df = correct_QTY_column(df)
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

    app_fields = [f.name for f in fields(AffectedPopulation)]
    app_fields.append("evacuationCenters")

    df = df.select(app_fields)

    df.write_csv(event_folder_path + "/cleaned_affected_population.csv")
    
    ents: List[AffectedPopulation] = []

    for row in df.iter_rows(named=True):
            rel_kwargs = {f.name: row.get(f.name) for f in fields(AffectedPopulation)}
            ent = AffectedPopulation(**rel_kwargs)

            ents.append(ent)

    
    return ents
    

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
    df.write_csv(event_folder_path + "/cleaned_casualties.csv")

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

def load_relief(event_folder_path: str) -> List[Relief] | None:

    # src_path = os.path.join(event_folder_path, "assistance_provided.csv")

    # handle different types of assistance provided tables
    src_paths: list[str] = []
    for file in os.listdir(event_folder_path):
        if "assistance_provided" in file and file.endswith(".csv"):
            src_paths.append(os.path.join(event_folder_path, file))
    
    if len(src_paths) == 0: return None
    
    reliefs: List[Relief] = []
    index = 1

    dfs: Iterable[pl.DataFrame] = []

    for src_path in src_paths:
    
        df = pl.read_csv(src_path)
        df = correct_QTY_column(df)
        df = df.rename(mapping=ASSISTANCE_PROVIDED_MAPPING, strict=False)
        
        # forward fill and retain most granular entity
        target_cols = ["Region", "Province", "City_Muni"]
        df = forward_fill_and_collapse(df, target_cols, "QTY", "itemCost")

        # Match locations
        locations = concat_loc_levels(df, ["City_Muni", "Province", "Region"], ",")
        matched_locations = LOCATION_MATCHER.match(locations)
        df = df.with_columns([
            pl.Series("hasLocation", matched_locations),
        ])

        df = to_float(df, ["itemCost", "itemCostPerUnit", "itemQuantity"])

        df = df.with_row_index("id", index)
        dfs.append(df)

        for row in df.iter_rows(named=True):
            rel_kwargs = {f.name: row.get(f.name) for f in fields(Relief)}
            rel = Relief(**rel_kwargs)

            reliefs.append(rel)
            index += 1
        
    final_dfs: Iterable[pl.DataFrame] = []

    for df in dfs:
        existing_cols = [f.name for f in fields(Relief) if f.name in df.columns]
        missing_cols = [f.name for f in fields(Relief) if f.name not in df.columns]
        
        # select existing columns
        df_selected = df.select(existing_cols)
        
        # add missing columns as None
        for col in missing_cols:
            df_selected = df_selected.with_columns(pl.lit(None).alias(col))
        
        # reorder columns to match Relief fields
        df_selected = df_selected.select([f.name for f in fields(Relief)])
        
        final_dfs.append(df_selected)

    # concatenate all DataFrames
    final_df = pl.concat(final_dfs, rechunk=True)

    # print(len(final_df) == len(reliefs))
    final_df.write_csv(event_folder_path + "/cleaned_assistance.csv")
 

    return reliefs

def load_infra(event_folder_path: str) -> List[Infrastructure] | None:

    src_path = os.path.join(event_folder_path, "damage_to_infrastructure.csv")
    
    if not os.path.exists(src_path): return None
    
    df = pl.read_csv(src_path)
    df = correct_QTY_column(df)
    df = df.rename(mapping=INFRA_MAPPING, strict=False)

    # forward fille and retain most granular entity
    target_cols = ["Region", "Province", "City_Muni"]
    df = forward_fill_and_collapse(df, target_cols, "QTY", "infraDamageType")

    # Match locations
    locations = concat_loc_levels(df, ["City_Muni", "Province", "Region"], ",")
    matched_locations = LOCATION_MATCHER.match(locations)
    df = df.with_columns([
        pl.Series("hasLocation", matched_locations),
    ])


    df = to_int(df, ["numberInfraDamaged"])
    df = to_million_php(df, ["infraDamageAmount"])

    df = df.with_row_index("id", 1)
    df.write_csv(event_folder_path + "/cleaned_infra.csv")

    infra: List[Infrastructure] = []

    for row in df.iter_rows(named=True):
            rel_kwargs = {f.name: row.get(f.name) for f in fields(Infrastructure)}
            inf = Infrastructure(**rel_kwargs)

            infra.append(inf)

    
    return infra

def load_housing(event_folder_path: str) -> List[Housing] | None:

    src_path = os.path.join(event_folder_path, "damaged_houses.csv")
    
    if not os.path.exists(src_path): return None
    
    df = pl.read_csv(src_path)
    df = correct_QTY_column(df)
    df = df.rename(mapping=HOUSES_MAPPING, strict=False)

    # forward fill
    target_cols = ["Region", "Province"]
    df = df.with_columns(
        pl.col(c).forward_fill() for c in target_cols
    )

    # remove summary rows
    df = remove_summary_rows(df, nulls=["City_Muni", "Barangay"])

    # remove dupes
    df = df.unique(
        subset=[
                "City_Muni",
                "Barangay",
                "totallyDamagedHouses",
                "partiallyDamagedHouses",
                "housingDamageAmount"
            ],
        maintain_order=True)

    # forward fill
    df = df.with_columns(
        pl.col("City_Muni").forward_fill()
    )

    # remove city summary rows
    df = df.filter(
        ~(
            pl.col("Barangay").is_null()
            & pl.col("City_Muni").is_duplicated()
        )
    )

    # Match locations
    locations = concat_loc_levels(df, ["City_Muni", "Province", "Region"], ",")
    matched_locations = LOCATION_MATCHER.match(locations)
    df = df.with_columns([
        pl.Series("hasLocation", matched_locations),
    ])

    df = to_int(df, ["totallyDamagedHouses", "partiallyDamagedHouses"])
    df = to_million_php(df, ["housingDamageAmount"])

    df = df.with_row_index("id", 1)
    df.write_csv(event_folder_path + "/cleaned_houses.csv")

    houses: List[Housing] = []

    for row in df.iter_rows(named=True):
            rel_kwargs = {f.name: row.get(f.name) for f in fields(Housing)}
            h = Housing(**rel_kwargs)

            houses.append(h)

    
    return houses

def load_agri(event_folder_path: str) -> List[Agriculture] | None:


    src_path = None
    for file in os.listdir(event_folder_path):
        if "agriculture" in file.lower() and file.endswith(".csv"):
            src_path = os.path.join(event_folder_path, file)
            break
    
    if not src_path: return None
    
    df = pl.read_csv(src_path)
    df = correct_QTY_column(df)
    df = replace_column_whitespace_with_underscore(df)
    df = df.rename(mapping=AGRI_MAPPING, strict=False)

    # forward fille and retain most granular entity
    target_cols = ["Region", "Province", "City_Muni"]
    df = forward_fill_and_collapse(df, target_cols, "QTY", "agriDamageClassification")

    # Match locations
    locations = concat_loc_levels(df, ["City_Muni", "Province", "Region"], ",")
    matched_locations = LOCATION_MATCHER.match(locations)
    df = df.with_columns([
        pl.Series("hasLocation", matched_locations),
    ])

    df = to_int(df, ["partiallyDamagedInfrastructure", "totallyDamagedInfrastructure", "farmerFisherfolkAffected"])
    df = to_million_php(df, ["productionLossCost", "agriDamageAmount"])
    df = to_float(df, ["productionLossVolume", "partiallyDamagedCropArea", "totallyDamagedCropArea"])

    df = df.with_row_index("id", 1)
    df.write_csv(event_folder_path + "/cleaned_agri.csv")

    ents: List[Agriculture] = []

    for row in df.iter_rows(named=True):
            rel_kwargs = {f.name: row.get(f.name) for f in fields(Agriculture)}
            ent = Agriculture(**rel_kwargs)

            ents.append(ent)

    
    return ents

def load_pevac(event_folder_path: str) -> List[PEvacuation] | None:


    # preemptive evac csv path
    src_path = os.path.join(event_folder_path, "pre-emptive_evacuation.csv")
    
    # affected pop path (for no. of evacuationCenters)
    ap_path = os.path.join(event_folder_path, "cleaned_affected_population.csv")

    src_exists = os.path.exists(src_path)
    ap_exists = os.path.exists(ap_path)

    if not src_exists and not ap_exists: return None
    
    e_df: pl.DataFrame = pl.DataFrame()
    if src_exists:

        e_df = pl.read_csv(src_path)
        e_df = correct_QTY_column(e_df)
        e_df = replace_column_whitespace_with_underscore(e_df)
        e_df = e_df.rename(mapping=PEVAC_MAPPING, strict=False)

        # forward fille and retain most granular entity
        target_cols = ["Region", "Province", "City_Muni"]
        e_df = forward_fill_and_collapse(e_df, target_cols, "QTY", "preemptPersons")

        # Match locations
        locations = concat_loc_levels(e_df, ["City_Muni", "Province", "Region"], ",")
        matched_locations = LOCATION_MATCHER.match(locations)
        e_df = e_df.with_columns([
            pl.Series("hasLocation", matched_locations),
        ])


        e_df = to_int(e_df, ["preemptFamilies", "preemptPersons"])


    ap_df: pl.DataFrame = pl.DataFrame()
    if ap_exists:

        ap_df = pl.read_csv(ap_path)
        ap_df = ap_df.select([
            "hasLocation",
            "hasBarangay",
            "evacuationCenters"
        ])

        ap_df = to_int(ap_df, ["evacuationCenters"])

    
    df: pl.DataFrame = pl.DataFrame()

    if ap_exists and not src_exists:
        df = ap_df
        df = df.filter(
            ~(
                (pl.col("evacuationCenters") == 0)
            )
        )
    elif src_exists and not ap_exists:
        df = e_df
    else:
        df = e_df.join(
            other=ap_df,
            on=["hasLocation"],
            how="full", 
            coalesce=True
        )

        # remove entries with all empty values
        df = df.filter(
            ~(
                (pl.col("evacuationCenters") == 0)
                & pl.all_horizontal(
                    pl.col(["preemptFamilies", "preemptPersons"]).is_null()
                )
            )
        )

        # merge barangay columns, skip same barangays
        df = df.with_columns(
            pl.when(pl.col("hasBarangay") == pl.col("hasBarangay_right"))
            .then(pl.col("hasBarangay"))
            .otherwise(
                pl.concat_str(
                    ["hasBarangay", "hasBarangay_right"],
                    separator="",
                    ignore_nulls=True
                )
            ).replace("", None)
            .alias("hasBarangay")
        )

    

    
    

    df = df.with_row_index("id", 1)
    df.write_csv(event_folder_path + "/cleaned_preemptive_evacuation.csv")

    ents: List[PEvacuation] = []

    for row in df.iter_rows(named=True):
            rel_kwargs = {f.name: row.get(f.name) for f in fields(PEvacuation)}
            ent = PEvacuation(**rel_kwargs)

            ents.append(ent)

    return ents

if __name__ == "__main__":
    load_aff_pop("../data/parsed/ndrrmc_mini/Combined Effects of  Enhanced SWM and TCs FERDIE GENER and HELEN IGME 2024")
    load_pevac("../data/parsed/ndrrmc_mini/Combined Effects of  Enhanced SWM and TCs FERDIE GENER and HELEN IGME 2024")

    # load_housing("../data/parsed/ndrrmc_mini/Magnitude 6 8 Earthquake in Sarangani Davao Occidental/")
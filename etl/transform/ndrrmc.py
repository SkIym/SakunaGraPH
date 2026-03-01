import os
import uuid
import json
from typing import Iterable, List, Mapping, Type, TypeVar, Callable
from dataclasses import fields
from datetime import datetime

import polars as pl
from semantic_processing.location_matcher import LOCATION_MATCHER
from semantic_processing.disaster_classifier import DISASTER_CLASSIFIER

from mappings.ndrrmc import (
    AFF_POP_COL_MAP, AGRI_MAPPING, ASSISTANCE_PROVIDED_MAPPING, CASUALTY_MAPPING, CLASS_MAPPING, COMMS_MAPPING, DOC, DOC_MAPPING,
    HOUSES_MAPPING, INCIDENT_COLUMN_MAPPINGS, INFRA_MAPPING, PEVAC_MAPPING,
    POWER_MAPPING, RNB_MAPPING, STRANDED_MAPPING, WATER_DIS_MAPPING, WATER_DISRUPTION, WORK_MAPPING,
    AffectedPopulation, Agriculture, Casualties, ClassDisruption, CommunicationLines, Event,
    Housing, Infrastructure, PEvacuation, Power, Provenance, Incident, Relief, RNB, Stranded, WorkDisruption
)

from transform.ndrrmc_cleaner import (
    concat_loc_levels, correct_QTY_Barangay_column, event_name_expander,
    forward_fill_and_collapse, normalize_datetime, remove_summary_rows,
    replace_column_whitespace_with_underscore, to_float, to_int, to_million_php
)

T = TypeVar("T")

def load_csv_df(
    path: str,
    *,
    mapping: dict[str, str] | None = None,
    target_cols: list[str] | None = None,
    collapse_on: str | None = None,
    collapse_key: str | None = None,
    replace_ws: bool = False,
    match_location: bool = True,
    schema_overrides: Mapping[str, pl.DataType] | None = None
) -> pl.DataFrame:
    df = pl.read_csv(path, schema_overrides=schema_overrides)
    df = correct_QTY_Barangay_column(df)

    if replace_ws:
        df = replace_column_whitespace_with_underscore(df)

    if mapping:
        df = df.rename(mapping=mapping, strict=False)

    if target_cols and collapse_on and collapse_key:
        df = forward_fill_and_collapse(df, target_cols, collapse_on, collapse_key)

    if match_location:
        locations = concat_loc_levels(df, ["City_Muni", "Province", "Region"], ",")
        df = df.with_columns(
            pl.Series("hasLocation", LOCATION_MATCHER.match(locations))
        )

    return df

def df_to_entities(df: pl.DataFrame, cls: Type[T]) -> list[T]:
    field_names = {f.name for f in fields(cls)}
    return [
        cls(**{k: row.get(k) for k in field_names})
        for row in df.iter_rows(named=True)
    ]

def load_multiple_csvs(
    folder: str,
    predicate: Callable[[str], bool],
    **df_kwargs,
) -> pl.DataFrame | None:
    paths = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if predicate(f)
    ]
    if not paths:
        return None

    dfs = [load_csv_df(p, **df_kwargs) for p in paths]
    return pl.concat(dfs, rechunk=True)

def load_uuids(folder_path: str):
    for folder in next(os.walk(folder_path))[1]:
        meta_path = os.path.join(folder_path, folder, "metadata.json")
        meta = {}

        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
            except json.JSONDecodeError:
                pass

        meta.setdefault("id", uuid.uuid4().hex)

        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)
        
def load_events(folder_path: str) -> list[Event]:
    events: list[Event] = []

    for folder in next(os.walk(folder_path))[1]:
        meta_path = os.path.join(folder_path, folder, "metadata.json")
        src_path = os.path.join(folder_path, folder, "source.json")

        if not os.path.exists(meta_path):
            continue

        with open(meta_path, "r", encoding="utf-8") as f:
            meta: dict[str, str] = json.load(f)

        with open(src_path, "r", encoding="utf-8") as f:
            src: dict[str, str] = json.load(f)

        event_name = event_name_expander(meta.get("eventName", folder))
        text = (meta.get("remarks") or "").split(". ")[0]
        pred, _ = DISASTER_CLASSIFIER.classify(
            [event_name + src.get("reportName", "") + text]
        )[0]

        events.append(Event(
            id=meta.get("id"),
            eventName=meta.get("eventName", folder),
            startDate=datetime.fromisoformat(meta["startDate"]) if meta.get("startDate") else None,
            endDate=datetime.fromisoformat(meta["endDate"]) if meta.get("endDate") else None,
            remarks=meta.get("remarks"),
            hasType=pred,
        ))

    return events


def load_provenance(event_folder_path: str) -> Provenance | None:
    src_path = os.path.join(event_folder_path, "source.json")
    if not os.path.exists(src_path):
        return None

    src = json.load(open(src_path))
    return Provenance(
        lastUpdateDate=datetime.fromisoformat(src["lastUpdateDate"]),
        reportLink=src.get("reportLink"),
        reportName=src.get("reportName", ""),
        obtainedDate=src.get("obtainedDate"),
    )

def load_aff_pop(event_folder_path: str) -> list[AffectedPopulation] | None:
    path = os.path.join(event_folder_path, "affected_population.csv")
    if not os.path.exists(path):
        return None

    df = load_csv_df(
        path,
        mapping=AFF_POP_COL_MAP,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="affectedBarangays",
    )

    df = to_int(df, [
        "affectedFamilies", "affectedBarangays",
        "affectedPersons", "displacedFamilies", "displacedPersons"
    ])

    df = df.with_row_index("id", 1)
    return df_to_entities(df, AffectedPopulation)

def load_infra(event_folder_path: str) -> list[Infrastructure] | None:
    path = os.path.join(event_folder_path, "damage_to_infrastructure.csv")
    if not os.path.exists(path):
        return None

    df = load_csv_df(
        path,
        mapping=INFRA_MAPPING,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="infraDamageType",
    )

    df = to_int(df, ["numberInfraDamaged"])
    df = to_million_php(df, ["infraDamageAmount"])
    df = df.with_row_index("id", 1)

    return df_to_entities(df, Infrastructure)

def load_relief(event_folder_path: str) -> list[Relief] | None:

    src_paths: list[str] = []
    for file in os.listdir(event_folder_path):
        if "assistance_provided" in file and file.endswith(".csv"):
            src_paths.append(os.path.join(event_folder_path, file))
    
    if len(src_paths) == 0: return None

    dfs: Iterable[pl.DataFrame] = []
    index = 1

    for src_path in src_paths:
        df = load_csv_df(
            src_path,
            mapping=ASSISTANCE_PROVIDED_MAPPING,
            target_cols=["Region", "Province", "City_Muni"],
            collapse_on="QTY",
            collapse_key="itemCost",
            match_location=True,
            schema_overrides={"QUANTITY": pl.Utf8()}
        )

        df = to_float(df, ["itemCost", "itemCostPerUnit", "itemQuantity"])

        df = df.with_row_index("id", index)
        index += len(df)
        dfs.append(df)
        
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

    return df_to_entities(final_df, Relief)

def load_casualties(event_folder_path: str) -> list[Casualties] | None:

    # Locate casualties CSV
    src_path = next(
        (
            os.path.join(event_folder_path, f)
            for f in os.listdir(event_folder_path)
            if "casualties" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path: return None

    df = load_csv_df(
        src_path,
        mapping=CASUALTY_MAPPING,
        target_cols=["Region", "Province", "City_Muni", "Summary_Type"],
        collapse_on="QTY",
        collapse_key="VALIDATED",
        match_location=True
    )

    # Normalize casualty type
    df = df.with_columns(
        pl.when(pl.col("Summary_Type").str.contains(r"(?i)injured"))
            .then(pl.lit("INJURED"))
            .otherwise(pl.col("Summary_Type"))
            .alias("casualtyType")
    )

    df = df.rename({
        "QTY": "casualtyCount"
    })

    # df.write_csv(event_folder_path + "hakdog.csv")

    df = df.with_row_index("id", 1)

    return df_to_entities(df, Casualties)


def load_incidents(event_folder_path: str) -> List[Incident] | None:
    src_path = os.path.join(event_folder_path, "related_incidents.csv")
    if not os.path.exists(src_path):
        return None

    # --- Load + normalize base table (includes location matching) ---
    df = load_csv_df(
        src_path,
        mapping=INCIDENT_COLUMN_MAPPINGS,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="hasOrigType",
        match_location=True,
    )

    # --- Event context for semantic enrichment ---
    meta_path = os.path.join(event_folder_path, "metadata.json")
    with open(meta_path, "r", encoding="utf-8") as f:
        meta: dict[str, str] = json.load(f)

    event_name = event_name_expander(meta.get("eventName", event_folder_path))

    df = df.with_columns(
        pl.lit(f"due to {event_name}").alias("event_name")
    )

    # classify the type of the disaster based on incident type text and description
    type_texts = (
        df.select(
            pl.concat_str(
                [
                    "hasOrigType",
                    "incidentStatus",
                    "incidentDescription",
                    "event_name",
                ],
                separator=" — ",
                ignore_nulls=True,
            )
        )
        .to_series()
        .to_list()
    )

    predictions = DISASTER_CLASSIFIER.classify(type_texts)
    df = df.with_columns(
        pl.Series("hasType", [pred for pred, _ in predictions])
    )

    # --- Datetime normalization ---
    df = normalize_datetime(
        df,
        "DATE_OF\nOCCURENCE",
        "TIME_OF\nOCCURENCE",
        "%d %B %Y %I:%M %P",
        "%d %B %Y",
        "startDate",
    )

    # copy startDate as endDate
    df = df.with_columns(
        (pl.col('startDate')).alias('endDate')
    )

    # --- Row index ---
    df = df.with_row_index("id", 1)

    # --- Materialize Incident entities ---
    return df_to_entities(df, Incident)


def load_housing(event_folder_path: str) -> List[Housing] | None:
    src_path = os.path.join(event_folder_path, "damaged_houses.csv")
    if not os.path.exists(src_path):
        return None

    df = load_csv_df(
        src_path,
        mapping=HOUSES_MAPPING,
        match_location=False,  # location handled after housing-specific cleanup
    )

    # custom fill
    df = df.with_columns(
        pl.col("Region").forward_fill(),
        pl.col("Province").forward_fill(),
    )

    # remove summary adm rows
    df = remove_summary_rows(df, nulls=["City_Muni", "hasBarangay"])

    # deduplicate entries
    df = df.unique(
        subset=[
            "City_Muni",
            "hasBarangay",
            "totallyDamagedHouses",
            "partiallyDamagedHouses",
            "housingDamageAmount",
        ],
        maintain_order=True,
    )

    # Forward fill city after deduplication
    df = df.with_columns(
        pl.col("City_Muni").forward_fill()
    )

    # Remove city-level summary rows
    df = df.filter(
        ~(
            pl.col("hasBarangay").is_null()
            & pl.col("City_Muni").is_duplicated()
        )
    )

    # Location matching (final, correct level)
    locations = concat_loc_levels(df, ["City_Muni", "Province", "Region"], ",")
    df = df.with_columns(
        pl.Series("hasLocation", LOCATION_MATCHER.match(locations))
    )

    df = to_int(df, ["totallyDamagedHouses", "partiallyDamagedHouses"])
    df = to_million_php(df, ["housingDamageAmount"])

    df = df.with_row_index("id", 1)

    df.write_csv(event_folder_path + "/hakdog.csv")

    return df_to_entities(df, Housing)

def load_agri(event_folder_path: str) -> List[Agriculture] | None:
    # Locate agriculture CSV
    src_path = next(
        (
            os.path.join(event_folder_path, f)
            for f in os.listdir(event_folder_path)
            if "agriculture" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path:
        return None

    df = load_csv_df(
        src_path,
        mapping=AGRI_MAPPING,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="agriDamageClassification",
        replace_ws=True,
        match_location=True,
    )

    # --- Type normalization ---
    df = to_int(
        df,
        [
            "partiallyDamagedInfrastructure",
            "totallyDamagedInfrastructure",
            "farmerFisherfolkAffected",
        ],
    )

    df = to_million_php(
        df,
        ["productionLossCost", 
         "agriDamageAmount"],
    )

    df = to_float(
        df,
        [
            "productionLossVolume",
            "partiallyDamagedCropArea",
            "totallyDamagedCropArea",
        ],
    )

    df = df.with_row_index("id", 1)

    return df_to_entities(df, Agriculture)

def load_pevac(event_folder_path: str) -> List[PEvacuation] | None:
    evac_path = os.path.join(event_folder_path, "pre-emptive_evacuation.csv")
    ap_path = os.path.join(event_folder_path, "cleaned_affected_population.csv")

    evac_exists = os.path.exists(evac_path)
    ap_exists = os.path.exists(ap_path)

    if not evac_exists and not ap_exists:
        return None

    evac_df: pl.DataFrame | None = None
    ap_df: pl.DataFrame | None = None

    # --- Pre-emptive evacuation source ---
    if evac_exists:
        evac_df = load_csv_df(
            evac_path,
            mapping=PEVAC_MAPPING,
            target_cols=["Region", "Province", "City_Muni"],
            collapse_on="QTY",
            collapse_key="preemptPersons",
            replace_ws=True,
            match_location=True,
        )

        evac_df = to_int(
            evac_df,
            ["preemptFamilies", "preemptPersons"],
        )

    # --- Affected population (evacuation centers only) ---
    if ap_exists:
        ap_df = (
            pl.read_csv(ap_path)
            .select(
                [
                    "hasLocation",
                    "hasBarangay",
                    "evacuationCenters",
                ]
            )
        )

        ap_df = to_int(ap_df, ["evacuationCenters"])

    # --- Merge logic ---
    if evac_df is None:
        df = ap_df.filter(pl.col("evacuationCenters") != 0)

    elif ap_df is None:
        df = evac_df

    else:
        df = evac_df.join(
            ap_df,
            on="hasLocation",
            how="full",
            coalesce=True,
        )

        # drop rows with no evac signal at all
        df = df.filter(
            ~(
                (pl.col("evacuationCenters") == 0)
                & pl.all_horizontal(
                    pl.col(["preemptFamilies", "preemptPersons"]).is_null()
                )
            )
        )

        # reconcile barangays (skip duplicates)
        df = df.with_columns(
            pl.when(pl.col("hasBarangay") == pl.col("hasBarangay_right"))
            .then(pl.col("hasBarangay"))
            .otherwise(
                pl.concat_str(
                    ["hasBarangay", "hasBarangay_right"],
                    ignore_nulls=True,
                )
            )
            .replace("", None)
            .alias("hasBarangay")
        )

    df = df.with_row_index("id", 1)
    return df_to_entities(df, PEvacuation)

def load_rnb(event_folder_path: str) -> List[RNB] | None:
    # Locate road and bridges CSV
    src_path = next(
        (
            os.path.join(event_folder_path, f)
            for f in os.listdir(event_folder_path)
            if "road" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path:
        return None

    df = load_csv_df(
        src_path,
        mapping=RNB_MAPPING,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="roadBridgeType",
        replace_ws=True,
        match_location=True,
    )

    # normalize passable datetime
    df = normalize_datetime(df, 
                            "passableDate", 
                            "passableTime", 
                            "%d %B %Y %I:%M %P", 
                            "%d %B %Y",
                            "passableDateTime")
    
    # normalize not passable datetime
    df = normalize_datetime(df, 
                            "notPassableDate", 
                            "notPassableTime", 
                            "%d %B %Y %I:%M %P", 
                            "%d %B %Y",
                            "notPassableDateTime")

    df = df.with_row_index("id", 1)

    return df_to_entities(df, RNB)

def load_power(event_folder_path: str) -> List[Power] | None:
    # Locate power CSV
    src_path = next(
        (
            os.path.join(event_folder_path, f)
            for f in os.listdir(event_folder_path)
            if "power" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path:
        return None

    df = load_csv_df(
        src_path,
        mapping=POWER_MAPPING,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="disruptionType",
        replace_ws=True,
        match_location=True,
    )

    # normalize interruption datetime
    df = normalize_datetime(df, 
                            "interruptionDate", 
                            "interruptionTime", 
                            "%d %B %Y %H:%M", 
                            "%d %B %Y",
                            "interruptionDateTime")
    
    # normalize restoration datetime
    df = normalize_datetime(df, 
                            "restorationDate", 
                            "restorationTime", 
                            "%d %B %Y %H:%M", 
                            "%d %B %Y",
                            "restorationDateTime")

    df = df.with_row_index("id", 1)

    return df_to_entities(df, Power)

def load_comms(event_folder_path: str) -> List[CommunicationLines] | None:
    # Locate power CSV
    src_path = next(
        (
            os.path.join(event_folder_path, f)
            for f in os.listdir(event_folder_path)
            if "communication" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path:
        return None

    df = load_csv_df(
        src_path,
        mapping=COMMS_MAPPING,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="Province",
        replace_ws=True,
        match_location=True,
    )

    # normalize interruption datetime
    df = normalize_datetime(df, 
                            "interruptionDate", 
                            "interruptionTime", 
                            "%B %d, %Y %H:%M", 
                            "%B %d, %Y",
                            "interruptionDateTime")
    
    # normalize restoration datetime
    df = normalize_datetime(df, 
                            "restorationDate", 
                            "restorationTime", 
                            "%B %d, %Y %H:%M", 
                            "%B %d, %Y",
                            "restorationDateTime")

    df = df.with_row_index("id", 1)

    return df_to_entities(df, CommunicationLines)

def load_docalamity(event_folder_path: str) -> List[DOC] | None:

    src_path = next(
        (
            os.path.join(event_folder_path, f)
            for f in os.listdir(event_folder_path)
            if "calamity" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path:
        return None

    df = load_csv_df(
        src_path,
        mapping=DOC_MAPPING,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="resolutionNo",
        match_location=True
    )

    df = normalize_datetime(
        df,
        date_col="resolutionDate",
        time_col=None,
        datetime_format="",
        date_format="%d %B %Y",
        new_col="resolutionDate"
    )

    df = df.with_row_index("id", 1)

    # df.write_csv(event_folder_path + "/hakdog.csv")

    return df_to_entities(df, DOC)

def load_class_suspension(event_folder_path: str) -> List[ClassDisruption] | None:

    src_path = next(
        (
            os.path.join(event_folder_path, f)
            for f in os.listdir(event_folder_path)
            if "class" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path:
        return None

    df = load_csv_df(
        src_path,
        mapping=CLASS_MAPPING,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="fromClassLevel",
        replace_ws=True,
        match_location=True
    )

    df = normalize_datetime(
        df,
        date_col="cancellationDate",
        time_col="cancellationTime",
        datetime_format="%d %B %Y %H:%M",
        date_format="%d %B %Y",
        new_col="cancellationDateTime"
    )

    df = normalize_datetime(
        df,
        date_col="resumptionDate",
        time_col="resumptionTime",
        datetime_format="%d %B %Y %H:%M",
        date_format="%d %B %Y",
        new_col="resumptionDateTime"
    )

    df = df.with_row_index("id", 1)

    # df.write_csv(event_folder_path + "/hakdog.csv")

    return df_to_entities(df, ClassDisruption)

def load_work_suspension(event_folder_path: str) -> List[WorkDisruption] | None:

    src_path = next(
        (
            os.path.join(event_folder_path, f)
            for f in os.listdir(event_folder_path)
            if "work" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path:
        return None

    df = load_csv_df(
        src_path,
        mapping=WORK_MAPPING,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="cancellationDate",
        replace_ws=True,
        match_location=True
    )

    df = normalize_datetime(
        df,
        date_col="cancellationDate",
        time_col="cancellationTime",
        datetime_format="%d %B %Y %H:%M",
        date_format="%d %B %Y",
        new_col="cancellationDateTime"
    )

    df = normalize_datetime(
        df,
        date_col="resumptionDate",
        time_col="resumptionTime",
        datetime_format="%d %B %Y %H:%M",
        date_format="%d %B %Y",
        new_col="resumptionDateTime"
    )

    df = df.with_row_index("id", 1)

    # df.write_csv(event_folder_path + "/hakdog.csv")

    return df_to_entities(df, WorkDisruption)

def load_stranded_events(event_folder_path: str) -> List[Stranded] | None:
    src_path = next(
        (
            os.path.join(event_folder_path, f)
            for f in os.listdir(event_folder_path)
            if "stranded" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path:
        return None

    df = load_csv_df(
        src_path,
        mapping=STRANDED_MAPPING,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="Province",
        replace_ws=True,
        match_location=True
    )

    df = to_int(df, ["passengers", 
                     "vessels", 
                     "motorBancas", 
                     "rollingCargoes"])

    df = df.with_row_index("id", 1)

    # df.write_csv(event_folder_path + "/hakdog.csv")

    return df_to_entities(df, Stranded)

def load_water(event_folder_path: str) -> List[WATER_DISRUPTION] | None:
    # Locate water supply CSV
    src_path = next(
        (
            os.path.join(event_folder_path, f)
            for f in os.listdir(event_folder_path)
            if "water" in f.lower() and f.endswith(".csv")
        ),
        None,
    )

    if not src_path:
        return None

    df = load_csv_df(
        src_path,
        mapping=WATER_DIS_MAPPING,
        target_cols=["Region", "Province", "City_Muni"],
        collapse_on="QTY",
        collapse_key="interruptionDate",
        replace_ws=True,
        match_location=True,
    )

    # normalize interruption datetime
    df = normalize_datetime(df, 
                            "interruptionDate", 
                            "interruptionTime", 
                            "%d %B %Y %I:%M %P", 
                            "%d %B %Y",
                            "interruptionDateTime")
    
    # normalize restoration datetime
    df = normalize_datetime(df, 
                            "restorationDate", 
                            "restorationTime", 
                            "%d %B %Y %I:%M %P", 
                            "%d %B %Y",
                            "restorationDateTime")

    df = df.with_row_index("id", 1)
    df.write_csv(event_folder_path + "/hakdog.csv")

    return df_to_entities(df, WATER_DISRUPTION)

if __name__ == "__main__":
    # load_aff_pop("../data/parsed/ndrrmc_mini/Combined Effects of  Enhanced SWM and TCs FERDIE GENER and HELEN IGME 2024")
    # load_stranded_events("../data/parsed/ndrrmc_mini/Combined Effects of  Enhanced SWM and TCs FERDIE GENER and HELEN IGME 2024")

    load_relief("../data/parsed/ndrrmc_mini/SWM enhanced by TCs EGAY and FALCON 2023")

    # load_housing("../data/parsed/ndrrmc_mini/Magnitude 6 8 Earthquake in Sarangani Davao Occidental/")
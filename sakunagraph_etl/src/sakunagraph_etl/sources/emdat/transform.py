# EMDAT XLSX MAPPER HERE
import argparse
import os
import uuid
from dataclasses import dataclass, fields
from pathlib import Path
import polars as pl
from polars import DataFrame
import datetime
from sakunagraph_etl.rdf.iris import EMDAT_EVENT_NS
from sakunagraph_etl.transform.helpers import to_float, to_int
from .rdf import Assistance, Event, Recovery, Source, DamageGeneral, Casualties, AffectedPopulation
from sakunagraph_etl.enrichment.locations import canonicalize_column
from typing import Any, Callable, TypeAlias

from .parse import ParsedEmdatWorkbook, parse_workbook

EmdatEntity: TypeAlias = Event | Assistance | Recovery | DamageGeneral | Casualties | AffectedPopulation
EmdatEntityMap: TypeAlias = dict[type[EmdatEntity], list[EmdatEntity]]
LocationCanonicalizer: TypeAlias = Callable[..., DataFrame]


@dataclass(frozen=True, slots=True)
class EmdatTransformResult:
    """Explicit contract between EM-DAT transformation and RDF mapping."""

    workbook: ParsedEmdatWorkbook
    source: Source
    normalized_rows: DataFrame
    entities: EmdatEntityMap

    @property
    def entity_count(self) -> int:
        return sum(len(rows) for rows in self.entities.values())


class EmdatTransformer:
    """Normalize parsed rows with an injectable location resolver."""

    def __init__(self, location_canonicalizer: LocationCanonicalizer = canonicalize_column) -> None:
        self.location_canonicalizer = location_canonicalizer

    def transform(
        self,
        workbook: ParsedEmdatWorkbook,
        *,
        debug_dir: str | Path | None = None,
    ) -> EmdatTransformResult:
        df = workbook.rows.rename(mapping=COLUMN_MAPPINGS)
        df = _clean_columns(df, canonicalizer=self.location_canonicalizer)
        df = df.with_columns(pl.col("id").map_elements(_event_id, return_dtype=pl.String))
        df = df.with_row_index("rowNumber", 1)
        entities = _entities_from_frame(df)

        if debug_dir is not None:
            debug_path = Path(debug_dir) / "emdat" / "normalized.csv"
            debug_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_csv(debug_path)

        return EmdatTransformResult(
            workbook=workbook,
            source=load_source(workbook.source_path),
            normalized_rows=df,
            entities=entities,
        )

EMDAT_SUBTYPE_TO_URI = {

    # ── Epidemic ──────────────────────────────────────────────────────────────
    "bacterial disease":                "BacterialDisease",
    "viral disease":                    "ViralDisease",
    "fungal disease":                   "FungalDisease",
    "parasitic disease":                "ParasiticDisease",
    "infectious disease (general)":     "InfectiousDiseaseGeneral",
    "prion disease":     "PrionDisease",

    "grasshopper infestation": "GrashopperInfestation",
    "infestation (general)": "InfestationGeneral",
    "locust infestation": "LocustInfestation",
    "worms infestation": "WormsInfestation",

    "drought": "Drought",
    "glacial": "Glacial",
    "forest fire": "ForestFire",
    "land fire": "LandFire",
    "wildfire (general)": "WildfireGeneral",

    "space impact": "SpaceImpact",
    "space weather": "SpaceWeather",


    # ── Flood ─────────────────────────────────────────────────────────────────
    "flash flood":                      "FlashFlood",
    "riverine flood":                   "RiverineFlood",
    "coastal flood":                    "CoastalFlood",
    "flood (general)":                  "FloodGeneral",
    "ice jam flood": "IceJamFlood",
    
    "rogue wave": "RogueWave",
    "seiche": "Seiche",


    # ── Storm ─────────────────────────────────────────────────────────────────
    "tropical cyclone":                 "TropicalCyclone",
    "tornado":                          "Tornado",
    "storm surge":                      "StormSurge",
    "severe weather":                   "SevereWeather",
    "storm (general)":                  "StormGeneral",
    "blizzard strom": "BlizzardStorm",
    "derecho": "Derecho",
    "extra-tropical storm": "ExtratropicalStorm",
    "hail": "Hail",
    "sand storm": "Sand Storm",
    "thunderstorms": "Thunderstorms",



    # ── Earthquake ────────────────────────────────────────────────────────────
    "ground movement":                  "GroundMovement",
    "tsunami":                          "Tsunami",

    # ── Volcanic Activity ─────────────────────────────────────────────────────
    "ash fall":                         "Ashfall",
    "lahar":                            "Lahar",
    "lava flow":                        "LavaFlow",
    "pyroclastic flow":                        "PyroclasticFlow",
    "volcanic activity (general)":      "VolcanicActivityGeneral",

    # ── Mass Movement (Wet) ───────────────────────────────────────────────────
    "avalanche (wet)":                  "AvalancheWet",
    "landslide (wet)":                  "LandslideWet",
    "sudden subsidence (wet)":          "SuddenSubsidenceWet",
    "mudslide": "Mudslide",
    "rockfall (wet)": "RockfallWet",


    # ── Mass Movement (Dry) ───────────────────────────────────────────────────
    "landslide (dry)":                  "LandslideDry",
    "rockfall (dry)":                   "RockfallDry",
    "avalanche (dry)": "AvalancheDry",
    "sudden subsidence (dry)": "SuddenSubsidenceDry",

    # ── Extreme Temperature ───────────────────────────────────────────────────
    "heat wave":                        "HeatWave",
    "cold wave": "ColdWave",
    "severe winter conditions": "SevereWinterConditions",

    "fog": "Fog",


    # ── Industrial / Technological Accidents ──────────────────────────────────
    "chemical spill":                   "ChemicalSpill",
    "oil spill":                        "OilSpill",
    "poisoning":                        "Poisoning",
    "gas leak": "GasLeak",
    "industrial accident (general)": "IndustrialAccidentGeneral",
    "radiation": "Radiation",
    "explosion (industrial)":           "ExplosionIndustrial",
    "explosion (miscellaneous)":        "ExplosionMiscellaneous",
    "fire (industrial)":                "FireIndustrial",
    "fire (miscellaneous)":             "FireMiscellaneous",
    "collapse (industrial)":            "CollapseIndustrial",
    "collapse (miscellaneous)":         "CollapseMiscellaneous",
    "miscellaneous accident (general)": "MiscellaneousAccidentGeneral",

    # ── Transport Accidents ───────────────────────────────────────────────────
    "air":                              "Air",
    "rail":                             "Rail",
    "road":                             "Road",
    "water":                            "Water",

    "tsunami/tidal wave": "Tsunami",
    "transport accident": "Transport",
    "flood": "Flood",
    "lightening": "Thunderstorms",
    "earthquake": "Earthquake",
    "storm": "Storm"

}

COLUMN_MAPPINGS = {
    "AID Contribution ('000 US$)": "contributionAID",
    "OFDA/BHA Response": "internationalOrgsPresent",
    "Magnitude": "hasMagnitude",
    "Magnitude Scale": "hasMagnitudeScale",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Total Deaths": "dead",
    "No. Injured": "injured",
    "No. Affected": "affectedPersons",
    "No. Homeless": "displacedPersons",
    "Reconstruction Costs, Adjusted ('000 US$)": "postStructureCost",
    "Insured Damage, Adjusted ('000 US$)": "insuredDamage",
    "Total Damage, Adjusted ('000 US$)":
    "generalDamageAmount",
    "CPI": "cpi",
    "Entry Date": "entryDate",
    "Last Update": "lastUpdateDate",
    "DisNo.": "id",
    "Event Name": "eventName",
    "Location": "hasLocation"
}

def _event_id(disno: str) -> str:
    """Deterministic hex ID from DisNo."""
    return uuid.uuid5(EMDAT_EVENT_NS, disno).hex

def _clean_loc(df: DataFrame, col: str):

    df = df.with_columns(
        pl.col(col)
            # strip noise words and artifacts
            .str.replace_all(r"\(\d+\)\s|(?i)provinces*|(?i)is\.|(?i)isl\.|\)|regions*\s*|municipality|municipalities|(?i)near|(?i)Mt\.|\bkm\b|\btown\b|(?i)island|(?i)area|(?i)between|districts*|strait|\s*\-\s*[A-Za-z]*", "")
            .str.strip_chars()
            # separators → |
            .str.replace_all(r"\s\(|\band\b", ",")
            .str.replace_all(r"(?i)cty", "City")
            .str.replace_all(r",|;", "|")
            # spelling fixes
            .str.replace_all(r"Manilla|Manille", "Manila")
            # geographic normalizations
            .str.replace_all(r"(?i)panay", "Western Visayas")
            .str.replace_all(r"(?i)nationwide|(?i)all country", "Philippines")
            .str.replace_all(r"(?i)\bCentral\b|(?i)visayan", "Visayas")
            # abbreviation expansions
            .str.replace_all(r"(?i)W\.", "")
            .str.replace_all(r"(?i)E\.", "")
            .str.replace_all(r"(?i)N\.", "")
            .str.replace_all(r"(?i)S\.", "")
            .str.replace_all(r"(?i)\bCtr\.\b", "Central")
            .str.replace_all(r"West Luzon|Luzon*", "Luzon")
            .str.replace_all(r"Visayas\sLuzon", "Visayas|Luzon")
            .str.replace_all("city", "City")
            .str.replace_all("Bagio", "Baguio")
    )

    return df

def _normalize_date(df: DataFrame, start_cols: list[str], end_cols: list[str]):
    """
    If start and end day is empty, fall back to 01 - 31
    """
    def parse_date(cols: list[str], alias: str) -> pl.Expr:
        is_end = alias == "endDate"

        combined = (
            pl.concat_str(cols, separator="-", ignore_nulls=True)
            .str.strip_chars_end("-")
        )

        is_year_only    = combined.str.len_chars() == 4
        is_year_month   = combined.str.len_chars() == 7 
        is_partial      = is_year_only | is_year_month   # <-- capture BEFORE padding

        year_month_day = (
            pl.when(is_year_month)
            .then(combined + "-01")
            .when(is_year_only)
            .then(combined + ("-12-01" if is_end else "-01-01"))
            .otherwise(combined)
        )

        parsed = year_month_day.str.to_date("%Y-%m-%d", strict=True)

        if is_end:
            parsed = (
                pl.when(is_partial)        # use the pre-computed flag, not a re-derived length
                .then(parsed.dt.month_end())
                .otherwise(parsed)
            )

        return parsed.alias(alias)

    df = df.with_columns(
        parse_date(start_cols, "startDate"),
        parse_date(end_cols, "endDate"),
    )

    return df

def _clean_columns(
    df: DataFrame,
    *,
    canonicalizer: LocationCanonicalizer = canonicalize_column,
) -> DataFrame:

    # Strip leading/trailing whitespace from string columns
    df = df.with_columns(
        [pl.col(c).str.strip_chars() for c in df.columns if df[c].dtype == pl.Utf8]
    )


    # match disaster subtype (most granular) to ontology iri
    df = df.with_columns(
        hasDisasterType=pl.col("Disaster Subtype").str.to_lowercase().map_elements(
            lambda s: EMDAT_SUBTYPE_TO_URI.get(s),
            return_dtype=pl.String
        )
    )


    # match associated types as disaster subtype
    df = df.with_columns(
        hasDisasterSubtype=pl.col("Associated Types").map_elements(
            lambda s: "|".join(filter(None, [
                EMDAT_SUBTYPE_TO_URI.get(part.strip().lower())
                for part in s.split("|")
            ])) if s is not None else None,
            return_dtype=pl.String
        )
    )

    # fill empty location values with Philippines
    df = df.with_columns(
        pl.col('hasLocation')
        .replace("", None)
        .fill_null(pl.lit("Philippines"))
    )


    df = _clean_loc(df, "hasLocation")

    canon_loc_df = canonicalizer(
        df=df.select("hasLocation"),
        col="hasLocation",
        threshold=80
    )

    # add canonicalized loc values
    df = df.with_columns(
        hasLocation=canon_loc_df.select("hasLocation_iri").to_series()
    )

    df = df.with_columns(
        pl.col("Start Day").replace("", None),
        pl.col("End Day").replace("", None)
    )

    df = df.with_columns(
         pl.when(pl.col("Start Month").is_not_null() & (pl.col("Start Month") != ""))
        .then(pl.col("Start Month").str.zfill(2))
        .otherwise(None)
    )

    df = df.with_columns(
         pl.when(pl.col("End Month").is_not_null() & (pl.col("End Month") != ""))
        .then(pl.col("End Month").str.zfill(2))
        .otherwise(None)
    )

    # normalize start and end dates
    df = _normalize_date(df, ["Start Year", "Start Month", "Start Day"], ["End Year", "End Month", "End Day"])
    
    # normalize entry and last update dates
    df = df.with_columns(
        pl.col('entryDate').str.to_date("%Y-%m-%d").alias('entryDate'),
        pl.col('lastUpdateDate').str.to_date("%Y-%m-%d").alias('lastUpdateDate'),

    )

    df = to_float(df, ["contributionAID", "postStructureCost", "generalDamageAmount", "cpi", "hasMagnitude", "latitude", "longitude"])

    df = to_int(df, ["dead", "injured", "affectedPersons", "displacedPersons"])


    return df


def _entities_from_frame(df: DataFrame) -> EmdatEntityMap:
    clss: list[type[EmdatEntity]] = [
        Event,
        Assistance,
        Recovery,
        DamageGeneral,
        Casualties,
        AffectedPopulation,
    ]
    entities: EmdatEntityMap = {cls: [] for cls in clss}

    for row in df.to_dicts():
    
        for cls in clss:
            data: dict[str, Any] = {}
            class_fields = fields(cls)

            for f in class_fields:
                value = row.get(f.name, None)


                if value is None or (isinstance(value, str) and value.strip().lower() == "none"):             
                    data[f.name] = None

                # do not propagate
                elif f.name == "hasLocation" and "|" in str(value) and cls != Event:
                    data[f.name] = None
                else:
                    data[f.name] = value 

            # Do not map impact/response rows whose sole value is a location.
            if any(v is not None and v != "" for k, v in data.items() if k != "id" and k != "hasLocation"):
                entities[cls].append(cls(**data))
    return entities


def transform(
    workbook: ParsedEmdatWorkbook,
    *,
    debug_dir: str | Path | None = None,
    transformer: EmdatTransformer | None = None,
) -> EmdatTransformResult:
    """Transform a parsed workbook into the typed RDF-mapping contract."""

    return (transformer or EmdatTransformer()).transform(workbook, debug_dir=debug_dir)


def transform_emdat(
    input_path: str | Path,
    *,
    debug_dir: str | Path | None = None,
) -> EmdatEntityMap:
    """Legacy convenience API returning the historical entity dictionary."""

    return transform(parse_workbook(input_path), debug_dir=debug_dir).entities

def load_source(path: str | Path) -> Source:

    name, ext = os.path.splitext(os.path.basename(path))

    ext = ext.replace(".", "")

    mod_timestamp = os.path.getmtime(path)
    mod_date = datetime.datetime.fromtimestamp(mod_timestamp)

    return Source(
        format=ext,
        reportName=name,
        obtainedDate=mod_date,
    )                



def main() -> int:
    parser = argparse.ArgumentParser(
        description="Transform EM-DAT XLSX to SakunaGraPH-conformant RDF."
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to the EM-DAT .xlsx file.",
    )
    parser.add_argument("--debug-dir", type=Path, help="Optional diagnostic CSV root.")
    args = parser.parse_args()
    transform_emdat(args.input, debug_dir=args.debug_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AffectedPopulation",
    "Assistance",
    "COLUMN_MAPPINGS",
    "Casualties",
    "DamageGeneral",
    "EMDAT_SUBTYPE_TO_URI",
    "EmdatEntityMap",
    "EmdatTransformResult",
    "EmdatTransformer",
    "Event",
    "Recovery",
    "Source",
    "load_source",
    "main",
    "transform",
    "transform_emdat",
]

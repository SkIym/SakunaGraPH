# EMDAT XLSX MAPPER HERE
import argparse
import re
import unicodedata
from datetime import datetime, date
from pathlib import Path
from typing import Any, List

import polars as pl
from polars import DataFrame

from semantic_processing.location_matcher_single import canonicalize_column

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

}

def clean_loc(df: DataFrame, col: str):

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

def load_emdat(path: str | Path) -> pl.DataFrame:
    """
    Load the EM-DAT Data sheet with Polars.

    The file uses a header row; all columns are read as strings initially
    so we can apply our own type coercions in transform_row().
    """
    df = pl.read_excel(
        source=str(path),
        sheet_name="EM-DAT Data",
        infer_schema_length=0,
    )
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

    # fill empty location values with Philippines
    df = df.with_columns(
        pl.col('Location')
        .replace("", None)
        .fill_null(pl.lit("Philippines"))
    )


    df = clean_loc(df, "Location")

    canon_loc_df = canonicalize_column(
        df=df.select("Location"),
        col="Location",
        threshold=80
    )

    # add canonicalized loc values
    df = df.with_columns(
        hasLocation=canon_loc_df.select("Location_iri").to_series()
    )
    

    df.write_csv("sam.csv")
    canon_loc_df.write_csv("sam_loc.csv")
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Transform EM-DAT XLSX to SakunaGraPH-conformant RDF."
    )
    parser.add_argument(
        "--input", "-i",
        default="../data/raw/static/public_emdat_custom_request_2026-01-04_8433220b-a682-4ad5-89e9-53ef8205b03e.xlsx",
        help="Path to the EM-DAT .xlsx file.",
    )
    args = parser.parse_args()
    load_emdat(args.input)


if __name__ == "__main__":
    main()
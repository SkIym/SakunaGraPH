# EMDAT XLSX MAPPER HERE
import argparse
import re
import unicodedata
from datetime import datetime, date
from pathlib import Path
from typing import Any, List

import polars as pl
from polars import DataFrame

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

    

    df.write_csv("sam.csv")
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
"""
PSGC to RDF Converter
=====================
Converts the Philippine Standard Geographic Code (PSGC) 4Q-2025 Excel datafile
into RDF/Turtle format using Polars for data wrangling and rdflib for graph construction.

Output is designed for consumption by LocationMatcher (location_matcher.py):
  - All individual IRIs use the sakuna.ph base: https://sakuna.ph/<psgc_code>
  - Region IRIs use the exact slugs expected by the matcher's region_map
  - rdfs:label holds the plain location name (used directly as a match key)
  - sakuna:isPartOf links each node to its parent

Features:
  - Maps all 11 source columns to ontology properties
  - Handles isPartOf hierarchy (Bgy → City/Mun/SubMun → Prov → Reg)
  - Correct PSGC segmentation: RR-PPP-MM-BBB (not RR-PP-MMM-BBB)
  - HUC/Pateros fallback: province-less nodes link directly to their region
  - Emits OWL class assertions for all six geographic levels

Requirements:
  pip install polars rdflib openpyxl

Usage:
  python psgc_to_rdf.py
  python psgc_to_rdf.py --input data.xlsx --output psgc.ttl
"""

import re
import argparse
from pathlib import Path

import polars as pl
from rdflib import Graph, Namespace, URIRef, Literal, RDF, RDFS, OWL, XSD
from rdflib.namespace import DCTERMS, SKOS


# ─────────────────────────────────────────────────────────────────────────────
# Namespaces
# ─────────────────────────────────────────────────────────────────────────────

SKG    = Namespace("https://sakuna.ph/")
GEO    = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
SCHEMA = Namespace("https://schema.org/")


# ─────────────────────────────────────────────────────────────────────────────
# Region IRI map
#   key   : 10-digit PSGC code (zero-padded)
#   value : URI slug appended to SKG base
#
# These slugs must exactly match the values in LocationMatcher.region_map so
# that match_region() resolves to the same IRI that the graph contains.
# ─────────────────────────────────────────────────────────────────────────────

REGION_SLUGS: dict[str, str] = {
    "0100000000": "Region_I",
    "0200000000": "Region_II",
    "0300000000": "Region_III",
    "0400000000": "Region_IV-A",
    "1700000000": "Region_IV-B",        # MIMAROPA — separated from IV-A
    "0500000000": "Region_V",
    "0600000000": "Region_VI",
    "1800000000": "Negros_Island_Region",
    "0700000000": "Region_VII",
    "0800000000": "Region_VIII",
    "0900000000": "Region_IX",
    "1000000000": "Region_X",
    "1100000000": "Region_XI",
    "1200000000": "Region_XII",
    "1300000000": "National_Capital_Region",
    "1400000000": "Cordillera_Administrative_Region",
    "1600000000": "Region_XIII",
    "1900000000": "Bangsamoro_Autonomous_Region_In_Muslim_Mindanao",
}


# ─────────────────────────────────────────────────────────────────────────────
# Level → OWL class  /  human-readable label
# ─────────────────────────────────────────────────────────────────────────────

LEVEL_CLASS: dict[str, URIRef] = {
    "Reg":    SKG["Region"],
    "Prov":   SKG["Province"],
    "City":   SKG["City"],
    "Mun":    SKG["Municipality"],
    "SubMun": SKG["SubMunicipality"],
    "Bgy":    SKG["Barangay"],
}

LEVEL_LABEL: dict[str, str] = {
    "Reg":    "Region",
    "Prov":   "Province",
    "City":   "City",
    "Mun":    "Municipality",
    "SubMun": "Sub-Municipality",
    "Bgy":    "Barangay",
}


# ─────────────────────────────────────────────────────────────────────────────
# Individual URI
#
# Regions use their human-readable slug so match_region() comparisons work.
# All other levels use the numeric PSGC code — unique and stable.
# ─────────────────────────────────────────────────────────────────────────────

def location_uri(code: str, level: str) -> URIRef:
    # if level == "Reg":
    #     slug = REGION_SLUGS.get(code.zfill(10))
    #     if slug:
    #         return SKG[slug]
    return SKG[code.zfill(10)]


# ─────────────────────────────────────────────────────────────────────────────
# PSGC 10-digit code structure — CORRECT segmentation
#
#   RR PPP MM BBB
#   RR  = region    (pos 0–1,  2 digits)
#   PPP = province  (pos 2–4,  3 digits)
#   MM  = mun/city  (pos 5–6,  2 digits)
#   BBB = barangay  (pos 7–9,  3 digits)
#
# Confirmed:
#   Abra province  1400100000  (RR=14, PPP=001, MM=00, BBB=000)
#   Bangued mun    1400101000  (RR=14, PPP=001, MM=01, BBB=000)
#   parent(Bangued) = 14+001+00000 = 1400100000  ✓
#
# Special cases:
#   HUCs in NCR — code[:5]+'00000' resolves to the city itself (self-ref);
#                 fall back to the region code.
#   Pateros      — computed parent 1381700000 does not exist in the data;
#                 fall back to the region code.
# ─────────────────────────────────────────────────────────────────────────────

def parent_code(code: str, level: str, all_codes: set[str]) -> str | None:
    c = code.zfill(10)
    match level:
        case "Bgy":
            return c[:7] + "000"
        case "SubMun":
            return c[:5] + "00000"
        case "City" | "Mun":
            candidate = c[:5] + "00000"
            if candidate == c or candidate not in all_codes:
                return c[:2] + "00000000"   # HUC / orphan mun → region
            return candidate
        case "Prov":
            return c[:2] + "00000000"
        case "Reg":
            return None
        case _:
            return None


# ─────────────────────────────────────────────────────────────────────────────
# Excel → Polars loader
# ─────────────────────────────────────────────────────────────────────────────

SHEET_NAME = "PSGC"

COLUMN_RENAME: dict[str, str] = {
    "10-digit PSGC":                                 "psgc_code",
    "Name":                                          "name",
    "Correspondence Code":                           "correspondence_code",
    "Geographic Level":                              "geo_level",
    "Old names":                                     "old_names",
    "City Class":                                    "city_class",
    "Income\nClassification (DOF DO No. 074.2024)": "income_class",
    "Urban / Rural\n(based on 2020 CPH)":           "urban_rural",
    "2024 Population":                               "population_2024",
    "__col9__":                                      "population_note",  # no header in source
    "Status":                                        "status",
}


def load_dataframe(xlsx_path: Path) -> pl.DataFrame:
    df = pl.read_excel(xlsx_path, sheet_name=SHEET_NAME, engine="openpyxl")

    # The 10th column has no header; give it a stable placeholder
    raw_cols = df.columns
    for i, c in enumerate(raw_cols):
        if c.strip() == "":
            df = df.rename({raw_cols[i]: "__col9__"})
            break

    rename_map = {k: v for k, v in COLUMN_RENAME.items() if k in df.columns}
    df = df.rename(rename_map)

    df = df.with_columns(
        pl.col("psgc_code").cast(pl.Utf8).str.zfill(10).alias("psgc_code")
    )

    valid_levels = set(LEVEL_CLASS.keys())
    df = df.filter(pl.col("geo_level").is_in(list(valid_levels)))

    for col in ["name", "old_names", "city_class", "income_class",
                "urban_rural", "status", "population_note"]:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Utf8).str.strip_chars().alias(col)
            )

    for col in df.columns:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.when(pl.col(col).str.len_chars() == 0)
                  .then(None)
                  .otherwise(pl.col(col))
                  .alias(col)
            )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Ontology declaration (TBox)
# ─────────────────────────────────────────────────────────────────────────────

def build_tbox(g: Graph) -> None:
    for level, cls in LEVEL_CLASS.items():
        g.add((cls, RDF.type,        OWL.Class))
        g.add((cls, RDFS.label,      Literal(LEVEL_LABEL[level], lang="en")))
        g.add((cls, RDFS.subClassOf, SKG["AdministrativeUnit"]))

    g.add((SKG["AdministrativeUnit"], RDF.type,   OWL.Class))
    g.add((SKG["AdministrativeUnit"], RDFS.label, Literal("Administrative Unit", lang="en")))

    g.add((SKG["isPartOf"], RDF.type,              OWL.ObjectProperty))
    g.add((SKG["isPartOf"], RDFS.label,            Literal("Is Part Of", lang="en")))
    g.add((SKG["isPartOf"], RDFS.subPropertyOf,    DCTERMS.isPartOf))

    dt_props = [
        (SKG["psgcCode"],            "PSGC Code",              XSD.string,  "10-digit Philippine Standard Geographic Code"),
        (SKG["correspondenceCode"],  "Correspondence Code",    XSD.string,  "Legacy correspondence/old PSGC code"),
        (SKG["geographicLevel"],     "Geographic Level",       XSD.string,  "Level abbreviation: Reg, Prov, City, Mun, SubMun, Bgy"),
        (SKG["cityClass"],           "City Class",             XSD.string,  "HUC, ICC, or CC"),
        (SKG["incomeClassification"],"Income Classification",  XSD.string,  "DOF income classification (1st–6th)"),
        (SKG["urbanRural"],          "Urban/Rural",            XSD.string,  "U = Urban, R = Rural (2020 CPH)"),
        (SKG["population2024"],      "2024 Population",        XSD.integer, "Population count from 2024 census"),
        (SKG["populationNote"],      "Population Note",        XSD.string,  "Qualification note on population figure"),
        (SKG["status"],              "Status",                 XSD.string,  "Capital or Pob. (Poblacion)"),
    ]
    for prop, label, dtype, comment in dt_props:
        g.add((prop, RDF.type,     OWL.DatatypeProperty))
        g.add((prop, RDFS.label,   Literal(label, lang="en")))
        g.add((prop, RDFS.comment, Literal(comment, lang="en")))
        g.add((prop, RDFS.range,   dtype))


# ─────────────────────────────────────────────────────────────────────────────
# ABox population
# ─────────────────────────────────────────────────────────────────────────────

def build_abox(g: Graph, df: pl.DataFrame, include_barangay: bool = True) -> tuple[int, int]:
    """
    Add one RDF individual per PSGC row plus isPartOf triples.
    Returns (individuals_added, isPartOf_triples_added).
    """

    # Pre-build code → URI lookup for parent resolution
    code_to_uri: dict[str, URIRef] = {
        row["psgc_code"]: location_uri(row["psgc_code"], row["geo_level"])
        for row in df.iter_rows(named=True)
    }
    all_codes: set[str] = set(code_to_uri.keys())

    ind_count  = 0
    part_count = 0

    for row in df.iter_rows(named=True):
        code  = row["psgc_code"]
        level = row.get("geo_level")

        if level is None:
            continue

        if level == "Bgy" and not include_barangay:
            continue

        name  = (row.get("name") or "").strip()

        uri = location_uri(code, level)
        cls = LEVEL_CLASS.get(level, SKG["Location"])

        # ── Type ─────────────────────────────────────────────────────────────
        g.add((uri, RDF.type, cls))
        g.add((uri, RDF.type, OWL.NamedIndividual))

        # ── rdfs:label — plain name, used directly as a match key ────────────
        if name:
            g.add((uri, RDFS.label, Literal(name, lang="en")))

        # ── PSGC code ────────────────────────────────────────────────────────
        g.add((uri, SKG["psgcCode"], Literal(code, datatype=XSD.string)))

        # ── Correspondence code ───────────────────────────────────────────────
        cc = row.get("correspondence_code")
        if cc is not None:
            g.add((uri, SKG["correspondenceCode"],
                   Literal(str(cc).strip(), datatype=XSD.string)))

        # ── Geographic level ─────────────────────────────────────────────────
        if level:
            g.add((uri, SKG["geographicLevel"],
                   Literal(level, datatype=XSD.string)))

        # ── Old names → skos:altLabel ─────────────────────────────────────────
        old = row.get("old_names")
        if old:
            for alt in (n.strip() for n in old.split(",") if n.strip()):
                g.add((uri, SKOS.altLabel, Literal(alt, lang="fil")))

        # ── City class ────────────────────────────────────────────────────────
        city_class = row.get("city_class")
        if city_class:
            g.add((uri, SKG["cityClass"], Literal(city_class, datatype=XSD.string)))

        # ── Income classification ─────────────────────────────────────────────
        inc = row.get("income_class")
        if inc:
            g.add((uri, SKG["incomeClassification"], Literal(inc, datatype=XSD.string)))

        # ── Urban / Rural ─────────────────────────────────────────────────────
        ur = row.get("urban_rural")
        if ur:
            g.add((uri, SKG["urbanRural"], Literal(ur, datatype=XSD.string)))

        # ── 2024 Population ───────────────────────────────────────────────────
        pop = row.get("population_2024")
        if pop is not None:
            try:
                g.add((uri, SKG["population2024"],
                       Literal(int(pop), datatype=XSD.integer)))
            except (ValueError, TypeError):
                pass

        # ── Population note ───────────────────────────────────────────────────
        pnote = row.get("population_note")
        if pnote:
            g.add((uri, SKG["populationNote"], Literal(pnote, datatype=XSD.string)))

        # ── Status ────────────────────────────────────────────────────────────
        status = row.get("status")
        if status:
            g.add((uri, SKG["status"], Literal(status, datatype=XSD.string)))

        ind_count += 1

        # ── isPartOf ─────────────────────────────────────────────────────────
        par_code = parent_code(code, level, all_codes)
        if par_code and par_code in code_to_uri:
            g.add((uri, SKG["isPartOf"], code_to_uri[par_code]))
            part_count += 1

    return ind_count, part_count


# ─────────────────────────────────────────────────────────────────────────────
# Graph init
# ─────────────────────────────────────────────────────────────────────────────

def init_graph() -> Graph:
    g = Graph()
    g.bind("",      SKG)
    g.bind("geo",   GEO)
    g.bind("skos",  SKOS)
    g.bind("owl",   OWL)
    g.bind("rdfs",  RDFS)
    g.bind("xsd",   XSD)
    g.bind("dct",   DCTERMS)
    g.bind("schema", SCHEMA)
    return g


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert PSGC Excel datafile to RDF/Turtle"
    )
    parser.add_argument("--input",  "-i", default="../data/raw/PSGC-4Q-2025-Publication-Datafile.xlsx")
    parser.add_argument("--output", "-o", default="psgc.ttl")
    parser.add_argument("--format", "-f", default="turtle",
                        choices=["turtle", "xml", "n3", "nt", "json-ld"])
    parser.add_argument("--barangay", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    xlsx_path = Path(args.input)
    out_path  = Path(args.output)

    print(f"[1/4] Loading {xlsx_path} …")
    df = load_dataframe(xlsx_path)
    print(f"      {len(df):,} rows across {df['geo_level'].n_unique()} levels: "
          f"{sorted(df['geo_level'].unique().to_list())}")

    dupes = (
        df.group_by(["name", "geo_level"])
          .agg(pl.count("psgc_code").alias("count"))
          .filter(pl.col("count") > 1)
          .sort("count", descending=True)
    )
    print(f"\n[2/4] Duplicate name check: {len(dupes):,} (name, level) pairs appear more than once.")
    if len(dupes):
        print("      Top duplicates (IRIs are unique via PSGC code; labels are shared plain names):")
        for r in dupes.head(5).iter_rows(named=True):
            print(f"        '{r['name']}' [{r['geo_level']}] × {r['count']}")

    print(f"\n[3/4] Building RDF graph …")
    g = init_graph()
    # build_tbox(g)
    ind_n, part_n = build_abox(g, df, args.barangay)
    print(f"      {ind_n:,} individuals | {part_n:,} isPartOf triples | {len(g):,} total triples")

    print(f"\n[4/4] Serialising → {out_path} ({args.format}) …")
    g.serialize(destination=str(out_path), format=args.format)
    print(f"      Done — {out_path.stat().st_size / 1024:,.1f} KB")

    print("""
Example SPARQL:

  PREFIX skg: <https://sakuna.ph/>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

  # All municipalities in Abra
  SELECT ?mun ?name WHERE {
    ?mun a skg:Municipality ;
         rdfs:label ?name ;
         skg:isPartOf skg:1400100000 .
  }

  # Locations with population > 1 million
  SELECT ?loc ?name ?pop WHERE {
    ?loc skg:population2024 ?pop ;
         rdfs:label ?name .
    FILTER(?pop > 1000000)
  } ORDER BY DESC(?pop)
""")


if __name__ == "__main__":
    main()
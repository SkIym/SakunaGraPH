"""
PSGC Location Fuzzy Matcher
Extracts all valid PH locations from psgc.ttl and fuzzy-matches dirty values.
Returns both the canonical location name and its IRI.
"""

import re
import polars as pl
from rapidfuzz import process, fuzz
from dataclasses import dataclass


BASE_IRI  = "https://sakuna.ph/"
TTL_PATH  = "../data/rdf/psgc.ttl"

# ---------------------------------------------------------------------------
# 1. Data model
# ---------------------------------------------------------------------------

@dataclass
class LocationMatch:
    name:  str      # canonical label, e.g. "City of Quezon"
    iri:   str      # full IRI,        e.g. "https://sakuna.ph/1381050000"
    score: float    # fuzzy confidence 0–100


# ---------------------------------------------------------------------------
# 2. Module-level singleton — loaded once on first use
# ---------------------------------------------------------------------------

_names:    list[str]      | None = None
_name2iri: dict[str, str] | None = None


def _ensure_loaded() -> tuple[list[str], dict[str, str]]:
    global _names, _name2iri
    if _names is None:
        _names, _name2iri = _load_locations(TTL_PATH)
    return _names, _name2iri


def _load_locations(ttl_path: str) -> tuple[list[str], dict[str, str]]:
    level_map = {"Reg", "Prov", "City", "Mun", "SubMun"}
    block_re  = re.compile(
        r":(\w+)\s+a owl:NamedIndividual.*?rdfs:label \"(.+?)\"@en.*?:geographicLevel \"(\w+)\"",
        re.DOTALL,
    )

    with open(ttl_path, encoding="utf-8") as f:
        content = f.read()

    names:    list[str]      = []
    name2iri: dict[str, str] = {}

    for m in block_re.finditer(content):
        psgc_id, label, level = m.group(1), m.group(2).strip(), m.group(3)
        if level in level_map:
            iri = f"{BASE_IRI}{psgc_id}"
            names.append(label)
            name2iri[label] = iri

    return names, name2iri


# ---------------------------------------------------------------------------
# 3. Region alias map — exact (case-insensitive) lookup before fuzzy
# ---------------------------------------------------------------------------
# Maps lowercased aliases → PSGC code. The canonical name is resolved from
# psgc.ttl at runtime so it stays in sync with the ontology.
_REGION_ALIAS_MAP: dict[str, str] = {
    "i":                        "0100000000",
    "1":                        "0100000000",
    "region i":                 "0100000000",
    "region 1":                 "0100000000",
    "ilocos":                   "0100000000",
    "ilocos region":            "0100000000",
    "north luzon":            "0100000000",
    "northern luzon": "0100000000",

    "ii":                       "0200000000",
    "2":                        "0200000000",
    "region ii":                "0200000000",
    "region 2":                 "0200000000",
    "cagayan valley":           "0200000000",

    "iii":                      "0300000000",
    "3":                        "0300000000",
    "region iii":               "0300000000",
    "region 3":                 "0300000000",
    "central luzon":            "0300000000",

    "iv-a":                     "0400000000",
    "4a":                       "0400000000",
    "iva":                      "0400000000",
    "region iv-a":              "0400000000",
    "calabarzon":               "0400000000",
    "southern tagalog":               "0400000000",
    "south luzon":               "0400000000",

    "iv-b":                     "1700000000",
    "4b":                       "1700000000",
    "ivb":                      "1700000000",
    "region iv-b":              "1700000000",
    "mimaropa":                 "1700000000",

    "v":                        "0500000000",
    "5":                        "0500000000",
    "region v":                 "0500000000",
    "region 5":                 "0500000000",
    "bicol":                    "0500000000",
    "bicol region":             "0500000000",

    "vi":                       "0600000000",
    "6":                        "0600000000",
    "region vi":                "0600000000",
    "region 6":                 "0600000000",
    "western visayas":          "0600000000",

    "vii":                      "0700000000",
    "7":                        "0700000000",
    "region vii":               "0700000000",
    "region 7":                 "0700000000",
    "central visayas":          "0700000000",

    "viii":                     "0800000000",
    "8":                        "0800000000",
    "region viii":              "0800000000",
    "region 8":                 "0800000000",
    "eastern visayas":          "0800000000",

    "ix":                       "0900000000",
    "9":                        "0900000000",
    "region ix":                "0900000000",
    "region 9":                 "0900000000",
    "zamboanga peninsula":      "0900000000",

    "x":                        "1000000000",
    "10":                       "1000000000",
    "region x":                 "1000000000",
    "region 10":                "1000000000",
    "northern mindanao":        "1000000000",

    "xi":                       "1100000000",
    "11":                       "1100000000",
    "region xi":                "1100000000",
    "region 11":                "1100000000",
    "davao region":             "1100000000",

    "xii":                      "1200000000",
    "12":                       "1200000000",
    "region xii":               "1200000000",
    "region 12":                "1200000000",
    "soccsksargen":             "1200000000",

    "xiii":                     "1600000000",
    "13":                       "1600000000",
    "region xiii":              "1600000000",
    "region 13":                "1600000000",
    "caraga":                   "1600000000",
    "rtr":                      "1600000000",

    "ncr":                      "1300000000",
    "metro manila":             "1300000000",
    "national capital region":  "1300000000",
    "metropolitan manila":  "1300000000",

    "car":                      "1400000000",
    "cordillera":               "1400000000",

    "barmm":                    "1900000000",
    "armm":                     "1900000000",
    "bangsamoro":               "1900000000",

    "nir":                      "1800000000",
    "negros island region":     "1800000000",
}

# Build reverse map: psgc_code → canonical name (populated lazily from TTL)
_psgc2name: dict[str, str] = {}

def _resolve_region_alias(value: str) -> LocationMatch | None:
    """Exact alias lookup (case-insensitive). Resolves name from TTL."""
    psgc_code = _REGION_ALIAS_MAP.get(value.strip().lower())
    if not psgc_code:
        return None

    iri = f"{BASE_IRI}{psgc_code}"

    # Resolve canonical name from TTL if not cached yet
    if psgc_code not in _psgc2name:
        _, name2iri = _ensure_loaded()
        iri2name = {v: k for k, v in name2iri.items()}
        _psgc2name.update({v.split("/")[-1]: k for k, v in name2iri.items()})
        # Also index by code directly
        for name, full_iri in name2iri.items():
            code = full_iri.replace(BASE_IRI, "")
            _psgc2name[code] = name

    name = _psgc2name.get(psgc_code, psgc_code)
    return LocationMatch(name=name, iri=iri, score=100.0)


# ---------------------------------------------------------------------------
# 3b. Broad-region overrides — last resort for vague/misspelled values
# ---------------------------------------------------------------------------
# Island groups expand to all their constituent regions instead of a single
# vague IRI. The PSGC codes here match the region-level entries in psgc.ttl.

_ISLAND_GROUP_EXPANSIONS: list[tuple[re.Pattern, list[str]]] = [
    (re.compile(r"^luzon$",        re.I), [
        "0100000000",  # Region I   – Ilocos
        "0200000000",  # Region II  – Cagayan Valley
        "0300000000",  # Region III – Central Luzon
        "0400000000",  # Region IV-A – CALABARZON
        "1700000000",  # Region IV-B – MIMAROPA
        "0500000000",  # Region V   – Bicol
        "1300000000",  # NCR
        "1400000000",  # CAR
    ]),
    (re.compile(r"^visayas?$|^bisayas?$", re.I), [
        "0600000000",  # Region VI  – Western Visayas
        "0700000000",  # Region VII – Central Visayas
        "0800000000",  # Region VIII – Eastern Visayas
        "1800000000",  # NIR – Negros Island Region
    ]),
    (re.compile(r"^mindanao$",     re.I), [
        "0900000000",  # Region IX  – Zamboanga Peninsula
        "1000000000",  # Region X   – Northern Mindanao
        "1100000000",  # Region XI  – Davao
        "1200000000",  # Region XII – SOCCSKSARGEN
        "1600000000",  # Region XIII – CARAGA
        "1900000000",  # BARMM
    ]),
]

_BROAD_OVERRIDES: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"philippine|filipin", re.I), "Philippines", "Philippines"),
]


def _override_matches(value: str) -> list[LocationMatch]:
    """
    Return a list of LocationMatches for broad/vague region keywords.
    Island groups (Luzon, Visayas, Mindanao) expand to all constituent regions.
    Philippines returns a single fixed entry.
    Returns [] if nothing matched.
    """
    # Check island-group expansions first — only when value is EXACTLY the island name
    for pattern, psgc_codes in _ISLAND_GROUP_EXPANSIONS:
        if pattern.fullmatch(value.strip()):
            _, name2iri = _ensure_loaded()
            iri2name = {v: k for k, v in name2iri.items()}
            results = []
            for code in psgc_codes:
                iri  = f"{BASE_IRI}{code}"
                name = iri2name.get(iri, code)
                results.append(LocationMatch(name=name, iri=iri, score=100.0))
            return results

    # Broad single-entry overrides (e.g. Philippines)
    for pattern, name, iri_suffix in _BROAD_OVERRIDES:
        if pattern.search(value):
            return [LocationMatch(name=name, iri=f"{BASE_IRI}{iri_suffix}", score=100.0)]

    return []


# ---------------------------------------------------------------------------
# 4. Core match function
# ---------------------------------------------------------------------------

def match_location(
    value:     str,
    threshold: int = 80,
    scorer=fuzz.WRatio,
) -> LocationMatch | None:
    """
    Fuzzy-match a dirty location string against the canonical PSGC list.

    Args:
        value:      Raw / dirty location string.
        threshold:  Minimum score (0–100) to accept. Returns None if below.
        scorer:     rapidfuzz scorer. WRatio handles most cases well;
                    use token_sort_ratio for word-order issues.

    Returns:
        LocationMatch(name, iri, score)  or  None if no match found.
    """
    if not value or not isinstance(value, str):
        return None

    # 1. Exact alias lookup (region numbers, roman numerals, common names)
    alias = _resolve_region_alias(value)
    if alias:
        return alias

    # 2. Fuzzy match — try both scorers across all candidate forms, take the best.
    #    Candidates include the original value plus a "City of X" rewrite when the
    #    value ends with "City", to handle "Cebu City" → "City of Cebu" flips.
    names, name2iri = _ensure_loaded()

    city_match = re.match(r"^(.+?)\s+[Cc]ity$", value.strip())

    # When input ends with "City", try "City of X" first and exclusively —
    # the PSGC stores cities as "City of X" so this will always outscore the
    # plain province/municipality entry of the same name (e.g. "Cebu" province).
    # Only fall back to the original form if the rewritten form finds nothing.
    if city_match:
        rewritten = f"City of {city_match.group(1)}"
        r1 = process.extractOne(rewritten, names, scorer=fuzz.WRatio,          score_cutoff=threshold)
        r2 = process.extractOne(rewritten, names, scorer=fuzz.token_sort_ratio, score_cutoff=threshold)
        best = max((r for r in (r1, r2) if r), key=lambda r: r[1], default=None)
    else:
        best = None

    if not best:
        r1 = process.extractOne(value, names, scorer=fuzz.WRatio,          score_cutoff=threshold)
        r2 = process.extractOne(value, names, scorer=fuzz.token_sort_ratio, score_cutoff=threshold)
        best = max((r for r in (r1, r2) if r), key=lambda r: r[1], default=None)

    if best:
        matched_name, score, _ = best
        return LocationMatch(
            name  = matched_name,
            iri   = name2iri[matched_name],
            score = score,
        )

    # 3. Last resort — broad/vague keywords; island groups expand to first region
    overrides = _override_matches(value)
    return overrides[0] if overrides else None


# ---------------------------------------------------------------------------
# 4. Polars integration
# ---------------------------------------------------------------------------

def _match_cell(value: str, threshold: int) -> tuple[str | None, str | None, float | None]:
    """
    Handle a single cell value, which may contain multiple locations separated by |.
    Returns (names_str, iris_str, avg_score) where names/iris are | joined.
    Unmatched tokens are skipped; returns (None, None, None) if nothing matched.
    """
    if not value or not isinstance(value, str):
        return None, None, None

    tokens  = [t.strip() for t in value.split("|") if t.strip()]

    matched: list = []
    for t in tokens:
        # 1. Island group expansion — must be checked first and only fires on exact names
        expansions = _override_matches(t)
        if expansions:
            matched.extend(expansions)
            continue

        # 2. Exact region alias (roman numerals, numbers, common names)
        alias = _resolve_region_alias(t)
        if alias:
            matched.append(alias)
            continue

        # 3. Fuzzy match against full PSGC list
        names, name2iri = _ensure_loaded()
        result = process.extractOne(t, names, scorer=fuzz.WRatio, score_cutoff=threshold)
        if result:
            matched_name, score, _ = result
            matched.append(LocationMatch(name=matched_name, iri=name2iri[matched_name], score=score))

    if not matched:
        return None, None, None

    # Deduplicate by IRI, preserving first-seen order
    seen_iris: set[str] = set()
    unique: list = []
    for m in matched:
        if m.iri not in seen_iris:
            seen_iris.add(m.iri)
            unique.append(m)

    names_str = "|".join(m.name  for m in unique)
    iris_str  = "|".join(m.iri   for m in unique)
    avg_score = sum(m.score for m in unique) / len(unique)

    return names_str, iris_str, avg_score


def canonicalize_column(
    df:        pl.DataFrame,
    col:       str,
    threshold: int = 80,
    prefix:    str | None = None,
) -> pl.DataFrame:
    """
    Append canonical location columns to a Polars DataFrame.

    Each cell may contain a single location or multiple locations separated
    by "|". Output columns mirror the same "|"-joined format.

    Adds three columns (using `prefix` or `col` as base name):
      - {prefix}_name   : canonical label(s) joined by | or null
      - {prefix}_iri    : full IRI(s) joined by | or null
      - {prefix}_score  : average fuzzy confidence score across all tokens

    Example:
        df = canonicalize_column(df, "raw_location")
        # "Quezon Cty | mnla" → name: "Quezon City|City of Manila"
        #                      → iri:  "https://...| https://..."
    """
    base    = prefix or col
    results = [_match_cell(v, threshold) for v in df[col].to_list()]

    return df.with_columns([
        pl.Series(f"{base}_name",  [r[0] for r in results], dtype=pl.String),
        pl.Series(f"{base}_iri",   [r[1] for r in results], dtype=pl.String),
        pl.Series(f"{base}_score", [r[2] for r in results], dtype=pl.Float64),
    ])


# ---------------------------------------------------------------------------
# 5. Example usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # --- single value (includes override cases) ---
    for dirty in [
        "Quezon Cty", "mnla", "Cebu City", "davao",
        "Philippiens",        # override → Philippines
        "Northern Luzon",     # override → Luzon
        "Southern Mindanano", # override → Mindanao
        "Western Visayas",    # override → Visayas
        "Bandana",            # no match
    ]:
        m = match_location(dirty, threshold=70)
        if m:
            print(f"  {dirty!r:25} → {m.name!r:20}  {m.iri}  (score={m.score:.1f})")
        else:
            print(f"  {dirty!r:25} → no match")

    # --- Polars DataFrame (single and multi-value cells) ---
    df = pl.DataFrame({
        "raw_location": [
            "Quezon Cty",
            "mnla | Cebu City",
            "Northern Luzon | Western Visayas",
            "Philippiens",
            "Bandana",
        ]
    })
    df_clean = canonicalize_column(df, "raw_location", threshold=70)
    print("\n", df_clean)
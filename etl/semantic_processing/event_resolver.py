"""
er.py — SakunaGraPH entity resolution module.

Combines: config, models, extractor, blocker, scorer, aligner, merger.

Sections:
  1. Config
  2. Models
  3. Extractor
  4. Blocker
  5. Scorer
  6. Aligner
  7. Merger
"""

from __future__ import annotations

# ══════════════════════════════════════════════════════════════════════════════
# 1. CONFIG
# ══════════════════════════════════════════════════════════════════════════════

from pathlib import Path


# Earlier in list = higher authority
SOURCE_PRIORITY = ["ndrrmc", "dromic", "emdat", "gda"]

# Blocking
BLOCKING_DATE_TOLERANCE_DAYS  = 45
BLOCKING_LOCATION_PREFIX_LEN  = 4

# Scoring weights — label weight is redistributed when label is missing.
# See score_pair() for dynamic reweighting logic.
SCORE_WEIGHTS = {
    "label":         0.25,
    "disaster_type": 0.30,
    "date":          0.25,
    "location":      0.20,
}

MATCH_THRESHOLD     = 0.85
DATE_SCORE_MAX_DAYS = 30

# Maps free-text variants → canonical concept local name.
# Used as fallback when :hasDisasterType is a plain literal rather than a URI.
DISASTER_TYPE_ALIASES = {
    "flood":            "Flood",
    "flooding":         "Flood",
    "flash flood":      "FlashFlood",
    "flashflood":       "FlashFlood",
    "riverine flood":   "RiverineFlood",
    "coastal flood":    "CoastalFlood",
    "typhoon":          "TropicalCyclone",
    "tropical storm":   "TropicalCyclone",
    "tropical cyclone": "TropicalCyclone",
    "tc":               "TropicalCyclone",
    "storm surge":      "StormSurge",
    "stormsurge":       "StormSurge",
    "earthquake":       "Earthquake",
    "quake":            "Earthquake",
    "ground movement":  "GroundMovement",
    "tsunami":          "Tsunami",
    "landslide":        "LandslideWet",
    "mudslide":         "Mudslide",
    "landslide dry":    "LandslideDry",
    "rockfall":         "RockfallWet",
    "avalanche":        "AvalancheWet",
    "volcanic":         "VolcanicActivity",
    "eruption":         "VolcanicActivity",
    "ashfall":          "Ashfall",
    "lahar":            "Lahar",
    "lava flow":        "LavaFlow",
    "pyroclastic":      "PyroclasticFlow",
    "drought":          "Drought",
    "wildfire":         "Wildfire",
    "forest fire":      "ForestFire",
    "land fire":        "LandFire",
    "epidemic":         "Epidemic",
    "disease":          "InfectiousDiseaseGeneral",
    "infestation":      "Infestation",
    "locust":           "LocustInfestation",
    "fog":              "Fog",
    "thunderstorm":     "Thunderstorms",
    "tornado":          "Tornado",
    "hail":             "Hail",
    "blizzard":         "BlizzardStorm",
    "heat wave":        "HeatWave",
    "cold wave":        "ColdWave",
}

# ── SKOS-style disaster type hierarchy ───────────────────────────────────────
# Keys are the local names of :DisasterType NamedIndividuals exactly as defined
# in sakunagraph.ttl (i.e. the fragment after https://sakuna.ph/).
# Values are the local name of the skos:broader concept (None = top-level).
#
# Scoring by shared ancestor depth:
#   exact match (same concept)    → 1.0
#   sibling (same parent)         → 0.7
#   cousin  (same grandparent)    → 0.4
#   deeper shared ancestor        → 0.2
#   no shared ancestor            → 0.0
#   either type missing           → neutral (weight redistributed to others)

DISASTER_TYPE_HIERARCHY: dict[str, str | None] = {
    # ── Top-level ─────────────────────────────────────────────────────
    "Natural":                    None,
    "Technological":              None,

    # ── Natural → Biological ──────────────────────────────────────────
    "Biological":                 "Natural",
    "Epidemic":                   "Biological",
    "BacterialDisease":           "Epidemic",
    "FungalDisase":               "Epidemic",
    "InfectiousDiseaseGeneral":   "Epidemic",
    "ParasiticDisease":           "Epidemic",
    "PrionDisease":               "Epidemic",
    "ViralDisease":               "Epidemic",
    "Infestation":                "Biological",
    "GrasshopperInfestation":     "Infestation",
    "InfestationGeneral":         "Infestation",
    "LocustInfestation":          "Infestation",
    "WormsInfestation":           "Infestation",
    "AnimalAccident":             "Biological",

    # ── Natural → Climatological ──────────────────────────────────────
    "Climatological":             "Natural",
    "Drought":                    "Climatological",
    "Glacial":                    "Climatological",
    "Wildfire":                   "Climatological",
    "ForestFire":                 "Wildfire",
    "LandFire":                   "Wildfire",
    "WildfireGeneral":            "Wildfire",

    # ── Natural → Extraterrestrial ────────────────────────────────────
    "Extraterrestrial":           "Natural",
    "SpaceImpact":                "Extraterrestrial",
    "SpaceWeather":               "Extraterrestrial",

    # ── Natural → Geophysical ─────────────────────────────────────────
    "Geophysical":                "Natural",
    "Earthquake":                 "Geophysical",
    "GroundMovement":             "Earthquake",
    "Tsunami":                    "Earthquake",
    "MassMovementDry":            "Geophysical",
    "AvalancheDry":               "MassMovementDry",
    "LandslideDry":               "MassMovementDry",
    "RockfallDry":                "MassMovementDry",
    "SuddenSubsidenceDry":        "MassMovementDry",
    "VolcanicActivity":           "Geophysical",
    "Ashfall":                    "VolcanicActivity",
    "Lahar":                      "VolcanicActivity",
    "LavaFlow":                   "VolcanicActivity",
    "PyroclasticFlow":            "VolcanicActivity",
    "VolcanicActivityGeneral":    "VolcanicActivity",

    # ── Natural → Hydrological ────────────────────────────────────────
    "Hydrological":               "Natural",
    "Flood":                      "Hydrological",
    "CoastalFlood":               "Flood",
    "FlashFlood":                 "Flood",
    "FloodGeneral":               "Flood",
    "IceJamFlood":                "Flood",
    "RiverineFlood":              "Flood",
    "MassMovementWet":            "Hydrological",
    "AvalancheWet":               "MassMovementWet",
    "LandslideWet":               "MassMovementWet",
    "Mudslide":                   "MassMovementWet",
    "RockfallWet":                "MassMovementWet",
    "SuddenSubsidenceWet":        "MassMovementWet",
    "WaveAction":                 "Hydrological",
    "RogueWave":                  "WaveAction",
    "Seiche":                     "WaveAction",
    "WaveActionGeneral":          "WaveAction",

    # ── Natural → Meteorological ──────────────────────────────────────
    "Meteorological":             "Natural",
    "ExtremeTemperature":         "Meteorological",
    "ColdWave":                   "ExtremeTemperature",
    "HeatWave":                   "ExtremeTemperature",
    "SevereWinterConditions":     "ExtremeTemperature",
    "Fog":                        "Meteorological",
    "Storm":                      "Meteorological",
    "BlizzardStorm":              "Storm",
    "Derecho":                    "Storm",
    "ExtratropicalStorm":         "Storm",
    "Hail":                       "Storm",
    "SandStorm":                  "Storm",
    "SevereWeather":              "Storm",
    "StormGeneral":               "Storm",
    "StormSurge":                 "Storm",
    "Thunderstorms":              "Storm",
    "Tornado":                    "Storm",
    "TropicalCyclone":            "Storm",

    # ── Technological → Industrial Accident ───────────────────────────
    "IndustrialAccident":         "Technological",
    "ChemicalSpill":              "IndustrialAccident",
    "CollapseIndustrial":         "IndustrialAccident",
    "ExplosionIndustrial":        "IndustrialAccident",
    "FireIndustrial":             "IndustrialAccident",
    "GasLeak":                    "IndustrialAccident",
    "IndustrialAccidentGeneral":  "IndustrialAccident",
    "OilSpill":                   "IndustrialAccident",
    "Poisoning":                  "IndustrialAccident",
    "Radiation":                  "IndustrialAccident",

    # ── Technological → Miscellaneous Accident ────────────────────────
    "MiscellaneousAccident":      "Technological",
    "CollapseMiscellaneous":      "MiscellaneousAccident",
    "ExplosionMiscellaneous":     "MiscellaneousAccident",
    "FireMiscellaneous":          "MiscellaneousAccident",
    "MiscellaneousAccidentGeneral": "MiscellaneousAccident",

    # ── Technological → Transport ─────────────────────────────────────
    "Transport":                  "Technological",
    "Air":                        "Transport",
    "Rail":                       "Transport",
    "Road":                       "Transport",
    "Water":                      "Transport",

    # ── Technological → Armed Conflict ────────────────────────────────
    "ArmedConflict":              "Technological",
}


# ══════════════════════════════════════════════════════════════════════════════
# 2. MODELS
# ══════════════════════════════════════════════════════════════════════════════

from dataclasses import dataclass, field
from datetime import date
from typing import Optional
from rdflib import URIRef


@dataclass
class DisasterEvent:
    """
    Lightweight representation of a disaster event extracted from RDF.
    Used for blocking, scoring, and alignment — not for full triple storage.
    """
    uri:           URIRef
    source:        str

    label:             Optional[str]    = None
    disaster_type:     Optional[str]    = None  # local name, e.g. "Flood"
    disaster_type_uri: Optional[URIRef] = None  # full URI from ont:hasDisasterType
    start_date:        Optional[date]   = None
    end_date:      Optional[date] = None
    blocking_keys: list[str] = field(default_factory=lambda: [])

    def __hash__(self):
        return hash(self.uri)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DisasterEvent) and self.uri == other.uri

    def __repr__(self):
        return (
            f"DisasterEvent(source={self.source!r}, label={self.label!r}, "
            f"type={self.disaster_type!r}, date={self.start_date}, "
            f"location={self.location!r})"
        )


# ══════════════════════════════════════════════════════════════════════════════
# 3. EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════

import re
from datetime import datetime

from rdflib import Graph, Namespace, Literal, RDF, RDFS, OWL
from rdflib.namespace import SKOS, XSD
from mappings.graph import SKG, GEO, PROV, create_graph

BAW = Namespace("https://raw.githubusercontent.com/beAWARE-project/ontology/master/beAWARE_ontology#")

SAKUNA_NS = str(SKG)   # "https://sakuna.ph/" — keep for string-based URI construction


def _infer_source(uri: URIRef) -> str:
    uri_str = str(uri)
    for src in SOURCE_PRIORITY:
        if src.lower() in uri_str.lower():
            return src.lower()
    parts = uri_str.rstrip("/").split("/")
    return parts[-2].lower() if len(parts) >= 2 else "unknown"


def _parse_date(value: str) -> date | None:
    # ontology range is xsd:dateTime — strip time part if present
    value = value.strip().split("T")[0]
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _normalize_type(raw: str) -> str | None:
    raw = re.sub(r"^.*[/#]", "", raw.lower())
    raw = raw.replace("_", " ").replace("-", " ").strip()
    for alias, canonical in DISASTER_TYPE_ALIASES.items():
        if alias in raw:
            return canonical
    return raw if raw else None


def _get_literal(g: Graph, subject: URIRef, *predicates) -> str | None:
    for pred in predicates:
        for obj in g.objects(subject, pred):
            if isinstance(obj, Literal):
                return str(obj)
    return None


def _get_uri(g: Graph, subject: URIRef, *predicates) -> URIRef | None:
    for pred in predicates:
        for obj in g.objects(subject, pred):
            if isinstance(obj, URIRef):
                return obj
    return None


def extract_events_from_graph(g: Graph, source: str) -> list[DisasterEvent]:
    """Extract all DisasterEvent instances from a loaded RDF graph."""
    events = []
    event_uris: set[URIRef] = set()

    for s, _, o in g.triples((None, RDF.type, None)):
        if not isinstance(s, URIRef):
            continue
        # Accept :DisasterEvent, :MajorEvent, :Incident (all subclasses)
        if str(o) in (
            f"{SAKUNA_NS}DisasterEvent",
            f"{SAKUNA_NS}MajorEvent",
            f"{SAKUNA_NS}Incident",
        ):
            event_uris.add(s)

    for uri in event_uris:
        inferred_source = source or _infer_source(uri)

        # ── Label: :eventName (primary), rdfs:label, skos:prefLabel ──────────
        label = _get_literal(g, uri,
            SKG.eventName,       # :eventName xsd:string — the canonical name field
            RDFS.label,
            SKOS.prefLabel,
        )

        # ── Disaster type ─────────────────────────────────────────────────────
        disaster_type     = None
        disaster_type_uri = None

        # Primary: :hasDisasterType → :DisasterType individual URI
        dt_uri = _get_uri(g, uri, SKG.hasDisasterType, BAW.isOfDisasterType)
        if dt_uri:
            disaster_type_uri = dt_uri
            local = re.sub(r"^.*[/#]", "", str(dt_uri))
            disaster_type = local if local in DISASTER_TYPE_HIERARCHY else _normalize_type(local)

        # Fallback: :hasDisasterSubtype
        if not disaster_type:
            dt_uri = _get_uri(g, uri, SKG.hasDisasterSubtype)
            if dt_uri:
                disaster_type_uri = dt_uri
                local = re.sub(r"^.*[/#]", "", str(dt_uri))
                disaster_type = local if local in DISASTER_TYPE_HIERARCHY else _normalize_type(local)

        # Fallback: plain literal (some sources emit a string instead of URI)
        if not disaster_type:
            raw = _get_literal(g, uri, SKG.hasDisasterType)
            if raw:
                disaster_type = _normalize_type(raw)

        # ── Dates: :startDate / :endDate (xsd:dateTime in ontology) ──────────
        # baw:hasDisasterStart ≡ :startDate, baw:hasDisasterEnd ≡ :endDate
        start_str = _get_literal(g, uri,
            SKG.startDate,
            BAW.hasDisasterStart,
        )
        end_str = _get_literal(g, uri,
            SKG.endDate,
            BAW.hasDisasterEnd,
        )

        start_date = _parse_date(start_str) if start_str else None
        end_date   = _parse_date(end_str)   if end_str   else None


        events.append(DisasterEvent(
            uri=uri, source=inferred_source,
            label=label, disaster_type=disaster_type,
            disaster_type_uri=disaster_type_uri,
            start_date=start_date, end_date=end_date,
        ))

    return events


def load_all_sources(sources_dir: Path = SOURCES_DIR) -> list[DisasterEvent]:
    """Load all .ttl files from sources_dir and return a flat list of events."""
    all_events = []
    for ttl_path in sorted(sources_dir.glob("*.ttl")):
        source_name = ttl_path.stem.lower()
        g = Graph()
        g.parse(str(ttl_path), format="turtle")
        events = extract_events_from_graph(g, source=source_name)
        print(f"  [{source_name}] extracted {len(events)} events")
        all_events.extend(events)
    return all_events


# ══════════════════════════════════════════════════════════════════════════════
# 4. BLOCKER
# ══════════════════════════════════════════════════════════════════════════════

import unicodedata
from collections import defaultdict
from itertools import combinations


def normalize_text(text: str) -> str:
    """Lowercase, strip accents, collapse whitespace, remove punctuation."""
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def location_token(location: str | None) -> str:
    if not location:
        return "unknown"
    normalized = normalize_text(location)
    stopwords = {
        "region", "province", "city", "municipality", "barangay",
        "the", "of", "and", "northern", "southern", "eastern", "western",
        "central", "national", "capital",
    }
    tokens = [t for t in normalized.split() if t not in stopwords and len(t) >= 3]
    if not tokens:
        return normalized[:BLOCKING_LOCATION_PREFIX_LEN] or "unknown"
    return tokens[0][:BLOCKING_LOCATION_PREFIX_LEN]


def year_windows(d: date | None) -> list[str]:
    if not d:
        return ["unknown"]
    years = {str(d.year)}
    if d.month == 1:  years.add(str(d.year - 1))
    if d.month == 12: years.add(str(d.year + 1))
    return list(years)


def generate_blocking_keys(event: DisasterEvent) -> list[str]:
    """Generate blocking keys: {disaster_type}|{year}|{location_prefix}"""
    dtype  = event.disaster_type or "unknown"
    years  = year_windows(event.start_date)
    loctok = location_token(event.location)
    keys = []
    for year in years:
        keys.append(f"{dtype}|{year}|{loctok}")
    for year in years:
        fallback = f"{dtype}|{year}|*"
        if fallback not in keys:
            keys.append(fallback)
    return keys


def assign_blocking_keys(events: list[DisasterEvent]) -> None:
    for event in events:
        event.blocking_keys = generate_blocking_keys(event)


def generate_candidate_pairs(
    events: list[DisasterEvent],
) -> list[tuple[DisasterEvent, DisasterEvent]]:
    """Build inverted index, yield unique cross-source pairs within each bucket."""
    assign_blocking_keys(events)

    buckets: dict[str, list[DisasterEvent]] = defaultdict(list)
    for event in events:
        for key in event.blocking_keys:
            buckets[key].append(event)

    seen: set[frozenset] = set()
    pairs: list[tuple[DisasterEvent, DisasterEvent]] = []

    for key, bucket in buckets.items():
        if len(bucket) < 2:
            continue
        for a, b in combinations(bucket, 2):
            if a.source == b.source:
                continue
            pair_id = frozenset([a.uri, b.uri])
            if pair_id in seen:
                continue
            seen.add(pair_id)
            if a.start_date and b.start_date:
                if abs((a.start_date - b.start_date).days) > BLOCKING_DATE_TOLERANCE_DAYS:
                    continue
            pairs.append((a, b))

    return pairs


def blocking_stats(
    events: list[DisasterEvent],
    pairs: list[tuple[DisasterEvent, DisasterEvent]],
) -> dict:
    n = len(events)
    total_possible = n * (n - 1) // 2
    reduction = 1.0 - (len(pairs) / total_possible) if total_possible > 0 else 1.0
    return {
        "total_events":         n,
        "total_possible_pairs": total_possible,
        "candidate_pairs":      len(pairs),
        "reduction_ratio":      round(reduction, 4),
    }


# ══════════════════════════════════════════════════════════════════════════════
# 5. SCORER
# ══════════════════════════════════════════════════════════════════════════════

from dataclasses import dataclass as _dataclass
from rapidfuzz import fuzz


@_dataclass
class ScoreBreakdown:
    label:         float
    disaster_type: float
    date:          float
    location:      float
    total:         float
    is_match:      bool

    def as_dict(self) -> dict:
        return {
            "label":         round(self.label, 4),
            "disaster_type": round(self.disaster_type, 4),
            "date":          round(self.date, 4),
            "location":      round(self.location, 4),
            "total":         round(self.total, 4),
            "is_match":      self.is_match,
        }


def score_label(a: DisasterEvent, b: DisasterEvent) -> float | None:
    """
    Fuzzy label similarity using token_sort, token_set, and partial ratios.
    Returns None when either label is missing — caller redistributes the weight.
    """
    if not a.label or not b.label:
        return None  # missing — do not inject a neutral value
    la, lb = normalize_text(a.label), normalize_text(b.label)
    return max(
        fuzz.token_sort_ratio(la, lb) / 100.0,
        fuzz.token_set_ratio(la, lb)  / 100.0,
        fuzz.partial_ratio(la, lb)    / 100.0,
    )


# ── Hierarchy helpers ─────────────────────────────────────────────────────────

def _ancestors(concept: str) -> list[str]:
    """
    Return the ancestor chain for a concept, from itself up to the root.
    e.g. FlashFlood -> [FlashFlood, Flood, HydrologicalDisaster, NaturalDisaster]
    """
    chain = []
    current: str | None = concept
    seen = set()
    while current is not None and current not in seen:
        chain.append(current)
        seen.add(current)
        current = DISASTER_TYPE_HIERARCHY.get(current)
    return chain


def _resolve_concept(event: DisasterEvent) -> str | None:
    """
    Get the best concept local name for an event's disaster type.
    Prefers the local name derived from the URI; falls back to normalized alias.
    """
    if event.disaster_type_uri:
        local = re.sub(r"^.*[/#]", "", str(event.disaster_type_uri))
        if local in DISASTER_TYPE_HIERARCHY:
            return local
    if event.disaster_type:
        # Try direct match first
        if event.disaster_type in DISASTER_TYPE_HIERARCHY:
            return event.disaster_type
        # Try alias lookup
        normalized = event.disaster_type.lower().replace("_", " ")
        return DISASTER_TYPE_ALIASES.get(normalized)
    return None


def score_disaster_type(a: DisasterEvent, b: DisasterEvent) -> float | None:
    """
    Hierarchy-aware disaster type scoring.

    Resolves each event's type to a concept in DISASTER_TYPE_HIERARCHY,
    then scores based on the depth of the lowest common ancestor (LCA):

      Same concept                 → 1.0
      Sibling (same parent)        → 0.7
      Cousin  (same grandparent)   → 0.4
      Deeper shared ancestor       → 0.2
      No shared ancestor           → 0.0
      Either type unresolvable     → None  (weight redistributed to others)
    """
    ca = _resolve_concept(a)
    cb = _resolve_concept(b)

    if not ca or not cb:
        return None  # missing — do not inject a neutral value

    if ca == cb:
        return 1.0

    ancestors_a = _ancestors(ca)
    ancestors_b = _ancestors(cb)
    set_b = set(ancestors_b)

    # Find LCA: first ancestor of a that also appears in b's chain
    for depth_a, anc in enumerate(ancestors_a):
        if anc in set_b:
            depth_b = ancestors_b.index(anc)
            # Combined distance from both nodes to LCA
            combined_distance = depth_a + depth_b
            if combined_distance == 2:   return 0.7   # siblings
            elif combined_distance == 4: return 0.4   # cousins
            else:                        return 0.2   # deeper shared root

    return 0.0  # completely unrelated subtrees


def score_date(a: DisasterEvent, b: DisasterEvent) -> float | None:
    """Linear decay. Returns None when either date is missing."""
    if not a.start_date or not b.start_date:
        return None
    delta_days = abs((a.start_date - b.start_date).days)
    return max(0.0, 1.0 - (delta_days / DATE_SCORE_MAX_DAYS))


def score_location(a: DisasterEvent, b: DisasterEvent) -> float | None:
    """Token set ratio. Returns None when either location is missing."""
    if not a.location or not b.location:
        return None
    return fuzz.token_set_ratio(normalize_text(a.location), normalize_text(b.location)) / 100.0


def score_pair(a: DisasterEvent, b: DisasterEvent) -> ScoreBreakdown:
    """
    Compute weighted similarity. When a feature returns None (missing data),
    its weight is redistributed proportionally across the present features
    rather than injecting a neutral 0.5 that dilutes the signal.
    """
    base_w = SCORE_WEIGHTS

    raw_scores = {
        "label":         score_label(a, b),
        "disaster_type": score_disaster_type(a, b),
        "date":          score_date(a, b),
        "location":      score_location(a, b),
    }

    # Separate present and missing features
    present  = {k: v for k, v in raw_scores.items() if v is not None}
    missing  = {k for k, v in raw_scores.items() if v is None}

    if not present:
        # Extreme edge case: no features available at all
        total = 0.0
    else:
        # Redistribute missing weights proportionally to present features
        missing_weight = sum(base_w[k] for k in missing)
        present_weight = sum(base_w[k] for k in present)

        adjusted_w = {
            k: base_w[k] + base_w[k] / present_weight * missing_weight
            for k in present
        }

        total = sum(adjusted_w[k] * present[k] for k in present)

    # Fill None scores with 0.0 for the breakdown record (display only)
    filled = {k: (v if v is not None else 0.0) for k, v in raw_scores.items()}

    return ScoreBreakdown(
        label=filled["label"],
        disaster_type=filled["disaster_type"],
        date=filled["date"],
        location=filled["location"],
        total=total,
        is_match=total >= MATCH_THRESHOLD,
    )


def score_all_pairs(
    pairs: list[tuple[DisasterEvent, DisasterEvent]],
    verbose: bool = False,
) -> list[tuple[DisasterEvent, DisasterEvent, ScoreBreakdown]]:
    results = []
    for a, b in pairs:
        breakdown = score_pair(a, b)
        results.append((a, b, breakdown))
        if verbose and breakdown.total >= 0.70:
            print(
                f"  [{breakdown.total:.3f}] "
                f"{a.source}/{a.label!r} <-> {b.source}/{b.label!r} "
                f"{'MATCH' if breakdown.is_match else ''}"
            )
    results.sort(key=lambda x: x[2].total, reverse=True)
    return results


# ══════════════════════════════════════════════════════════════════════════════
# 6. ALIGNER
# ══════════════════════════════════════════════════════════════════════════════

import json
from datetime import datetime as _datetime

import networkx as nx
from rdflib import OWL as _OWL


def pick_canonical(cluster: set[URIRef]) -> URIRef:
    """Pick canonical URI from a sameAs cluster using SOURCE_PRIORITY."""
    for source in SOURCE_PRIORITY:
        for uri in sorted(cluster):
            if source.lower() in str(uri).lower():
                return uri
    return sorted(cluster)[0]


def build_clusters(
    matches: list[tuple[DisasterEvent, DisasterEvent, ScoreBreakdown]]
) -> list[tuple[URIRef, set[URIRef]]]:
    """Build connected components from match pairs using Union-Find (networkx)."""
    G = nx.Graph()
    for a, b, score in matches:
        if score.is_match:
            G.add_edge(a.uri, b.uri, weight=score.total)

    clusters = []
    for component in nx.connected_components(G):
        canonical = pick_canonical(component)
        clusters.append((canonical, component))
    return clusters


def write_alignments(
    matches: list[tuple[DisasterEvent, DisasterEvent, ScoreBreakdown]],
    output_path: Path = ALIGNMENTS_PATH,
    include_scores: bool = True,
) -> Graph:
    """Write owl:sameAs alignment triples + confidence metadata to Turtle."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    g = Graph()
    g.bind("",     SKG)    # base prefix — ontology terms and instances
    g.bind("owl",  OWL)
    g.bind("skos", SKOS)
    g.bind("prov", PROV)
    g.bind("xsd",  XSD)

    run_uri = URIRef(
        f"{SAKUNA_NS}alignment-run/{_datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
    )
    g.add((run_uri, RDF.type,       PROV.Activity))
    g.add((run_uri, SKG.entryDate, Literal(_datetime.utcnow().isoformat(), datatype=XSD.dateTime)))
    g.add((run_uri, RDFS.label,     Literal("SakunaGraPH entity alignment run")))

    clusters = build_clusters(matches)
    pair_scores: dict[frozenset, float] = {
        frozenset([a.uri, b.uri]): sc.total
        for a, b, sc in matches if sc.is_match
    }

    for canonical, cluster in clusters:
        for other in sorted(cluster - {canonical}):
            g.add((canonical, OWL.sameAs, other))

            if include_scores:
                stmt_uri = URIRef(
                    f"{SAKUNA_NS}alignment/"
                    f"{str(canonical).split('/')[-1]}--{str(other).split('/')[-1]}"
                )
                g.add((stmt_uri, RDF.type,      RDF.Statement))
                g.add((stmt_uri, RDF.subject,   canonical))
                g.add((stmt_uri, RDF.predicate, OWL.sameAs))
                g.add((stmt_uri, RDF.object,    other))
                score = pair_scores.get(frozenset([canonical, other]))
                if score is not None:
                    g.add((stmt_uri, SKG.matchConfidence,
                           Literal(round(score, 4), datatype=XSD.decimal)))
                g.add((stmt_uri, PROV.wasGeneratedBy, run_uri))

    g.serialize(str(output_path), format="turtle")
    print(f"  Wrote {len(clusters)} clusters -> {output_path}")
    return g


def save_registry(
    clusters: list[tuple[URIRef, set[URIRef]]],
    registry_path: Path = REGISTRY_PATH,
) -> None:
    """Persist canonical -> duplicates mapping as JSON for incremental runs."""
    registry = {
        str(canonical): [str(u) for u in sorted(cluster)]
        for canonical, cluster in clusters
    }
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    with open(registry_path, "w") as f:
        json.dump(registry, f, indent=2)
    print(f"  Registry saved -> {registry_path}")


def load_registry(registry_path: Path = REGISTRY_PATH) -> dict[str, list[str]]:
    if not registry_path.exists():
        return {}
    with open(registry_path) as f:
        return json.load(f)


def get_known_pairs(registry: dict[str, list[str]]) -> set[frozenset]:
    known = set()
    for canonical, duplicates in registry.items():
        for dup in duplicates:
            known.add(frozenset([canonical, dup]))
    return known


# ══════════════════════════════════════════════════════════════════════════════
# 7. MERGER
# ══════════════════════════════════════════════════════════════════════════════

from rdflib import BNode


MULTI_VALUE_PREDICATES = {
    RDF.type,
    OWL.sameAs,
    RDFS.label,
    SKOS.prefLabel,
    SKG.hasDisasterType,
    SKG.hasLocation,
}


def _infer_source_rank(uri: URIRef) -> int:
    uri_str = str(uri).lower()
    for i, src in enumerate(SOURCE_PRIORITY):
        if src.lower() in uri_str:
            return i
    return len(SOURCE_PRIORITY)


def build_uri_to_canonical(alignment_graph: Graph) -> dict[URIRef, URIRef]:
    """Read owl:sameAs triples, build connected components, return uri -> canonical map."""
    G = nx.Graph()
    for s, _, o in alignment_graph.triples((None, OWL.sameAs, None)):
        if isinstance(s, URIRef) and isinstance(o, URIRef):
            G.add_edge(s, o)

    uri_to_canonical: dict[URIRef, URIRef] = {}
    for component in nx.connected_components(G):
        canonical = min(component, key=lambda u: (_infer_source_rank(u), str(u)))
        for uri in component:
            uri_to_canonical[uri] = canonical
    return uri_to_canonical


def _remap_node(node, uri_to_canonical: dict):
    if isinstance(node, URIRef):
        return uri_to_canonical.get(node, node)
    return node


def _resolve_conflicts(
    triple_candidates: dict[tuple, list[tuple[int, any]]],
) -> list[tuple]:
    result = []
    for (s, p), candidates in triple_candidates.items():
        if p in MULTI_VALUE_PREDICATES:
            seen_objects: set = set()
            for _, o in candidates:
                if o not in seen_objects:
                    result.append((s, p, o))
                    seen_objects.add(o)
        else:
            _, best_obj = min(candidates, key=lambda x: x[0])
            result.append((s, p, best_obj))
    return result


def merge_graphs(
    sources_dir:     Path = SOURCES_DIR,
    alignments_path: Path = ALIGNMENTS_PATH,
    output_path:     Path = CANONICAL_PATH,
) -> Graph:
    """
    Full merge pipeline:
      load sources -> remap URIs -> resolve conflicts -> serialize canonical.ttl
    """
    import glob as _glob

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Load sources
    print("Loading source graphs...")
    source_graphs: dict[str, Graph] = {}
    for ttl_path in sorted(sources_dir.glob("*.ttl")):
        g = Graph()
        g.parse(str(ttl_path), format="turtle")
        source_graphs[ttl_path.stem.lower()] = g
        print(f"  [{ttl_path.stem}] {len(g)} triples")

    # Load alignments
    print("Loading alignments...")
    alignments = Graph()
    if alignments_path.exists():
        alignments.parse(str(alignments_path), format="turtle")
        n_sameas = len(list(alignments.triples((None, OWL.sameAs, None))))
        print(f"  {n_sameas} owl:sameAs pairs")
    else:
        print("  No alignments file found — producing unmerged graph")

    uri_to_canonical = build_uri_to_canonical(alignments)
    print(f"  {len(uri_to_canonical)} URIs remapped to canonical")

    # Collect and remap triples
    print("Remapping triples...")
    triple_candidates: dict[tuple, list] = defaultdict(list)

    for source_name, g in source_graphs.items():
        rank = SOURCE_PRIORITY.index(source_name) \
            if source_name in SOURCE_PRIORITY else len(SOURCE_PRIORITY)
        for s, p, o in g:
            if isinstance(s, BNode) or isinstance(o, BNode):
                continue
            canonical_s = _remap_node(s, uri_to_canonical)
            canonical_o = _remap_node(o, uri_to_canonical)
            triple_candidates[(canonical_s, p)].append((rank, canonical_o))

    # Resolve conflicts
    print("Resolving conflicts...")
    resolved = _resolve_conflicts(triple_candidates)

    # Build output graph
    merged = Graph()
    merged.bind("",    SKG)   # base prefix
    merged.bind("baw", BAW)
    merged.bind("owl", OWL)
    merged.bind("geo", GEO)
    merged.bind("skos", SKOS)
    merged.bind("prov", PROV)
    merged.bind("xsd", XSD)

    for triple in resolved:
        merged.add(triple)

    # Re-add owl:sameAs provenance backlinks
    for original, canonical in uri_to_canonical.items():
        if original != canonical:
            merged.add((canonical, OWL.sameAs, original))

    merged.serialize(str(output_path), format="turtle")
    print(f"\ncanonical.ttl written: {len(merged)} triples -> {output_path}")
    return merged


def reload_oxigraph(canonical_path: Path, db_path: str) -> None:
    """Drop and reload the Oxigraph store from canonical.ttl."""
    try:
        import pyoxigraph as ox
    except ImportError:
        print("pyoxigraph not installed — skipping store reload")
        return

    print(f"Reloading Oxigraph store at {db_path}...")
    store = ox.Store(db_path)
    store.remove_graph(ox.DefaultGraph())
    for graph in list(store.named_graphs()):
        store.remove_graph(graph)
    with open(canonical_path, "rb") as f:
        store.load(f, mime_type="text/turtle")
    print("Oxigraph reloaded")
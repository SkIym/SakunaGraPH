"""
er.py — SakunaGraPH entity resolution module.

Combines: config, models, extractor, blocker, scorer, aligner, merger.

Resolution criteria (all gates must pass):
  0. Report level   - skips ndrrmc incident types derived from tables
  1. Date gate      — |start_date_a - start_date_b| ≤ DATE_HARD_GATE_DAYS (hard reject)
  2. Type gate      — same concept OR same parent concept (hard reject on mismatch)
  3. Label/location — if both eventNames present: fuzzy ≥ threshold OR shared proper noun
                      if absent: at least one location token overlaps (skipped if both missing)
  4. PSGC gate      — activates when either event is an Incident; checks that at least
                      one location URI pair shares a region (2-digit), province (4-digit),
                      or exact (10-digit) PSGC prefix; skipped (None) when either side
                      has no resolvable PSGC codes

Sections:
  1. Config
  2. Models
  3. Extractor
  4. Blocker
  5. Scorer
  6. Aligner
"""

from __future__ import annotations

# ══════════════════════════════════════════════════════════════════════════════
# 1. CONFIG
# ══════════════════════════════════════════════════════════════════════════════

from pathlib import Path
from typing import Dict, Generic, TypeVar, Set
from collections import defaultdict

from mappings.iris import mint_canonical_iri

# Earlier in list = higher authority
SOURCE_PRIORITY = ["ndrrmc", "dromic", "emdat", "gda"]


# Blocking
BLOCKING_DATE_TOLERANCE_DAYS = 5   # mirrors DATE_HARD_GATE_DAYS — no point surfacing pairs we'll reject
BLOCKING_LOCATION_PREFIX_LEN = 4

# ── Resolution gates ──────────────────────────────────────────────────────────
DATE_HARD_GATE_DAYS = 5            # Gate 1: dates must be within this many days
                                    # Gate 2: disaster type must be exact or same parent
LABEL_FUZZY_THRESHOLD = 0.70        # Gate 3a: minimum fuzzy similarity when both labels present
PROPER_NOUN_MIN_LEN   = 4           # Gate 3a: minimum length for a capitalised token to count
                                    # Gate 3b (no label): any overlapping location token = pass

# ── PSGC hierarchy matching levels (Gate 4) ───────────────────────────────────
# PSGC codes are 10-digit strings:  RRPPMMBBBB
#   RR   = region   (digits 1–2)
#   RRPP = province (digits 1–4)
#   full = exact municipality/city/barangay match
#
# Gate 4 passes if any code from event A shares a prefix at one of these
# levels with any code from event B.
PSGC_MATCH_LEVELS: tuple[int, ...] = (2, 4, 10)

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
from datetime import date, timezone
from typing import Optional
from rdflib import URIRef

# Valid rdf_type values stored on DisasterEvent
RDF_TYPE_INCIDENT       = "Incident"
RDF_TYPE_DISASTER_EVENT = "DisasterEvent"
RDF_TYPE_MAJOR_EVENT    = "MajorEvent"


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
    end_date:          Optional[date]   = None
    location:          Optional[str]    = None
    # RDF class of this event: "Incident", "DisasterEvent", or "MajorEvent".
    # Used by the PSGC gate to decide whether location disambiguation is required.
    rdf_type:          str              = RDF_TYPE_DISASTER_EVENT
    # Location URIs pointing to PSGC-keyed :Municipality/:City/:Province/:Region
    # individuals.  Populated alongside the existing `location` literal; empty
    # when the source emits only a plain string or no location at all.
    location_uris:     list[URIRef]     = field(default_factory=lambda: [])
    blocking_keys:     list[str]        = field(default_factory=lambda: [])

    def __hash__(self):
        return hash(self.uri)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DisasterEvent) and self.uri == other.uri

    def __repr__(self):
        return (
            f"DisasterEvent(source={self.source!r}, label={self.label!r}, "
            f"type={self.disaster_type!r}, date={self.start_date}, "
            f"location={self.location!r}, rdf_type={self.rdf_type!r})"
        )


# ══════════════════════════════════════════════════════════════════════════════
# 3. EXTRACTOR
# ══════════════════════════════════════════════════════════════════════════════

import re
from datetime import datetime

from rdflib import Graph, Namespace, Literal, RDF, RDFS, OWL
from rdflib.namespace import SKOS, XSD
from mappings.graph import SKG, GEO, PROV

BAW = Namespace("https://raw.githubusercontent.com/beAWARE-project/ontology/master/beAWARE_ontology#")

SAKUNA_NS = str(SKG)   # "https://sakuna.ph/"

# Map full RDF type URI → rdf_type string stored on DisasterEvent
_RDF_TYPE_MAP = {
    f"{SAKUNA_NS}DisasterEvent": RDF_TYPE_DISASTER_EVENT,
    f"{SAKUNA_NS}MajorEvent":    RDF_TYPE_MAJOR_EVENT,
    f"{SAKUNA_NS}Incident":      RDF_TYPE_INCIDENT,
}


def _infer_source(uri: URIRef) -> str:
    uri_str = str(uri)
    for src in SOURCE_PRIORITY:
        if src.lower() in uri_str.lower():
            return src.lower()
    parts = uri_str.rstrip("/").split("/")
    return parts[-2].lower() if len(parts) >= 2 else "unknown"


def _parse_date(value: str) -> date | None:
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


def _get_literal(g: Graph, subject: URIRef, *predicates: URIRef) -> str | None:
    for pred in predicates:
        for obj in g.objects(subject, pred):
            if isinstance(obj, Literal):
                return str(obj)
    return None


def _get_uri(g: Graph, subject: URIRef, *predicates: URIRef) -> URIRef | None:
    for pred in predicates:
        for obj in g.objects(subject, pred):
            if isinstance(obj, URIRef):
                return obj
    return None


def _get_all_uris(g: Graph, subject: URIRef, *predicates: URIRef) -> list[URIRef]:
    """Collect all URIRef objects for a subject across one or more predicates."""
    results: list[URIRef] = []
    seen: set[URIRef] = set()
    for pred in predicates:
        for obj in g.objects(subject, pred):
            if isinstance(obj, URIRef) and obj not in seen:
                results.append(obj)
                seen.add(obj)
    return results


def extract_events_from_graph(g: Graph, source: str) -> list[DisasterEvent]:
    """Extract all DisasterEvent instances from a loaded RDF graph."""
    events: list[DisasterEvent] = []
    # Maps URI → rdf_type string so we can carry it through to the dataclass.
    event_uris: dict[URIRef, str] = {}

    for s, _, o in g.triples((None, RDF.type, None)):
        if not isinstance(s, URIRef):
            continue
        type_str = str(o)
        rdf_type = _RDF_TYPE_MAP.get(type_str)
        if rdf_type is None:
            continue
        # Incidents from ndrrmc are skip-listed (report-level check)
        if rdf_type == RDF_TYPE_INCIDENT and source == "ndrrmc":
            continue
        # Keep the most specific type if the URI appears under multiple rdf:type
        # triples (MajorEvent > DisasterEvent > Incident by specificity order).
        existing = event_uris.get(s)
        if existing is None or _type_rank(rdf_type) > _type_rank(existing):
            event_uris[s] = rdf_type

    for uri, rdf_type in event_uris.items():
        inferred_source = source or _infer_source(uri)

        # ── Label ─────────────────────────────────────────────────────────────
        label = _get_literal(g, uri,
            SKG.eventName,
            RDFS.label,
            SKOS.prefLabel,
        )

        # ── Disaster type ─────────────────────────────────────────────────────
        disaster_type     = None
        disaster_type_uri = None

        dt_uri = _get_uri(g, uri, SKG.hasDisasterType, BAW.isOfDisasterType)
        if dt_uri:
            disaster_type_uri = dt_uri
            local = re.sub(r"^.*[/#]", "", str(dt_uri))
            disaster_type = local if local in DISASTER_TYPE_HIERARCHY else _normalize_type(local)

        if not disaster_type:
            dt_uri = _get_uri(g, uri, SKG.hasDisasterSubtype)
            if dt_uri:
                disaster_type_uri = dt_uri
                local = re.sub(r"^.*[/#]", "", str(dt_uri))
                disaster_type = local if local in DISASTER_TYPE_HIERARCHY else _normalize_type(local)

        if not disaster_type:
            raw = _get_literal(g, uri, SKG.hasDisasterType)
            if raw:
                disaster_type = _normalize_type(raw)

        # ── Dates ─────────────────────────────────────────────────────────────
        start_str = _get_literal(g, uri, SKG.startDate, BAW.hasDisasterStart)
        end_str   = _get_literal(g, uri, SKG.endDate,   BAW.hasDisasterEnd)
        start_date = _parse_date(start_str) if start_str else None
        end_date   = _parse_date(end_str)   if end_str   else None

        # ── Location ──────────────────────────────────────────────────────────
        # Preserve the existing literal for Gate 3b token overlap.
        location = _get_literal(g, uri,
            SKG.hasLocation,
            RDFS.label,
        )
        # Collect URIs in parallel for Gate 4 PSGC matching.
        # Only URIRef objects qualify — plain literals cannot be PSGC-matched.
        location_uris = _get_all_uris(g, uri, SKG.hasLocation)

        events.append(DisasterEvent(
            uri=uri, source=inferred_source,
            label=label, disaster_type=disaster_type,
            disaster_type_uri=disaster_type_uri,
            start_date=start_date, end_date=end_date,
            location=location,
            rdf_type=rdf_type,
            location_uris=location_uris,
        ))

    return events


def _type_rank(rdf_type: str) -> int:
    """Higher rank = more specific type; used to resolve multi-typed URIs."""
    return {RDF_TYPE_INCIDENT: 0, RDF_TYPE_DISASTER_EVENT: 1, RDF_TYPE_MAJOR_EVENT: 2}.get(rdf_type, 0)


def load_all_sources(sources_dir: Path) -> list[DisasterEvent]:
    """Load all .ttl files from sources_dir and return a flat list of events."""
    all_events: list[DisasterEvent] = []
    for ttl_path in sorted(sources_dir.glob("*.ttl")):
        source_name = re.sub(r"-\d+$", "", ttl_path.stem).lower()
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
    """Generate blocking keys: {disaster_type}|{year}|"""
    dtype = event.disaster_type or "unknown"
    years = year_windows(event.start_date)
    keys: list[str] = []
    for year in years:
        keys.append(f"{dtype}|{year}|")
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

    seen: set[frozenset[URIRef]] = set()
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
) -> dict[str, int | float]:
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
    """
    Diagnostic record for a scored pair.

    Gate fields:
      date_gate     — None if either date missing, True/False otherwise
      type_gate     — None if either type unresolvable, True/False otherwise
      label_gate    — None if used location fallback or skipped, True/False otherwise
      location_gate — None if labels were present (gate not used) or both locations
                      missing; True/False when used as fallback
      psgc_gate     — None if neither event is an Incident (gate not active), or if
                      the gate is active but either side has no resolvable PSGC codes
                      (non-blocking skip); True if a PSGC prefix match is found;
                      False if the gate is active, both sides have codes, and no
                      prefix matches at any level (hard reject)
      psgc_match_level — finest prefix level that matched (2, 4, or 10); None when
                         psgc_gate is not True

    is_match is True only when every applicable gate passes (no gate is False).
    """
    date_days_delta:  int | None
    date_gate:        bool | None
    type_gate:        bool | None
    label_similarity: float | None
    proper_noun_hit:  bool | None
    label_gate:       bool | None
    location_gate:    bool | None
    psgc_gate:        bool | None
    psgc_match_level: int | None
    is_match:         bool

    def as_dict(self) -> dict[str, int | float | bool | None]:
        return {
            "date_days_delta":  self.date_days_delta,
            "date_gate":        self.date_gate,
            "type_gate":        self.type_gate,
            "label_similarity": round(self.label_similarity, 4) if self.label_similarity is not None else None,
            "proper_noun_hit":  self.proper_noun_hit,
            "label_gate":       self.label_gate,
            "location_gate":    self.location_gate,
            "psgc_gate":        self.psgc_gate,
            "psgc_match_level": self.psgc_match_level,
            "is_match":         self.is_match,
        }


# ── Gate 1: Date ──────────────────────────────────────────────────────────────

def _gate_date(a: DisasterEvent, b: DisasterEvent) -> tuple[int | None, bool | None]:
    if not a.start_date or not b.start_date:
        return None, None
    delta = abs((a.start_date - b.start_date).days)
    return delta, delta <= DATE_HARD_GATE_DAYS


# ── Gate 2: Disaster type ─────────────────────────────────────────────────────

def _resolve_concept(event: DisasterEvent) -> str | None:
    if event.disaster_type_uri:
        local = re.sub(r"^.*[/#]", "", str(event.disaster_type_uri))
        if local in DISASTER_TYPE_HIERARCHY:
            return local
    if event.disaster_type:
        if event.disaster_type in DISASTER_TYPE_HIERARCHY:
            return event.disaster_type
        normalised = event.disaster_type.lower().replace("_", " ")
        return DISASTER_TYPE_ALIASES.get(normalised)
    return None


def _gate_type(a: DisasterEvent, b: DisasterEvent) -> bool | None:
    ca = _resolve_concept(a)
    cb = _resolve_concept(b)

    if not ca or not cb:
        return None

    if ca == cb:
        return True

    parent_a = DISASTER_TYPE_HIERARCHY.get(ca)
    parent_b = DISASTER_TYPE_HIERARCHY.get(cb)

    if parent_a is not None and parent_a == parent_b:
        return True

    if ca == parent_b or cb == parent_a:
        return True

    return False


# ── Gate 3: Label / location ──────────────────────────────────────────────────

def _proper_nouns(raw_label: str) -> set[str]:
    tokens = re.findall(r"[A-Z][A-Za-z0-9\-]*", raw_label)
    return {t for t in tokens if len(t) >= PROPER_NOUN_MIN_LEN}


def _gate_label(
    a: DisasterEvent, b: DisasterEvent
) -> tuple[float | None, bool | None, bool | None, bool | None]:
    if a.label and b.label:
        sim = max(
            fuzz.token_sort_ratio(normalize_text(a.label), normalize_text(b.label)) / 100.0,
            fuzz.token_set_ratio(normalize_text(a.label), normalize_text(b.label)) / 100.0,
            fuzz.partial_ratio(normalize_text(a.label), normalize_text(b.label))  / 100.0,
        )
        pn_a = _proper_nouns(a.label)
        pn_b = _proper_nouns(b.label)
        pn_hit = bool(pn_a & pn_b)
        label_gate = sim >= LABEL_FUZZY_THRESHOLD or pn_hit
        return sim, pn_hit, label_gate, None

    else:
        if not a.location and not b.location:
            return None, None, None, None

        if not a.location or not b.location:
            return None, None, None, None

        toks_a = {t for t in normalize_text(a.location).split() if len(t) >= 3}
        toks_b = {t for t in normalize_text(b.location).split() if len(t) >= 3}
        loc_gate = bool(toks_a & toks_b) if toks_a and toks_b else None
        return None, None, None, loc_gate


# ── Gate 4: PSGC location (Incident pairs only) ───────────────────────────────

def _psgc_from_uri(uri: URIRef) -> str | None:
    """
    Extract the 10-digit PSGC code from a sakuna.ph location URI.

    URIs follow the pattern https://sakuna.ph/XXXXXXXXXX where the path
    fragment is the 10-digit PSGC code (e.g. '0102801000').
    Returns None for any URI whose fragment is not exactly 10 digits.
    """
    fragment = str(uri).rsplit("/", 1)[-1]
    if fragment.isdigit() and len(fragment) == 10:
        return fragment
    return None


def _gate_psgc(
    a: DisasterEvent, b: DisasterEvent
) -> tuple[bool | None, int | None]:
    """
    Gate 4: PSGC hierarchical location match for Incident-involved pairs.

    Activation: gate runs when either event has rdf_type == RDF_TYPE_INCIDENT.
    If neither is an Incident, returns (None, None) — gate inactive.

    When active:
      - Collect valid 10-digit PSGC codes from each event's location_uris.
      - If either side yields no codes → (None, None) — non-blocking skip,
        since we cannot confirm or deny co-location without data.
      - Otherwise check every code pair for a shared prefix at each level
        in PSGC_MATCH_LEVELS (2=region, 4=province, 10=exact).
        → (True, best_level) if any pair matches at any level.
        → (False, None) if codes exist on both sides but nothing matches —
          hard reject, the incidents are in different locations.

    Examples:
      Road accident in Cebu City (0730600000) vs road accident in Davao (1130700000)
        region "07" ≠ "11" at any level → (False, None)  ← hard reject

      Road accident in Cebu City (0730600000) vs road accident in Danao City (0702223000)
        region "07" == "07" → (True, 2)

      Two floods (MajorEvent + MajorEvent) anywhere
        gate inactive → (None, None)
    """
    if a.rdf_type != RDF_TYPE_INCIDENT and b.rdf_type != RDF_TYPE_INCIDENT:
        return None, None

    codes_a = [c for c in (_psgc_from_uri(u) for u in a.location_uris) if c]
    codes_b = [c for c in (_psgc_from_uri(u) for u in b.location_uris) if c]

    # No codes on either side — skip rather than penalise missing data
    if not codes_a or not codes_b:
        return None, None

    best_level: int | None = None
    for code_a in codes_a:
        for code_b in codes_b:
            for level in PSGC_MATCH_LEVELS:
                if code_a[:level] == code_b[:level]:
                    if best_level is None or level > best_level:
                        best_level = level
                    # no break — check all levels to find the finest match

    if best_level is not None:
        return True, best_level
    return False, None


# ── Main scorer ───────────────────────────────────────────────────────────────

def score_pair(a: DisasterEvent, b: DisasterEvent) -> ScoreBreakdown:
    """
    Run all four gates and determine if the pair is a match.

    Gate evaluation:
      - False  → immediate rejection.
      - None   → missing data or gate inactive; skipped (non-blocking).
      - True   → passes.

    is_match = True only when no gate returns False.
    """
    delta_days, date_gate                       = _gate_date(a, b)
    type_gate                                   = _gate_type(a, b)
    sim, pn_hit, label_gate, location_gate      = _gate_label(a, b)
    psgc_gate, psgc_match_level                 = _gate_psgc(a, b)

    gates = [date_gate, type_gate, label_gate, location_gate, psgc_gate]
    is_match = all(g is not False for g in gates)

    return ScoreBreakdown(
        date_days_delta=delta_days,
        date_gate=date_gate,
        type_gate=type_gate,
        label_similarity=sim,
        proper_noun_hit=pn_hit,
        label_gate=label_gate,
        location_gate=location_gate,
        psgc_gate=psgc_gate,
        psgc_match_level=psgc_match_level,
        is_match=is_match,
    )


def score_all_pairs(
    pairs: list[tuple[DisasterEvent, DisasterEvent]],
    verbose: bool = False,
) -> list[tuple[DisasterEvent, DisasterEvent, ScoreBreakdown]]:

    results: list[tuple[DisasterEvent, DisasterEvent, ScoreBreakdown]] = []

    for a, b in pairs:
        breakdown = score_pair(a, b)
        results.append((a, b, breakdown))
        if verbose and breakdown.is_match:
            print(
                f"  MATCH  {a.source}/{a.label!r}/{a.uri} <-> {b.source}/{b.label!r}/{b.uri}"
                f"  Δ{breakdown.date_days_delta}d"
                f"  type={breakdown.type_gate}"
                f"  label_sim={breakdown.label_similarity}"
                f"  pn={breakdown.proper_noun_hit}"
                f"  loc={breakdown.location_gate}"
                f"  psgc={breakdown.psgc_gate}(L{breakdown.psgc_match_level})"
            )
    return results


# ══════════════════════════════════════════════════════════════════════════════
# 6. ALIGNER
# ══════════════════════════════════════════════════════════════════════════════

import json

def pick_canonical(cluster: set[URIRef]) -> URIRef:
    """Pick canonical URI from a sameAs cluster using SOURCE_PRIORITY."""
    for source in SOURCE_PRIORITY:
        for uri in sorted(cluster):
            if source.lower() in str(uri).lower():
                return uri
    return sorted(cluster)[0]

T = TypeVar("T")

class UnionFind(Generic[T]):
    def __init__(self) -> None:
        self.parent: Dict[T, T] = {}

    def find(self, x: T) -> T:
        if x not in self.parent:
            self.parent[x] = x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a: T, b: T) -> None:
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a != root_b:
            self.parent[root_b] = root_a

def expand_clusters(
    scored_pairs: list[tuple[DisasterEvent, DisasterEvent, ScoreBreakdown]],
    registry: dict[str, frozenset[str]],
) -> tuple[
    list[tuple[URIRef, frozenset[URIRef]]],  # expanded existing clusters
    list[tuple[DisasterEvent, DisasterEvent, ScoreBreakdown]],                        # remaining new-vs-new matches
]:
    # build reverse lookup: member_uri -> canonical_iri
    member_to_canonical: dict[str, str] = {
        member: canonical
        for canonical, members in registry.items()
        for member in members
    }

    expanded: dict[str, set[str] | None] = {}   # canonical_iri -> updated member set
    remaining: list[tuple[DisasterEvent, DisasterEvent, ScoreBreakdown]] = []

    for a, b, sc in scored_pairs:
        if not sc.is_match:
            continue

        a_known = a.uri in member_to_canonical
        b_known = b.uri in member_to_canonical

        if a_known and b_known:
            # both already clustered — could be same or different clusters
            canon_a = member_to_canonical[a.uri]
            canon_b = member_to_canonical[b.uri]
            if canon_a != canon_b:
                # two existing clusters now found to match — merge them
                # keep canon_a, absorb canon_b's members into it
                merged = (
                    set(registry[canon_a])
                    | set(registry[canon_b])
                )
                expanded[canon_a] = merged
                # mark canon_b as absorbed so save_registry can drop it
                expanded[canon_b] = None  
            continue

        if a_known:
            canon = member_to_canonical[a.uri]
            expanded.setdefault(canon, set(registry[canon]))
            expanded[canon].add(b.uri)
        elif b_known:
            canon = member_to_canonical[b.uri]
            expanded.setdefault(canon, set(registry[canon]))
            expanded[canon].add(a.uri)
        else:
            remaining.append((a, b, sc))

    # rebuild as (canonical_iri, frozenset) — drop absorbed clusters
    updated_clusters = [
        (canonical, frozenset(members))
        for canonical, members in expanded.items()
        if members is not None
    ]

    return updated_clusters, remaining


def build_clusters(
    matches: list[tuple[DisasterEvent, DisasterEvent, ScoreBreakdown]]
) -> list[tuple[URIRef, frozenset[URIRef]]]:

    uf = UnionFind[URIRef]()
    for a, b, score in matches:
        if score.is_match:
            uf.union(a.uri, b.uri)

    clusters: Dict[URIRef, Set[URIRef]] = defaultdict(set)
    for uri in uf.parent:
        root = uf.find(uri)
        clusters[root].add(uri)

    return [
        (mint_canonical_iri(frozenset(members)), frozenset(members))
        for members in clusters.values()
    ]


def write_alignments(
    clusters: list[tuple[URIRef, frozenset[URIRef]]],
    output_path: Path,
) -> Graph:
    """Write owl:sameAs alignment triples + gate diagnostics metadata to TTL."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    g = Graph()
    g.bind("",     SKG)
    g.bind("owl",  OWL)
    g.bind("skos", SKOS)
    g.bind("prov", PROV)
    g.bind("xsd",  XSD)

    run_uri = URIRef(
        f"{SAKUNA_NS}alignment-run/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    )
    g.add((run_uri, RDF.type,       PROV.Activity))
    g.add((run_uri, SKG.entryDate, Literal(datetime.now(timezone.utc).isoformat(), datatype=XSD.dateTime)))
    g.add((run_uri, RDFS.label,     Literal("SakunaGraPH entity alignment run")))


    for canonical, members in clusters:
        
        canonical = URIRef(canonical)

        g.add((canonical, RDF.type, SKG.DisasterEvent)) # commond ID
        member_list = list(members)

        stmt_uri = URIRef(
                f"{SAKUNA_NS}alignment/"
                f"{str(canonical).split('/')[-1]}"
        )

        g.add((stmt_uri, RDF.type,      RDF.Statement))
        g.add((stmt_uri, RDF.subject,   canonical))
        g.add((stmt_uri, RDF.predicate, PROV.alternateOf))
        
        # common uri link 

        for member in members:
            member = URIRef(member)

            g.add((canonical, PROV.alternateOf, member))
            g.add((stmt_uri, RDF.object, member))
            g.add((stmt_uri, PROV.wasGeneratedBy, run_uri))
        
        # event link
        for a, b in combinations(member_list, 2):
            a = URIRef(a)
            b = URIRef(b)
            g.add((a, PROV.alternateOf, b))
            g.add((b, PROV.alternateOf, a))

    g.serialize(str(output_path), format="turtle")
    print(f"  Wrote {len(clusters)} clusters -> {output_path}")
    return g


def save_registry(
    clusters: list[tuple[URIRef, frozenset[URIRef]]],
    path: Path
) -> None:
    registry = {
        canonical_iri: sorted(members)  # sorted for deterministic JSON
        for canonical_iri, members in clusters
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2)


def load_registry(path: Path) -> dict[str, frozenset[str]]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return {
        canonical_iri: frozenset(members)
        for canonical_iri, members in raw.items()
    }


def get_known_pairs(registry: dict[str, frozenset[str]]) -> set[str]:
    known_members: set[str] = set()
    for members in registry.values():
        known_members.update(members)
    return known_members

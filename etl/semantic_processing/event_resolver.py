"""
er.py — SakunaGraPH entity resolution module.

Combines: config, models, extractor, blocker, scorer, aligner, merger.

Resolution criteria (all gates must pass):
  0. Report level   - skips ndrrmc incident types derived from tables
  1. Date gate      — |start_date_a - start_date_b| ≤ DATE_HARD_GATE_DAYS (hard reject)
  2. Type gate      — same concept OR same parent concept (hard reject on mismatch)
  3. Label/location — if both eventNames present: fuzzy ≥ threshold OR shared proper noun
                      if absent: at least one location token overlaps (skipped if both missing)

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
from typing import Dict, Generic, TypeVar, Set
from collections import defaultdict

# Earlier in list = higher authority
SOURCE_PRIORITY = ["ndrrmc", "dromic", "emdat", "gda"]

# Blocking
BLOCKING_DATE_TOLERANCE_DAYS = 5   # mirrors DATE_HARD_GATE_DAYS — no point surfacing pairs we'll reject
BLOCKING_LOCATION_PREFIX_LEN = 4

# ── Resolution gates ──────────────────────────────────────────────────────────
DATE_HARD_GATE_DAYS = 5            # Gate 1: dates must be within this many days
                                    # Gate 2: disaster type must be exact or same parent
LABEL_FUZZY_THRESHOLD = 0.72        # Gate 3a: minimum fuzzy similarity when both labels present
PROPER_NOUN_MIN_LEN   = 4           # Gate 3a: minimum length for a capitalised token to count
                                    # Gate 3b (no label): any overlapping location token = pass

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
# Gate 2 logic:
#   same concept                       → pass (type_match = True)
#   same parent (sibling concepts)     → pass (type_match = True)
#   different parent or unresolvable   → fail (hard reject)

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
    blocking_keys:     list[str]        = field(default_factory=lambda: [])

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
from mappings.graph import SKG, GEO, PROV

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
    events: list[DisasterEvent] = []
    event_uris: set[URIRef] = set()

    # Skip NDRRMC incidents (not report level)
    for s, _, o in g.triples((None, RDF.type, None)):
        if not isinstance(s, URIRef):
            continue
        type_str = str(o)
        if type_str in (f"{SAKUNA_NS}DisasterEvent", f"{SAKUNA_NS}MajorEvent"):
            event_uris.add(s)
        elif type_str == f"{SAKUNA_NS}Incident" and source != "ndrrmc":
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

        # ── Location ──────────────────────────────────────────────────────────
        location = _get_literal(g, uri,
            SKG.hasLocation,
            RDFS.label,          # some sources embed location in label — handled at scoring
        )

        events.append(DisasterEvent(
            uri=uri, source=inferred_source,
            label=label, disaster_type=disaster_type,
            disaster_type_uri=disaster_type_uri,
            start_date=start_date, end_date=end_date,
            location=location,
        ))

    return events


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
            # Pre-filter using the same hard date gate used in scoring.
            # Pairs missing both dates are kept — the scorer handles that case.
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
      date_gate   — None if either date missing, True/False otherwise
      type_gate   — None if either type unresolvable, True/False otherwise
      label_gate  — None if used location fallback or skipped, True/False otherwise
      location_gate — None if both labels were present (gate not used),
                      True/False when used as fallback; also None if both missing
    is_match is True only when every applicable gate passes.
    """
    date_days_delta:  int | None       # absolute day difference (None if missing)
    date_gate:        bool | None
    type_gate:        bool | None
    label_similarity: float | None     # fuzzy score (None if labels absent)
    proper_noun_hit:  bool | None      # shared capitalised token (None if absent)
    label_gate:       bool | None
    location_gate:    bool | None
    is_match:         bool

    def as_dict(self) -> dict:
        return {
            "date_days_delta":  self.date_days_delta,
            "date_gate":        self.date_gate,
            "type_gate":        self.type_gate,
            "label_similarity": round(self.label_similarity, 4) if self.label_similarity is not None else None,
            "proper_noun_hit":  self.proper_noun_hit,
            "label_gate":       self.label_gate,
            "location_gate":    self.location_gate,
            "is_match":         self.is_match,
        }


# ── Gate 1: Date ──────────────────────────────────────────────────────────────

def _gate_date(a: DisasterEvent, b: DisasterEvent) -> tuple[int | None, bool | None]:
    """
    Returns (delta_days, gate_result).
    gate_result is None when either date is missing (treated as non-blocking —
    the pair is not outright rejected, but contributes no positive signal).
    """
    if not a.start_date or not b.start_date:
        return None, None
    delta = abs((a.start_date - b.start_date).days)
    return delta, delta <= DATE_HARD_GATE_DAYS


# ── Gate 2: Disaster type ─────────────────────────────────────────────────────

def _resolve_concept(event: DisasterEvent) -> str | None:
    """
    Get the best concept local name for an event's disaster type.
    Prefers the local name derived from the URI; falls back to normalised alias.
    """
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
    """
    Gate 2: same concept OR same direct parent.
    Returns None when either concept is unresolvable (non-blocking).

    Examples that PASS:
      FlashFlood  vs RiverineFlood  → both children of Flood         → True
      TropicalCyclone vs StormSurge → both children of Storm         → True
      Flood vs Flood                → exact match                     → True

    Examples that FAIL:
      Flood vs Earthquake           → different parents               → False
      Flood vs Storm                → Flood's parent ≠ Storm          → False
    """
    ca = _resolve_concept(a)
    cb = _resolve_concept(b)

    if not ca or not cb:
        return None  # unresolvable — do not hard-reject

    if ca == cb:
        return True  # exact match

    parent_a = DISASTER_TYPE_HIERARCHY.get(ca)
    parent_b = DISASTER_TYPE_HIERARCHY.get(cb)

    # Sibling: both share the same direct parent
    if parent_a is not None and parent_a == parent_b:
        return True

    # One is the direct parent of the other (e.g. Flood vs FlashFlood)
    if ca == parent_b or cb == parent_a:
        return True

    return False


# ── Gate 3: Label / location ──────────────────────────────────────────────────

def _proper_nouns(raw_label: str) -> set[str]:
    """
    Extract capitalised tokens of length ≥ PROPER_NOUN_MIN_LEN from the
    original (non-lowercased) label string.

    Rationale: typhoon names (Odette, Rolly), province names (Leyte, Samar),
    and NDRRMC event codes (PAB-001) all start with a capital letter and are
    ≥4 characters. We intentionally skip lowercasing here so that function
    words accidentally capitalised at sentence start are excluded by the
    length filter in practice.
    """
    tokens = re.findall(r"[A-Z][A-Za-z0-9\-]*", raw_label)
    return {t for t in tokens if len(t) >= PROPER_NOUN_MIN_LEN}


def _gate_label(
    a: DisasterEvent, b: DisasterEvent
) -> tuple[float | None, bool | None, bool | None, bool | None]:
    """
    Gate 3 dispatcher.

    Returns:
      (label_similarity, proper_noun_hit, label_gate, location_gate)

    When both labels are present:
      → label_gate = fuzzy ≥ threshold OR proper noun overlap
      → location_gate = None (gate not used)

    When either label is absent:
      → label_gate = None (gate not used)
      → location_gate = True if any location token overlaps, False if both
        locations present but no overlap, None if both locations missing.
    """
    if a.label and b.label:
        # ── 3a: label path ────────────────────────────────────────────
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
        # ── 3b: location fallback ─────────────────────────────────────
        if not a.location and not b.location:
            return None, None, None, None  # nothing to check — non-blocking

        if not a.location or not b.location:
            # One has location, one doesn't — treat as weak evidence, don't reject
            return None, None, None, None

        toks_a = set(normalize_text(a.location).split())
        toks_b = set(normalize_text(b.location).split())
        # Remove very short tokens (articles, "of", etc.)
        toks_a = {t for t in toks_a if len(t) >= 3}
        toks_b = {t for t in toks_b if len(t) >= 3}

        loc_gate = bool(toks_a & toks_b) if toks_a and toks_b else None
        return None, None, None, loc_gate


# ── Main scorer ───────────────────────────────────────────────────────────────

def score_pair(a: DisasterEvent, b: DisasterEvent) -> ScoreBreakdown:
    """
    Run all three gates in sequence and determine if the pair is a match.

    Gate evaluation:
      - A gate returning False  → immediate rejection (is_match = False).
      - A gate returning None   → missing data; gate is skipped (non-blocking).
      - A gate returning True   → passes.

    is_match = True only when no gate returns False (i.e. all applicable
    gates pass, even if some were skipped due to missing data).

    This intentionally allows matches on sparse data: if two events share
    the same disaster type and fall within 5 days but have no labels or
    locations recorded, they are still considered a candidate match.
    Downstream review (or provenance weighting in the merger) handles
    low-confidence sparse matches.
    """ 

    delta_days, date_gate    = _gate_date(a, b)
    type_gate                = _gate_type(a, b)
    sim, pn_hit, label_gate, location_gate = _gate_label(a, b)

    gates = [date_gate, type_gate, label_gate, location_gate]
    is_match = all(g is not False for g in gates)

    return ScoreBreakdown(
        date_days_delta=delta_days,
        date_gate=date_gate,
        type_gate=type_gate,
        label_similarity=sim,
        proper_noun_hit=pn_hit,
        label_gate=label_gate,
        location_gate=location_gate,
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
            )
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

def build_clusters(
    matches: list[tuple[DisasterEvent, DisasterEvent, ScoreBreakdown]]
) -> list[tuple[URIRef, set[URIRef]]]:

    uf = UnionFind[URIRef]()

    for a, b, score in matches:
        if score.is_match:
            uf.union(a.uri, b.uri)

    groups: Dict[URIRef, Set[URIRef]] = defaultdict(set)

    for uri in uf.parent:
        root = uf.find(uri)
        groups[root].add(uri)

    clusters: list[tuple[URIRef, set[URIRef]]] = []

    for component in groups.values():
        canonical = pick_canonical(component)
        clusters.append((canonical, component))

    return clusters


def write_alignments(
    matches: list[tuple[DisasterEvent, DisasterEvent, ScoreBreakdown]],
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

    clusters = build_clusters(matches)

    for canonical, cluster in clusters:
        for other in sorted(cluster - {canonical}):
            g.add((canonical, OWL.sameAs, other))

            stmt_uri = URIRef(
                f"{SAKUNA_NS}alignment/"
                f"{str(canonical).split('/')[-1]}--{str(other).split('/')[-1]}"
            )
            g.add((stmt_uri, RDF.type,      RDF.Statement))
            g.add((stmt_uri, RDF.subject,   canonical))
            g.add((stmt_uri, RDF.predicate, OWL.sameAs))
            g.add((stmt_uri, RDF.object,    other))
            g.add((stmt_uri, PROV.wasGeneratedBy, run_uri))

    g.serialize(str(output_path), format="turtle")
    print(f"  Wrote {len(clusters)} clusters -> {output_path}")
    return g


def save_registry(
    clusters: list[tuple[URIRef, set[URIRef]]],
    registry_path: Path,
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


def load_registry(registry_path: Path) -> dict[str, list[str]]:
    if not registry_path.exists():
        return {}
    with open(registry_path) as f:
        return json.load(f)


def get_known_pairs(registry: dict[str, list[str]]) -> set[frozenset[str]]:
    known: set[frozenset[str]] = set()
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


def build_uri_to_canonical(alignment_graph: Graph) -> Dict[URIRef, URIRef]:
    """Read owl:sameAs triples, build connected components, return uri -> canonical map."""

    uf = UnionFind[URIRef]()

    # Union all sameAs pairs
    for s, _, o in alignment_graph.triples((None, OWL.sameAs, None)):
        if isinstance(s, URIRef) and isinstance(o, URIRef):
            uf.union(s, o)

    # 2. Group by root
    groups: Dict[URIRef, Set[URIRef]] = defaultdict(set)
    for uri in uf.parent:
        root = uf.find(uri)
        groups[root].add(uri)

    # 3. Pick canonical per component
    uri_to_canonical: Dict[URIRef, URIRef] = {}

    for component in groups.values():
        canonical = min(
            component,
            key=lambda u: (_infer_source_rank(u), str(u))
        )
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
    sources_dir:     Path,
    alignments_path: Path,
    output_path:     Path,
) -> Graph:
    """
    Full merge pipeline:
      load sources -> remap URIs -> resolve conflicts -> serialize canonical.ttl
    """
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
    merged.bind("",    SKG)
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
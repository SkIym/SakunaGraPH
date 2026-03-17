"""Cross-source entity resolution: match the same disaster event across NDRRMC, EM-DAT, and GDA.

Produces owl:sameAs triples linking matched event IRIs from different sources.
"""

import json
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from rdflib import Graph, URIRef, Literal, RDF, OWL, XSD
from thefuzz import fuzz


SKG = "https://sakuna.ph/"

# Philippine ↔ international typhoon name mapping
# Common typhoons that appear across sources
TYPHOON_NAME_MAP: dict[str, str] = {}
_TYPHOON_NAMES_PATH = os.path.join(os.path.dirname(__file__), "typhoon_names.json")
if os.path.exists(_TYPHOON_NAMES_PATH):
    with open(_TYPHOON_NAMES_PATH, "r") as f:
        TYPHOON_NAME_MAP = json.load(f)


@dataclass
class EventRecord:
    """Minimal event representation for matching."""
    uri: str
    name: str
    start_date: date | None
    end_date: date | None
    disaster_type: str | None
    locations: list[str] = field(default_factory=list)
    source: str = ""  # "ndrrmc", "emdat", "gda"


@dataclass
class MatchResult:
    """A matched pair of events from different sources."""
    uri_a: str
    uri_b: str
    score: float
    source_a: str
    source_b: str


def _parse_date(value) -> date | None:
    """Parse an RDF date literal to a Python date."""
    if value is None:
        return None
    s = str(value)
    try:
        # Handle both xsd:date and xsd:dateTime
        return date.fromisoformat(s[:10])
    except (ValueError, IndexError):
        return None


def _extract_events(g: Graph, source: str) -> list[EventRecord]:
    """Extract EventRecords from an RDF graph."""
    skg = SKG
    events = []

    for uri in g.subjects(RDF.type, URIRef(skg + "MajorEvent")):
        name = ""
        start = None
        end = None
        dtype = None
        locations = []

        for _, p, o in g.triples((uri, None, None)):
            p_str = str(p)
            if p_str == skg + "eventName":
                name = str(o)
            elif p_str == skg + "startDate":
                start = _parse_date(o)
            elif p_str == skg + "endDate":
                end = _parse_date(o)
            elif p_str == skg + "hasDisasterType":
                dtype = str(o).replace(skg, "")
            elif p_str == skg + "hasLocation":
                locations.append(str(o))

        events.append(EventRecord(
            uri=str(uri),
            name=name,
            start_date=start,
            end_date=end,
            disaster_type=dtype,
            locations=locations,
            source=source,
        ))

    return events


def _date_proximity(a: EventRecord, b: EventRecord) -> float:
    """Score date proximity (0-1). 1.0 = same day, 0.0 = >30 days apart."""
    if a.start_date is None or b.start_date is None:
        return 0.0

    diff = abs((a.start_date - b.start_date).days)
    if diff == 0:
        return 1.0
    if diff > 30:
        return 0.0
    return 1.0 - (diff / 30.0)


def _name_similarity(a: EventRecord, b: EventRecord) -> float:
    """Score name similarity (0-1), accounting for PH ↔ international typhoon names."""
    if not a.name or not b.name:
        return 0.0

    # Direct fuzzy match
    direct_score = fuzz.token_sort_ratio(a.name.lower(), b.name.lower()) / 100.0

    # Check typhoon name mapping
    mapping_score = 0.0
    a_lower = a.name.lower()
    b_lower = b.name.lower()

    for ph_name, intl_name in TYPHOON_NAME_MAP.items():
        ph_lower = ph_name.lower()
        intl_lower = intl_name.lower()
        if (ph_lower in a_lower and intl_lower in b_lower) or \
           (intl_lower in a_lower and ph_lower in b_lower):
            mapping_score = 1.0
            break

    return max(direct_score, mapping_score)


def _location_overlap(a: EventRecord, b: EventRecord) -> float:
    """Jaccard index of location IRIs."""
    if not a.locations or not b.locations:
        return 0.0

    set_a = set(a.locations)
    set_b = set(b.locations)
    intersection = set_a & set_b

    if not intersection:
        return 0.0

    return len(intersection) / len(set_a | set_b)


class EventMatcher:
    """Match events across sources using temporal, name, and location signals."""

    def __init__(
        self,
        date_weight: float = 0.3,
        name_weight: float = 0.4,
        location_weight: float = 0.3,
        threshold: float = 0.7,
        date_window_days: int = 7,
    ):
        self.date_weight = date_weight
        self.name_weight = name_weight
        self.location_weight = location_weight
        self.threshold = threshold
        self.date_window_days = date_window_days

    def _candidates(
        self, events_a: list[EventRecord], events_b: list[EventRecord]
    ) -> list[tuple[EventRecord, EventRecord]]:
        """Generate candidate pairs filtered by temporal overlap + same disaster type."""
        pairs = []
        for a in events_a:
            for b in events_b:
                # Same disaster type filter (skip if unknown)
                if a.disaster_type and b.disaster_type and a.disaster_type != b.disaster_type:
                    continue

                # Temporal window filter
                if a.start_date and b.start_date:
                    diff = abs((a.start_date - b.start_date).days)
                    if diff > self.date_window_days:
                        continue

                pairs.append((a, b))

        return pairs

    def match(
        self, events_a: list[EventRecord], events_b: list[EventRecord]
    ) -> list[MatchResult]:
        """Match events from two sources. Returns list of MatchResults above threshold."""
        results = []

        for a, b in self._candidates(events_a, events_b):
            score = (
                self.date_weight * _date_proximity(a, b) +
                self.name_weight * _name_similarity(a, b) +
                self.location_weight * _location_overlap(a, b)
            )

            if score >= self.threshold:
                results.append(MatchResult(
                    uri_a=a.uri,
                    uri_b=b.uri,
                    score=score,
                    source_a=a.source,
                    source_b=b.source,
                ))

        return results

    def match_all(
        self,
        ndrrmc_events: list[EventRecord],
        emdat_events: list[EventRecord],
        gda_events: list[EventRecord],
    ) -> list[MatchResult]:
        """Match events across all three source pairs."""
        results = []
        results.extend(self.match(ndrrmc_events, emdat_events))
        results.extend(self.match(ndrrmc_events, gda_events))
        results.extend(self.match(emdat_events, gda_events))
        return results


def load_events_from_rdf(path: str, source: str) -> list[EventRecord]:
    """Load EventRecords from an RDF file (Turtle or N-Triples)."""
    g = Graph()

    if path.endswith(".nt"):
        g.parse(path, format="nt")
    else:
        g.parse(path, format="turtle")

    return _extract_events(g, source)


def matches_to_rdf(matches: list[MatchResult]) -> Graph:
    """Convert match results to an RDF graph with owl:sameAs triples."""
    g = Graph()
    g.bind("owl", OWL)

    for m in matches:
        g.add((URIRef(m.uri_a), OWL.sameAs, URIRef(m.uri_b)))

    return g

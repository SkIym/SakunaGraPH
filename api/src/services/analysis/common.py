import json
import re
from dataclasses import dataclass
from datetime import date

from src.schemas.analysis import AnalysisEventType
from src.services.common import ServiceError

SPARQL_PREFIXES = """PREFIX :     <https://sakuna.ph/>
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
"""

_PSGC_RE = re.compile(r"^\d{10}$")
_LOCAL_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9._~-]*$")


@dataclass(frozen=True)
class AnalysisFilters:
    event_type: AnalysisEventType = "all"
    start_date: date | None = None
    end_date: date | None = None
    location_ids: tuple[str, ...] = ()
    disaster_types: tuple[str, ...] = ()
    q: str | None = None


def make_analysis_filters(
    *,
    event_type: AnalysisEventType = "all",
    start_date: date | None = None,
    end_date: date | None = None,
    location_ids: list[str] | None = None,
    disaster_types: list[str] | None = None,
    q: str | None = None,
) -> AnalysisFilters:
    if start_date and end_date and start_date > end_date:
        raise ServiceError(422, "start_date must be on or before end_date")

    normalized_locations = tuple(sorted(set(location_ids or [])))
    for location_id in normalized_locations:
        if not _PSGC_RE.fullmatch(location_id):
            raise ServiceError(422, "location_ids values must be exactly 10 digits")

    normalized_types = tuple(sorted(set(disaster_types or [])))
    for disaster_type in normalized_types:
        if not _LOCAL_NAME_RE.fullmatch(disaster_type):
            raise ServiceError(
                422,
                "disaster_types values must be local taxonomy ids",
            )

    normalized_q = q.strip() if q else None
    return AnalysisFilters(
        event_type=event_type,
        start_date=start_date,
        end_date=end_date,
        location_ids=normalized_locations,
        disaster_types=normalized_types,
        q=normalized_q or None,
    )


def sparql_string(value: str) -> str:
    """Return a SPARQL-safe double-quoted string literal."""
    return json.dumps(value, ensure_ascii=False)


def local_name(iri: str | None) -> str:
    if not iri:
        return ""
    return iri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]


def date_only(value: str | None) -> str | None:
    if not value:
        return None
    return value.split("T", 1)[0]


def source_from_event_iri(event_iri: str) -> str | None:
    parts = event_iri.rstrip("/").split("/")
    labels = {
        "dromic": "DROMIC",
        "emdat": "EM-DAT",
        "gda": "GDA",
        "ndrrmc": "NDRRMC",
    }
    return next((labels[part.lower()] for part in parts if part.lower() in labels), None)


def event_filter_where(filters: AnalysisFilters) -> str:
    event_classes = {
        "major": ":MajorEvent",
        "incidents": ":Incident",
        "all": ":MajorEvent :Incident",
    }[filters.event_type]

    fragments = [
        f"VALUES ?eventClass {{ {event_classes} }}",
        "?event a ?eventClass ;\n         :startDate ?startDate .",
        "OPTIONAL { ?event :eventName ?eventNameValue }",
        "OPTIONAL { ?event :incidentDescription ?incidentDescription }",
        "BIND(COALESCE(?eventNameValue, ?incidentDescription) AS ?eventName)",
        "OPTIONAL { ?event :endDate ?endDate }",
        """FILTER NOT EXISTS {
  ?event prov:alternateOf ?alternateCandidate .
  ?alternateCandidate :startDate ?alternateStartDate .
  FILTER(
    SUBSTR(STR(?alternateStartDate), 1, 10) < SUBSTR(STR(?startDate), 1, 10) ||
    (
      SUBSTR(STR(?alternateStartDate), 1, 10) = SUBSTR(STR(?startDate), 1, 10) &&
      STR(?alternateCandidate) < STR(?event)
    )
  )
}""",
    ]

    if filters.start_date:
        fragments.append(
            'FILTER(SUBSTR(STR(?startDate), 1, 10) >= '
            f'"{filters.start_date.isoformat()}")'
        )
    if filters.end_date:
        fragments.append(
            'FILTER(SUBSTR(STR(?startDate), 1, 10) <= '
            f'"{filters.end_date.isoformat()}")'
        )

    if filters.location_ids:
        selected_locations = " ".join(f":{value}" for value in filters.location_ids)
        fragments.append(
            f"""FILTER EXISTS {{
  VALUES ?selectedLocation {{ {selected_locations} }}
  ?event :hasLocation ?filterLocation .
  ?filterLocation :isPartOf* ?selectedLocation .
}}"""
        )

    if filters.disaster_types:
        selected_types = " ".join(f":{value}" for value in filters.disaster_types)
        fragments.append(
            f"""FILTER EXISTS {{
  VALUES ?selectedDisasterType {{ {selected_types} }}
  ?event (:hasDisasterType|:hasDisasterSubtype) ?filterDisasterType .
  ?filterDisasterType skos:broader* ?selectedDisasterType .
}}"""
        )

    if filters.q:
        query_literal = sparql_string(filters.q)
        fragments.append(
            "FILTER(CONTAINS("
            "LCASE(CONCAT(COALESCE(STR(?eventName), \"\"), \" \", STR(?event))), "
            f"LCASE({query_literal})"
            "))"
        )

    return "\n".join(fragments)

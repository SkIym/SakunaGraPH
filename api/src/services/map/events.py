import asyncio
import re
import time
from typing import Any, Literal

from src.services.common import ServiceError
from src.services.sparql import execute_sparql

PAGE_SIZE = 10
_CACHE_TTL = 300
_PSGC_RE = re.compile(r"^\d{10}$")

EventType = Literal["MajorEvent", "Incident"]
EventMode = Literal["major", "incidents"]
EventScope = Literal["region", "province"]


class _TTLCache:
    def __init__(self) -> None:
        self._store: dict[tuple, tuple[float, Any]] = {}

    def get(self, key: tuple) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        ts, val = entry
        if time.monotonic() - ts > _CACHE_TTL:
            del self._store[key]
            return None
        return val

    def set(self, key: tuple, val: Any) -> None:
        self._store[key] = (time.monotonic(), val)


_cache = _TTLCache()

_PREFIXES = """PREFIX :    <https://sakuna.ph/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX prov: <http://www.w3.org/ns/prov#>
"""


def _events_query(psgc: str, event_type: EventType, limit: int, offset: int) -> str:
    return _PREFIXES + f"""
SELECT ?event ?startDate
       (SAMPLE(?eventName) AS ?eventName)
       (GROUP_CONCAT(DISTINCT ?locLabel; separator="|") AS ?locations)
       (GROUP_CONCAT(DISTINCT ?dtype; separator=",") AS ?disasterType)
       (SAMPLE(?alts) AS ?alternates)
       (SAMPLE(?srcLabel) AS ?source)
WHERE {{
  ?event a :{event_type} ;
         :hasDisasterType ?dtype ;
         :startDate ?startDate ;
         :hasLocation ?location .
  ?location :isPartOf* :{psgc} .
  OPTIONAL {{ ?event :eventName ?eventName }}
  OPTIONAL {{ ?location rdfs:label ?locLabel }}
  OPTIONAL {{
    SELECT ?event (GROUP_CONCAT(DISTINCT ?alt; separator=",") AS ?alts)
    WHERE {{ ?event prov:alternateOf ?alt }}
    GROUP BY ?event
  }}
  OPTIONAL {{
    ?event prov:wasDerivedFrom+/prov:wasAttributedTo ?agent .
    ?agent skos:prefLabel ?srcLabel
  }}
}}
GROUP BY ?event ?startDate
ORDER BY DESC(?startDate)
LIMIT {limit}
OFFSET {offset}
"""


def _count_query(psgc: str, event_type: EventType) -> str:
    return _PREFIXES + f"""
SELECT (COUNT(DISTINCT ?event) AS ?count)
WHERE {{
  ?event a :{event_type} ;
         :startDate ?startDate ;
         :hasLocation ?location .
  ?location :isPartOf* :{psgc} .
  OPTIONAL {{
    ?event prov:alternateOf ?alt .
    ?alt :startDate ?altDate .
    FILTER(?altDate < ?startDate || (?altDate = ?startDate && STR(?alt) < STR(?event)))
  }}
  FILTER(!BOUND(?altDate))
}}
"""


def _count_val(result: dict[Any, Any]) -> int:
    bindings = result.get("results", {}).get("bindings", [])
    if not bindings:
        return 0
    value = bindings[0].get("count", {}).get("value")
    return int(value) if value else 0


def _event_type_from_mode(mode: EventMode) -> EventType:
    return "MajorEvent" if mode == "major" else "Incident"


def _validate_psgc(psgc: str) -> None:
    if not _PSGC_RE.match(psgc):
        raise ServiceError(422, "psgc must be exactly 10 digits")


async def _fetch_events(
    psgc: str,
    event_type: EventType,
    page: int,
    limit: int,
) -> dict[str, Any]:
    _validate_psgc(psgc)
    offset = (page - 1) * limit
    events_res, major_res, incident_res = await asyncio.gather(
        execute_sparql(_events_query(psgc, event_type, limit, offset)),
        execute_sparql(_count_query(psgc, "MajorEvent")),
        execute_sparql(_count_query(psgc, "Incident")),
    )
    errors = [r for r in (events_res, major_res, incident_res) if isinstance(r, str)]
    if errors:
        raise ServiceError(502, errors[0])
    return {
        "events": events_res.get("results", {}).get("bindings", []),
        "majorCount": _count_val(major_res),
        "incidentCount": _count_val(incident_res),
    }


async def get_events(
    scope: EventScope,
    id: str,
    mode: EventMode = "major",
    page: int = 1,
) -> dict[str, Any]:
    event_type = _event_type_from_mode(mode)
    cache_key = ("events", scope, id, mode, page)
    if (cached := _cache.get(cache_key)) is not None:
        return cached

    result = await _fetch_events(id, event_type, page, PAGE_SIZE)
    _cache.set(cache_key, result)
    return result


async def get_region_events(
    psgc: str,
    event_type: EventType = "MajorEvent",
    page: int = 1,
    limit: int = 10,
) -> dict[str, Any]:
    cache_key = ("region", psgc, event_type, page, limit)
    if (cached := _cache.get(cache_key)) is not None:
        return cached

    result = await _fetch_events(psgc, event_type, page, limit)
    response = {
        "events": result["events"],
        "major_count": result["majorCount"],
        "incident_count": result["incidentCount"],
    }
    _cache.set(cache_key, response)
    return response


async def get_province_events(
    psgc: str,
    event_type: EventType = "MajorEvent",
    page: int = 1,
    limit: int = 10,
) -> dict[str, Any]:
    cache_key = ("province", psgc, event_type, page, limit)
    if (cached := _cache.get(cache_key)) is not None:
        return cached

    result = await _fetch_events(psgc, event_type, page, limit)
    response = {
        "events": result["events"],
        "major_count": result["majorCount"],
        "incident_count": result["incidentCount"],
    }
    _cache.set(cache_key, response)
    return response

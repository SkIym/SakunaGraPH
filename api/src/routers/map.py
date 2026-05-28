import asyncio
import re
import time
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query

from src.services.sparql_service import execute_sparql

router = APIRouter(prefix="/map", tags=["map"])

_CACHE_TTL = 300  # seconds

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

_PREFIXES = """PREFIX :     <https://sakuna.ph/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
"""

_PSGC_RE = re.compile(r"^\d{10}$")
EventType = Literal["MajorEvent", "Incident"]


def _region_events_query(psgc: str, event_type: str, limit: int, offset: int) -> str:
    return _PREFIXES + f"""
SELECT ?event ?disasterType ?startDate
       (SAMPLE(?eventName) AS ?eventName)
       (GROUP_CONCAT(DISTINCT ?locLabel; separator="|") AS ?locations)
WHERE {{
  ?event a :{event_type} ;
         :hasDisasterType ?disasterType ;
         :startDate ?startDate ;
         :hasLocation ?location .
  ?location :isPartOf* :{psgc} .
  OPTIONAL {{ ?event    :eventName ?eventName }}
  OPTIONAL {{ ?location rdfs:label ?locLabel  }}
}}
GROUP BY ?event ?disasterType ?startDate
ORDER BY DESC(?startDate)
LIMIT {limit}
OFFSET {offset}
"""


def _region_count_query(psgc: str, event_type: str) -> str:
    return _PREFIXES + f"""
SELECT (COUNT(DISTINCT ?event) AS ?count)
WHERE {{
  ?event a :{event_type} ;
         :hasLocation ?location .
  ?location :isPartOf* :{psgc} .
}}
"""


def _province_events_query(normalized: str, event_type: str, limit: int, offset: int) -> str:
    return _PREFIXES + f"""
SELECT ?event ?disasterType ?startDate
       (SAMPLE(?eventName) AS ?eventName)
       (GROUP_CONCAT(DISTINCT ?locLabel; separator="|") AS ?locations)
WHERE {{
  ?event a :{event_type} ;
         :hasDisasterType ?disasterType ;
         :startDate ?startDate ;
         :hasLocation ?location .
  ?location :isPartOf* ?prov .
  ?prov a :Province ; rdfs:label ?provLabel .
  FILTER(REPLACE(LCASE(STR(?provLabel)), "[^a-z0-9]", "") = "{normalized}")
  OPTIONAL {{ ?event    :eventName ?eventName }}
  OPTIONAL {{ ?location rdfs:label ?locLabel  }}
}}
GROUP BY ?event ?disasterType ?startDate
ORDER BY DESC(?startDate)
LIMIT {limit}
OFFSET {offset}
"""


def _province_count_query(normalized: str, event_type: str) -> str:
    return _PREFIXES + f"""
SELECT (COUNT(DISTINCT ?event) AS ?count)
WHERE {{
  ?event a :{event_type} ;
         :hasLocation ?location .
  ?location :isPartOf* ?prov .
  ?prov a :Province ; rdfs:label ?provLabel .
  FILTER(REPLACE(LCASE(STR(?provLabel)), "[^a-z0-9]", "") = "{normalized}")
}}
"""


def _count_val(result: dict[Any, Any]) -> int:
    bindings = result.get("results", {}).get("bindings", [])
    if not bindings:
        return 0
    v = bindings[0].get("count", {}).get("value")
    return int(v) if v else 0


@router.get("/events/region/{psgc}")
async def region_events(
    psgc: str,
    event_type: EventType = Query("MajorEvent"),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
) -> dict[str, Any]:
    if not _PSGC_RE.match(psgc):
        raise HTTPException(status_code=422, detail="psgc must be exactly 10 digits")
    offset = (page - 1) * limit
    cache_key = ("region", psgc, event_type, page, limit)
    if (cached := _cache.get(cache_key)) is not None:
        return cached
    events_res, major_res, incident_res = await asyncio.gather(
        execute_sparql(_region_events_query(psgc, event_type, limit, offset)),
        execute_sparql(_region_count_query(psgc, "MajorEvent")),
        execute_sparql(_region_count_query(psgc, "Incident")),
    )
    errors = [r for r in (events_res, major_res, incident_res) if isinstance(r, str)]
    if errors:
        raise HTTPException(status_code=502, detail=errors[0])
    result = {
        "events":         events_res.get("results", {}).get("bindings", []),
        "major_count":    _count_val(major_res),
        "incident_count": _count_val(incident_res),
    }
    _cache.set(cache_key, result)
    return result


@router.get("/events/province/{province_name}")
async def province_events(
    province_name: str,
    event_type: EventType = Query("MajorEvent"),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
) -> dict[str, Any]:
    normalized = re.sub(r"[^a-z0-9]", "", province_name.lower())
    if not normalized:
        raise HTTPException(status_code=422, detail="invalid province_name")
    offset = (page - 1) * limit
    cache_key = ("province", normalized, event_type, page, limit)
    if (cached := _cache.get(cache_key)) is not None:
        return cached
    events_res, major_res, incident_res = await asyncio.gather(
        execute_sparql(_province_events_query(normalized, event_type, limit, offset)),
        execute_sparql(_province_count_query(normalized, "MajorEvent")),
        execute_sparql(_province_count_query(normalized, "Incident")),
    )
    errors = [r for r in (events_res, major_res, incident_res) if isinstance(r, str)]
    if errors:
        raise HTTPException(status_code=502, detail=errors[0])
    result = {
        "events":         events_res.get("results", {}).get("bindings", []),
        "major_count":    _count_val(major_res),
        "incident_count": _count_val(incident_res),
    }
    _cache.set(cache_key, result)
    return result

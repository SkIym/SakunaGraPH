import asyncio
import csv
import io
import time
from decimal import Decimal, InvalidOperation
from typing import Any

from src.schemas.analysis import (
    AnalysisDamageAmount,
    AnalysisEvent,
    AnalysisEventClass,
    AnalysisEventFacet,
    AnalysisEventImpact,
    AnalysisEventSortBy,
    AnalysisEventsResponse,
    AnalysisSortDirection,
)
from src.services.analysis.common import (
    AnalysisFilters,
    SPARQL_PREFIXES,
    date_only,
    event_filter_where,
    local_name,
    source_from_event_iri,
)
from src.services.common import ServiceError
from src.services.ontology.utils import binding_value
from src.services.sparql import execute_sparql

_CACHE_TTL = 300
_MAX_CACHE_ENTRIES = 256
_ENRICHMENT_CHUNK_SIZE = 500
_ENRICHMENT_CONCURRENCY = 3

_SORT_EXPRESSIONS: dict[AnalysisEventSortBy, str] = {
    "startDate": "SUBSTR(STR(?startDate), 1, 10)",
    "endDate": "SUBSTR(STR(?endDate), 1, 10)",
    "eventName": "LCASE(COALESCE(STR(?eventName), \"\"))",
    "eventType": "STR(?eventClass)",
    "source": 'LCASE(STRAFTER(STR(?event), "https://sakuna.ph/"))',
}

_EVENT_IMPACT_PROPERTIES = " ".join(
    (
        ":hasAgricultureDamage",
        ":hasDamageGeneral",
        ":hasHousingDamage",
        ":hasInfrastructureDamage",
    )
)
_DAMAGE_AMOUNT_PROPERTIES = " ".join(
    (
        ":agriDamageAmount",
        ":commercialDamageAmount",
        ":crossSectoralDamageAmount",
        ":generalDamageAmount",
        ":housingDamageAmount",
        ":infraDamageAmount",
        ":socialDamageAmount",
    )
)


class _TTLCache:
    def __init__(self) -> None:
        self._store: dict[tuple[Any, ...], tuple[float, Any]] = {}

    def get(self, key: tuple[Any, ...]) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        timestamp, value = entry
        if time.monotonic() - timestamp > _CACHE_TTL:
            del self._store[key]
            return None
        return value

    def set(self, key: tuple[Any, ...], value: Any) -> None:
        if key not in self._store and len(self._store) >= _MAX_CACHE_ENTRIES:
            oldest = min(self._store, key=lambda item: self._store[item][0])
            del self._store[oldest]
        self._store[key] = (time.monotonic(), value)


_cache = _TTLCache()


def _events_query(
    filters: AnalysisFilters,
    sort_by: AnalysisEventSortBy,
    sort_dir: AnalysisSortDirection,
    *,
    limit: int | None,
    offset: int = 0,
) -> str:
    direction = sort_dir.upper()
    sort_expression = _SORT_EXPRESSIONS[sort_by]
    where = event_filter_where(filters)
    pagination = "" if limit is None else f"LIMIT {limit}\nOFFSET {offset}"

    return SPARQL_PREFIXES + f"""
SELECT DISTINCT ?event ?eventName ?eventClass ?startDate ?endDate
WHERE {{
{where}
}}
ORDER BY {direction}({sort_expression}) ASC(STR(?event))
{pagination}
"""


def _count_query(filters: AnalysisFilters) -> str:
    return SPARQL_PREFIXES + f"""
SELECT (COUNT(DISTINCT ?event) AS ?count)
WHERE {{
{event_filter_where(filters)}
}}
"""


def _values_clause(event_iris: list[str]) -> str:
    values = " ".join(f"<{event_iri}>" for event_iri in event_iris)
    return f"VALUES ?event {{ {values} }}"


def _metadata_query(event_iris: list[str]) -> str:
    values = _values_clause(event_iris)
    return SPARQL_PREFIXES + f"""
SELECT DISTINCT ?event ?kind ?resource ?id ?label
WHERE {{
  {values}
  {{
    ?event :hasLocation ?resource .
    BIND("location" AS ?kind)
    OPTIONAL {{ ?resource :psgcCode ?id }}
    OPTIONAL {{ ?resource rdfs:label ?label }}
  }}
  UNION
  {{
    ?event (:hasDisasterType|:hasDisasterSubtype) ?resource .
    BIND("disasterType" AS ?kind)
    OPTIONAL {{ ?resource (skos:prefLabel|rdfs:label) ?label }}
  }}
  UNION
  {{
    {{ ?event prov:alternateOf ?resource }}
    UNION
    {{ ?resource prov:alternateOf ?event }}
    BIND("alternate" AS ?kind)
  }}
  UNION
  {{
    ?event prov:wasDerivedFrom+/prov:wasAttributedTo ?resource .
    BIND("source" AS ?kind)
    OPTIONAL {{ ?resource (skos:prefLabel|rdfs:label) ?label }}
  }}
}}
ORDER BY ?event ?kind ?label ?resource
"""


def _impacts_query(event_iris: list[str]) -> str:
    values = _values_clause(event_iris)
    return SPARQL_PREFIXES + f"""
SELECT ?event ?metric ?value ?unit
WHERE {{
  {{
    {{
      SELECT ?event ?casualtyType (SUM(xsd:decimal(?rawValue)) AS ?value)
      WHERE {{
        {values}
        ?event :hasCasualties ?casualties .
        ?casualties :casualtyCount ?rawValue ;
                    :isOfCasualtyType ?casualtyType .
      }}
      GROUP BY ?event ?casualtyType
    }}
    BIND(LCASE(REPLACE(STR(?casualtyType), "^.*/", "")) AS ?metric)
  }}
  UNION
  {{
    {{
      SELECT ?event ?metric (SUM(xsd:decimal(?rawValue)) AS ?value)
      WHERE {{
        {values}
        VALUES (?populationProperty ?metric) {{
          (:affectedFamilies "affectedFamilies")
          (:affectedPersons "affectedPersons")
        }}
        ?event :hasAffectedPopulation ?population .
        ?population ?populationProperty ?rawValue .
      }}
      GROUP BY ?event ?metric
    }}
  }}
  UNION
  {{
    {{
      SELECT ?event ?unit (SUM(xsd:decimal(?rawValue)) AS ?value)
      WHERE {{
        {values}
        VALUES ?eventImpactProperty {{ {_EVENT_IMPACT_PROPERTIES} }}
        VALUES ?damageProperty {{ {_DAMAGE_AMOUNT_PROPERTIES} }}
        ?event ?eventImpactProperty ?damage .
        ?damage ?damageProperty ?measure .
        ?measure qudt:numericValue ?rawValue ;
                 qudt:unit ?unit .
      }}
      GROUP BY ?event ?unit
    }}
    BIND("damage" AS ?metric)
  }}
}}
ORDER BY ?event ?metric ?unit
"""


async def _execute_or_raise(query: str) -> dict[Any, Any]:
    result = await execute_sparql(query)
    if isinstance(result, str):
        raise ServiceError(502, result)
    return result


def _bindings(result: dict[Any, Any]) -> list[dict[Any, Any]]:
    return result.get("results", {}).get("bindings", [])


def _count_value(result: dict[Any, Any]) -> int:
    bindings = _bindings(result)
    if not bindings:
        return 0
    value = binding_value(bindings[0], "count", "0")
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ServiceError(502, "GraphDB returned an invalid event count") from None


def _event_class(value: str) -> AnalysisEventClass:
    name = local_name(value)
    if name not in {"MajorEvent", "Incident"}:
        raise ServiceError(502, f"GraphDB returned an unsupported event class: {name}")
    return name


def _base_events(result: dict[Any, Any]) -> list[AnalysisEvent]:
    events: list[AnalysisEvent] = []
    for binding in _bindings(result):
        event_iri = binding_value(binding, "event", "")
        if not event_iri:
            continue
        events.append(
            AnalysisEvent(
                event=event_iri,
                eventName=binding_value(binding, "eventName", "") or "(unnamed event)",
                eventType=_event_class(binding_value(binding, "eventClass", "")),
                startDate=date_only(binding_value(binding, "startDate", "")) or "",
                endDate=date_only(binding_value(binding, "endDate")),
                source=source_from_event_iri(event_iri),
            )
        )
    return events


def _decimal_value(binding: dict[Any, Any]) -> Decimal:
    value = binding_value(binding, "value", "0")
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        raise ServiceError(502, "GraphDB returned a non-numeric impact value") from None


def _apply_metadata(
    events_by_iri: dict[str, AnalysisEvent],
    result: dict[Any, Any],
) -> None:
    locations: dict[str, dict[str, AnalysisEventFacet]] = {
        event_iri: {} for event_iri in events_by_iri
    }
    disaster_types: dict[str, dict[str, AnalysisEventFacet]] = {
        event_iri: {} for event_iri in events_by_iri
    }
    alternates: dict[str, set[str]] = {event_iri: set() for event_iri in events_by_iri}
    sources: dict[str, set[str]] = {event_iri: set() for event_iri in events_by_iri}

    for binding in _bindings(result):
        event_iri = binding_value(binding, "event", "")
        event = events_by_iri.get(event_iri)
        if event is None:
            continue
        kind = binding_value(binding, "kind", "")
        resource = binding_value(binding, "resource", "")
        if not resource:
            continue

        if kind == "location":
            facet_id = binding_value(binding, "id") or local_name(resource)
            label = binding_value(binding, "label") or facet_id
            locations[event_iri][facet_id] = AnalysisEventFacet(id=facet_id, label=label)
        elif kind == "disasterType":
            facet_id = local_name(resource)
            label = binding_value(binding, "label") or facet_id
            disaster_types[event_iri][facet_id] = AnalysisEventFacet(
                id=facet_id,
                label=label,
            )
        elif kind == "alternate" and resource != event_iri:
            alternates[event_iri].add(resource)
        elif kind == "source":
            sources[event_iri].add(binding_value(binding, "label") or local_name(resource))

    for event_iri, event in events_by_iri.items():
        event.locations = sorted(
            locations[event_iri].values(),
            key=lambda item: (item.label.casefold(), item.id),
        )
        event.disasterTypes = sorted(
            disaster_types[event_iri].values(),
            key=lambda item: (item.label.casefold(), item.id),
        )
        event.alternates = sorted(alternates[event_iri])
        if sources[event_iri] and event.source is None:
            event.source = ", ".join(sorted(sources[event_iri], key=str.casefold))


def _apply_impacts(
    events_by_iri: dict[str, AnalysisEvent],
    result: dict[Any, Any],
) -> None:
    damage_by_event: dict[str, dict[str, Decimal]] = {
        event_iri: {} for event_iri in events_by_iri
    }

    for binding in _bindings(result):
        event_iri = binding_value(binding, "event", "")
        event = events_by_iri.get(event_iri)
        if event is None:
            continue
        metric = binding_value(binding, "metric", "")
        value = _decimal_value(binding)

        if metric in {"dead", "injured", "missing"}:
            setattr(event.impact, metric, int(value))
        elif metric == "affectedFamilies":
            event.impact.affectedFamilies = int(value)
        elif metric == "affectedPersons":
            event.impact.affectedPersons = int(value)
        elif metric == "damage":
            unit = local_name(binding_value(binding, "unit", "")) or "unknown"
            damage_by_event[event_iri][unit] = value

    for event_iri, event in events_by_iri.items():
        amounts = [
            AnalysisDamageAmount(amount=float(amount), unit=unit)
            for unit, amount in sorted(damage_by_event[event_iri].items())
        ]
        event.impact.damageByUnit = amounts
        if len(amounts) == 1:
            event.impact.damageAmount = amounts[0].amount
            event.impact.damageUnit = amounts[0].unit
        elif len(amounts) > 1:
            # Keep incompatible units separate instead of presenting a false total.
            event.impact.damageAmount = None
            event.impact.damageUnit = None


async def _enrich_events(events: list[AnalysisEvent]) -> list[AnalysisEvent]:
    if not events:
        return events

    events_by_iri = {event.event: event for event in events}
    event_iris = list(events_by_iri)

    semaphore = asyncio.Semaphore(_ENRICHMENT_CONCURRENCY)

    async def enrich_chunk(chunk: list[str]) -> None:
        chunk_events = {event_iri: events_by_iri[event_iri] for event_iri in chunk}
        async with semaphore:
            metadata_result, impacts_result = await asyncio.gather(
                _execute_or_raise(_metadata_query(chunk)),
                _execute_or_raise(_impacts_query(chunk)),
            )
        _apply_metadata(chunk_events, metadata_result)
        _apply_impacts(chunk_events, impacts_result)

    chunks = [
        event_iris[start : start + _ENRICHMENT_CHUNK_SIZE]
        for start in range(0, len(event_iris), _ENRICHMENT_CHUNK_SIZE)
    ]
    await asyncio.gather(*(enrich_chunk(chunk) for chunk in chunks))
    return events


async def get_analysis_events(
    *,
    filters: AnalysisFilters,
    page: int,
    page_size: int,
    sort_by: AnalysisEventSortBy,
    sort_dir: AnalysisSortDirection,
) -> AnalysisEventsResponse:
    cache_key = ("events", filters, page, page_size, sort_by, sort_dir)
    if (cached := _cache.get(cache_key)) is not None:
        return cached

    offset = (page - 1) * page_size
    events_result, count_result = await asyncio.gather(
        _execute_or_raise(
            _events_query(
                filters,
                sort_by,
                sort_dir,
                limit=page_size,
                offset=offset,
            )
        ),
        _execute_or_raise(_count_query(filters)),
    )
    items = await _enrich_events(_base_events(events_result))
    response = AnalysisEventsResponse(
        items=items,
        page=page,
        page_size=page_size,
        total=_count_value(count_result),
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    _cache.set(cache_key, response)
    return response


async def get_analysis_events_export(
    *,
    filters: AnalysisFilters,
    sort_by: AnalysisEventSortBy,
    sort_dir: AnalysisSortDirection,
) -> str:
    cache_key = ("events-export", filters, sort_by, sort_dir)
    if (cached := _cache.get(cache_key)) is not None:
        return cached

    result = await _execute_or_raise(
        _events_query(filters, sort_by, sort_dir, limit=None)
    )
    items = await _enrich_events(_base_events(result))
    csv_content = events_to_csv(items)
    _cache.set(cache_key, csv_content)
    return csv_content


def _csv_safe(value: str) -> str:
    if value.startswith(("=", "+", "-", "@", "\t", "\r")):
        return f"'{value}"
    return value


def _facet_csv(facets: list[AnalysisEventFacet]) -> str:
    return " | ".join(
        _csv_safe(f"{facet.label} ({facet.id})") for facet in facets
    )


def _damage_csv(impact: AnalysisEventImpact) -> tuple[str | float, str]:
    if not impact.damageByUnit:
        return 0, ""
    if len(impact.damageByUnit) == 1:
        damage = impact.damageByUnit[0]
        return damage.amount, damage.unit
    return (
        " | ".join(f"{damage.amount:g} {damage.unit}" for damage in impact.damageByUnit),
        "multiple",
    )


def events_to_csv(items: list[AnalysisEvent]) -> str:
    output = io.StringIO(newline="")
    fieldnames = [
        "event",
        "eventName",
        "eventType",
        "startDate",
        "endDate",
        "locations",
        "disasterTypes",
        "source",
        "alternates",
        "dead",
        "injured",
        "missing",
        "affectedFamilies",
        "affectedPersons",
        "damageAmount",
        "damageUnit",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for event in items:
        damage_amount, damage_unit = _damage_csv(event.impact)
        writer.writerow(
            {
                "event": event.event,
                "eventName": _csv_safe(event.eventName),
                "eventType": event.eventType,
                "startDate": event.startDate,
                "endDate": event.endDate or "",
                "locations": _facet_csv(event.locations),
                "disasterTypes": _facet_csv(event.disasterTypes),
                "source": _csv_safe(event.source or ""),
                "alternates": " | ".join(event.alternates),
                "dead": event.impact.dead,
                "injured": event.impact.injured,
                "missing": event.impact.missing,
                "affectedFamilies": event.impact.affectedFamilies,
                "affectedPersons": event.impact.affectedPersons,
                "damageAmount": damage_amount,
                "damageUnit": damage_unit,
            }
        )
    return output.getvalue()

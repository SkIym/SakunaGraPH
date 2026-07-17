import json
import re
from typing import Any
from urllib.parse import unquote, urlparse

from src.schemas.disasters import (
    DisasterOrganization,
    DisasterOrganizationsResponse,
    DisasterSource,
    DisasterSourcesResponse,
    EventDetailDisasterType,
    EventDetailLocation,
    EventDetailRelatedEvent,
    EventDetailsResponse,
    EventDetailSource,
    EventImpactResponse,
    IriLabel,
    ImpactClass,
    ImpactItem,
)
from src.services.common import ServiceError
from src.services.ontology.utils import binding_value
from src.services.sparql import execute_sparql

_PREFIXES = """PREFIX :     <https://sakuna.ph/>
PREFIX org:  <https://sakuna.ph/org/>
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
"""

_IMPACT_CLASSES_QUERY = _PREFIXES + """
SELECT DISTINCT ?class ?label ?definition WHERE {
  ?class a owl:Class ;
         rdfs:subClassOf* :Impact .
  FILTER(?class != :Impact)
  FILTER(STRSTARTS(STR(?class), "https://sakuna.ph/"))
  OPTIONAL { ?class rdfs:label ?label }
  OPTIONAL { ?class skos:definition ?definition }
}
ORDER BY ?class
"""

_FORBIDDEN_IRI_CHARS = re.compile(r'[\x00-\x20<>"{}|^`\\]')
_CAMEL_BOUNDARY = re.compile(r"(?<!^)(?=[A-Z])")


def _decode_path_value(value: str) -> str:
    decoded = unquote(value).strip()
    if decoded.startswith("https:/") and not decoded.startswith("https://"):
        decoded = decoded.replace("https:/", "https://", 1)
    if decoded.startswith("http:/") and not decoded.startswith("http://"):
        decoded = decoded.replace("http:/", "http://", 1)
    return decoded


def _validate_iri(value: str, field: str) -> str:
    iri = _decode_path_value(value)
    parsed = urlparse(iri)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ServiceError(422, f"{field} must be a full http(s) IRI")
    if _FORBIDDEN_IRI_CHARS.search(iri):
        raise ServiceError(422, f"{field} contains characters that are not allowed in a SPARQL IRI")
    return iri


def _iri(value: str) -> str:
    return f"<{value}>"


def _local_name(iri: str | None) -> str:
    if not iri:
        return ""
    return iri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]


def _key(value: str | None) -> str:
    local = _local_name(value or "")
    local = _CAMEL_BOUNDARY.sub(" ", local)
    return re.sub(r"[^a-z0-9]", "", local.lower())


async def _execute_or_raise(query: str) -> dict[Any, Any]:
    result = await execute_sparql(query)
    if isinstance(result, str):
        raise ServiceError(502, result)
    return result


async def _resolve_impact_class(impact: str) -> ImpactClass:
    requested = _decode_path_value(impact)
    requested_key = _key(requested)
    if not requested_key:
        raise ServiceError(422, "impact must be an ontology impact class name or IRI")

    result = await _execute_or_raise(_IMPACT_CLASSES_QUERY)
    bindings = result.get("results", {}).get("bindings", [])
    if not bindings:
        raise ServiceError(502, "No ontology impact classes were returned by GraphDB")

    for binding in bindings:
        class_iri = binding_value(binding, "class", "")
        label = binding_value(binding, "label", "")
        keys = {_key(class_iri), _key(label)}
        if requested_key in keys:
            return ImpactClass(
                uri=class_iri,
                id=_local_name(class_iri),
                label=label or _local_name(class_iri),
                definition=binding_value(binding, "definition", ""),
            )

    raise ServiceError(404, f"Unknown impact class: {impact}")


def _impact_query(event_iri: str, impact_class_iri: str) -> str:
    return _PREFIXES + f"""
SELECT DISTINCT
  ?subject ?subjectName ?property ?propertyLabel
  ?impact ?impactClass ?impactLabel
  ?location ?locationLabel
  ?predicate ?predicateLabel ?value ?unit
WHERE {{
  VALUES ?root {{ {_iri(event_iri)} }}
  {{
    BIND(?root AS ?subject)
    ?subject ?property ?impact .
  }}
  UNION
  {{
    ?root :hasRelatedIncident ?subject .
    ?subject ?property ?impact .
  }}

  FILTER(isIRI(?impact))
  FILTER(?property != :hasRelatedIncident)
  ?impact a ?impactClass .
  ?impactClass rdfs:subClassOf* {_iri(impact_class_iri)} .

  OPTIONAL {{ ?subject :eventName ?subjectName }}
  OPTIONAL {{ ?property rdfs:label ?propertyLabel }}
  OPTIONAL {{ ?impact rdfs:label|skos:prefLabel ?impactLabel }}
  OPTIONAL {{
    ?impact :hasLocation ?location .
    OPTIONAL {{ ?location rdfs:label ?locationLabel }}
  }}
  OPTIONAL {{
    {{
      ?impact ?predicate ?value .
      FILTER(?predicate NOT IN (rdf:type, :hasLocation))
      FILTER(isLiteral(?value) || isIRI(?value))
    }}
    UNION
    {{
      ?impact ?predicate ?measure .
      FILTER(?predicate NOT IN (rdf:type, :hasLocation))
      ?measure qudt:numericValue ?value .
      OPTIONAL {{ ?measure qudt:unit ?unit }}
    }}
    OPTIONAL {{ ?predicate rdfs:label ?predicateLabel }}
  }}
}}
ORDER BY ?subject ?impact ?predicate ?value
"""


def _organizations_query(event_iri: str) -> str:
    return _PREFIXES + f"""
SELECT DISTINCT
  ?organization ?label ?property ?propertyLabel
  ?holder ?holderClass ?relatedEvent ?relatedEventName
WHERE {{
  VALUES ?root {{ {_iri(event_iri)} }}
  {{
    BIND(?root AS ?relatedEvent)
  }}
  UNION
  {{
    ?root :hasRelatedIncident ?relatedEvent .
  }}

  OPTIONAL {{ ?relatedEvent :eventName ?relatedEventName }}

  {{
    ?relatedEvent ?eventProperty ?holder .
    FILTER(isIRI(?holder) || isBlank(?holder))
    ?holder ?property ?organization .
  }}
  UNION
  {{
    BIND(?relatedEvent AS ?holder)
    ?relatedEvent ?property ?organization .
  }}

  FILTER(isIRI(?organization))
  FILTER(?property != prov:wasAttributedTo)
  FILTER(
    STRSTARTS(STR(?organization), "https://sakuna.ph/org/")
    || EXISTS {{ ?organization a :Organization }}
    || EXISTS {{ ?organization a prov:Agent }}
  )

  OPTIONAL {{ ?organization skos:prefLabel|rdfs:label ?label }}
  OPTIONAL {{ ?property rdfs:label ?propertyLabel }}
  OPTIONAL {{ ?holder a ?holderClass }}
}}
ORDER BY ?label ?organization
"""


def _sources_query(event_iri: str) -> str:
    return _PREFIXES + f"""
SELECT DISTINCT
  ?source ?label ?sourceRecord ?recordLabel ?relatedEvent ?relatedEventName
WHERE {{
  VALUES ?root {{ {_iri(event_iri)} }}
  {{
    BIND(?root AS ?relatedEvent)
  }}
  UNION
  {{
    ?root :hasRelatedIncident ?relatedEvent .
  }}

  OPTIONAL {{ ?relatedEvent :eventName ?relatedEventName }}

  {{
    ?relatedEvent prov:wasDerivedFrom+ ?sourceRecord .
    ?sourceRecord prov:wasAttributedTo ?source .
  }}
  UNION
  {{
    ?relatedEvent prov:wasAttributedTo ?source .
    BIND(?relatedEvent AS ?sourceRecord)
  }}

  OPTIONAL {{ ?source skos:prefLabel|rdfs:label ?label }}
  OPTIONAL {{ ?sourceRecord rdfs:label|skos:prefLabel ?recordLabel }}
}}
ORDER BY ?label ?source ?sourceRecord
"""


def _event_details_query(event_iri: str) -> str:
    return _PREFIXES + f"""
SELECT DISTINCT
  ?kind ?resource ?id ?label ?eventClass ?startDate ?endDate
  ?remarks
  ?reportName ?reportLink ?obtainedDate ?lastUpdateDate ?format
  ?attributedTo ?attributedToLabel
WHERE {{
  VALUES ?root {{ {_iri(event_iri)} }}
  {{
    ?root a ?eventClass .
    VALUES ?eventClass {{ :MajorEvent :Incident }}
    OPTIONAL {{ ?root :eventName ?eventName }}
    OPTIONAL {{ ?root :incidentDescription ?incidentDescription }}
    OPTIONAL {{ ?root :startDate ?startDate }}
    OPTIONAL {{ ?root :endDate ?endDate }}
    OPTIONAL {{ ?root :remarks ?remarks }}
    BIND(COALESCE(?eventName, ?incidentDescription) AS ?label)
    BIND(?root AS ?resource)
    BIND("core" AS ?kind)
  }}
  UNION
  {{
    ?root :hasLocation ?resource .
    OPTIONAL {{ ?resource :psgcCode ?id }}
    OPTIONAL {{ ?resource rdfs:label ?label }}
    BIND("location" AS ?kind)
  }}
  UNION
  {{
    ?root (:hasDisasterType|:hasDisasterSubtype) ?resource .
    OPTIONAL {{ ?resource (skos:prefLabel|rdfs:label) ?label }}
    BIND("disasterType" AS ?kind)
  }}
  UNION
  {{
    {{
      {{ ?resource :hasRelatedIncident ?root }}
      UNION
      {{ ?root :isRelatedTo ?resource }}
    }}
    OPTIONAL {{ ?resource :eventName ?eventName }}
    OPTIONAL {{ ?resource :incidentDescription ?incidentDescription }}
    OPTIONAL {{ ?resource :startDate ?startDate }}
    OPTIONAL {{ ?resource :endDate ?endDate }}
    OPTIONAL {{
      ?resource a ?eventClass .
      VALUES ?eventClass {{ :MajorEvent :Incident }}
    }}
    BIND(COALESCE(?eventName, ?incidentDescription) AS ?label)
    BIND("majorEvent" AS ?kind)
  }}
  UNION
  {{
    {{
      {{ ?root :hasRelatedIncident ?resource }}
      UNION
      {{ ?resource :isRelatedTo ?root }}
    }}
    OPTIONAL {{ ?resource :eventName ?eventName }}
    OPTIONAL {{ ?resource :incidentDescription ?incidentDescription }}
    OPTIONAL {{ ?resource :startDate ?startDate }}
    OPTIONAL {{ ?resource :endDate ?endDate }}
    OPTIONAL {{
      ?resource a ?eventClass .
      VALUES ?eventClass {{ :MajorEvent :Incident }}
    }}
    BIND(COALESCE(?eventName, ?incidentDescription) AS ?label)
    BIND("incident" AS ?kind)
  }}
  UNION
  {{
    {{
      {{ ?root prov:alternateOf ?resource }}
      UNION
      {{ ?resource prov:alternateOf ?root }}
      UNION
      {{ ?root owl:sameAs ?resource }}
      UNION
      {{ ?resource owl:sameAs ?root }}
      UNION
      {{
        ?alignment rdf:subject ?root ;
                   rdf:predicate owl:sameAs ;
                   rdf:object ?resource .
      }}
      UNION
      {{
        ?alignment rdf:object ?root ;
                   rdf:predicate owl:sameAs ;
                   rdf:subject ?resource .
      }}
    }}
    FILTER(?resource != ?root)
    OPTIONAL {{ ?resource :eventName ?eventName }}
    OPTIONAL {{ ?resource :incidentDescription ?incidentDescription }}
    OPTIONAL {{ ?resource :startDate ?startDate }}
    OPTIONAL {{ ?resource :endDate ?endDate }}
    OPTIONAL {{
      ?resource a ?eventClass .
      VALUES ?eventClass {{ :MajorEvent :Incident }}
    }}
    BIND(COALESCE(?eventName, ?incidentDescription) AS ?label)
    BIND("alternate" AS ?kind)
  }}
  UNION
  {{
    {{
      {{ ?root prov:wasDerivedFrom+ ?resource . }}
      UNION
      {{
        ?majorEvent :hasRelatedIncident ?root .
        ?majorEvent prov:wasDerivedFrom+ ?resource .
      }}
    }}
    ?resource a :Source .
    OPTIONAL {{ ?resource :reportName ?reportName }}
    OPTIONAL {{ ?resource :reportLink ?reportLink }}
    OPTIONAL {{ ?resource :obtainedDate ?obtainedDate }}
    OPTIONAL {{ ?resource :lastUpdateDate ?lastUpdateDate }}
    OPTIONAL {{ ?resource :format ?format }}
    OPTIONAL {{
      ?resource prov:wasAttributedTo ?attributedTo .
      OPTIONAL {{ ?attributedTo (skos:prefLabel|rdfs:label) ?attributedToLabel }}
    }}
    BIND("source" AS ?kind)
  }}
}}
ORDER BY ?kind ?label ?resource
"""


def _binding_display_value(binding: dict[Any, Any], key: str) -> dict[str, Any] | None:
    term = binding.get(key)
    if not term:
        return None
    value = term.get("value", "")
    result: dict[str, Any] = {
        "value": _local_name(value) if term.get("type") == "uri" else value,
        "valueType": term.get("type"),
    }
    if datatype := term.get("datatype"):
        result["datatype"] = datatype
    if lang := term.get("xml:lang"):
        result["lang"] = lang
    return result


def _dedupe_append(items: list[dict[str, Any]], item: dict[str, Any], seen: set[str]) -> None:
    key = json.dumps(item, sort_keys=True)
    if key not in seen:
        seen.add(key)
        items.append(item)


def _build_impact_items(bindings: list[dict[Any, Any]]) -> list[ImpactItem]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}

    for binding in bindings:
        impact_iri = binding_value(binding, "impact", "")
        subject_iri = binding_value(binding, "subject", "")
        property_iri = binding_value(binding, "property", "")
        if not (impact_iri and subject_iri and property_iri):
            continue

        key = (impact_iri, subject_iri, property_iri)
        item = grouped.setdefault(
            key,
            {
                "uri": impact_iri,
                "label": binding_value(binding, "impactLabel", ""),
                "class": binding_value(binding, "impactClass", ""),
                "classLabel": _local_name(binding_value(binding, "impactClass", "")),
                "linkedFrom": {
                    "uri": subject_iri,
                    "name": binding_value(binding, "subjectName", ""),
                },
                "linkProperty": {
                    "uri": property_iri,
                    "label": binding_value(binding, "propertyLabel", _local_name(property_iri)),
                },
                "locations": [],
                "values": [],
                "_seenLocations": set(),
                "_seenValues": set(),
            },
        )

        if location := binding_value(binding, "location", ""):
            _dedupe_append(
                item["locations"],
                {
                    "uri": location,
                    "label": binding_value(binding, "locationLabel", _local_name(location)),
                },
                item["_seenLocations"],
            )

        predicate = binding_value(binding, "predicate", "")
        value = _binding_display_value(binding, "value")
        if predicate and value:
            value_item = {
                "predicate": predicate,
                "label": binding_value(binding, "predicateLabel", _local_name(predicate)),
                **value,
            }
            if unit := binding_value(binding, "unit", ""):
                value_item["unit"] = _local_name(unit)
            _dedupe_append(item["values"], value_item, item["_seenValues"])

    items = list(grouped.values())
    for item in items:
        del item["_seenLocations"]
        del item["_seenValues"]
    return [ImpactItem.model_validate(item) for item in items]


def _build_sources(bindings: list[dict[Any, Any]]) -> list[DisasterSource]:
    grouped: dict[str, dict[str, Any]] = {}

    for binding in bindings:
        source_iri = binding_value(binding, "source", "")
        if not source_iri:
            continue

        source = grouped.setdefault(
            source_iri,
            {
                "uri": source_iri,
                "label": binding_value(binding, "label", _local_name(source_iri)),
                "records": [],
                "_seenRecords": set(),
            },
        )

        record_iri = binding_value(binding, "sourceRecord", "")
        related_event = binding_value(binding, "relatedEvent", "")
        if record_iri:
            _dedupe_append(
                source["records"],
                {
                    "uri": record_iri,
                    "label": binding_value(binding, "recordLabel", _local_name(record_iri)),
                    "relatedEvent": {
                        "uri": related_event,
                        "name": binding_value(binding, "relatedEventName", ""),
                    },
                },
                source["_seenRecords"],
            )

    sources = list(grouped.values())
    for source in sources:
        del source["_seenRecords"]
    return [DisasterSource.model_validate(source) for source in sources]


def _build_organizations(bindings: list[dict[Any, Any]]) -> list[DisasterOrganization]:
    grouped: dict[str, dict[str, Any]] = {}

    for binding in bindings:
        org_iri = binding_value(binding, "organization", "")
        if not org_iri:
            continue

        organization = grouped.setdefault(
            org_iri,
            {
                "uri": org_iri,
                "label": binding_value(binding, "label", _local_name(org_iri)),
                "roles": [],
                "_seenRoles": set(),
            },
        )

        property_iri = binding_value(binding, "property", "")
        holder_iri = binding_value(binding, "holder", "")
        related_event = binding_value(binding, "relatedEvent", "")
        role = {
            "property": {
                "uri": property_iri,
                "label": binding_value(binding, "propertyLabel", _local_name(property_iri)),
            },
            "holder": {
                "uri": holder_iri,
                "class": binding_value(binding, "holderClass", ""),
                "classLabel": _local_name(binding_value(binding, "holderClass", "")),
            },
            "relatedEvent": {
                "uri": related_event,
                "name": binding_value(binding, "relatedEventName", ""),
            },
        }
        _dedupe_append(organization["roles"], role, organization["_seenRoles"])

    organizations = list(grouped.values())
    for organization in organizations:
        del organization["_seenRoles"]
    return [
        DisasterOrganization.model_validate(organization)
        for organization in organizations
    ]


def _date_only(value: str | None) -> str | None:
    if not value:
        return None
    return value.split("T", 1)[0]


def _related_event(binding: dict[Any, Any]) -> EventDetailRelatedEvent:
    event_class = _local_name(binding_value(binding, "eventClass", "")) or None
    return EventDetailRelatedEvent(
        uri=binding_value(binding, "resource", ""),
        name=binding_value(binding, "label", "") or "(unnamed event)",
        eventType=event_class,
        startDate=_date_only(binding_value(binding, "startDate")),
        endDate=_date_only(binding_value(binding, "endDate")),
    )


def _build_event_details(
    event_iri: str,
    bindings: list[dict[Any, Any]],
) -> EventDetailsResponse:
    core: dict[str, Any] | None = None
    locations: dict[str, EventDetailLocation] = {}
    disaster_types: dict[str, EventDetailDisasterType] = {}
    major_events: dict[str, EventDetailRelatedEvent] = {}
    incidents: dict[str, EventDetailRelatedEvent] = {}
    alternates: dict[str, EventDetailRelatedEvent] = {}
    sources: dict[str, dict[str, Any]] = {}
    remarks: list[str] = []
    seen_remarks: set[str] = set()

    for binding in bindings:
        kind = binding_value(binding, "kind", "")
        resource = binding_value(binding, "resource", "")

        if kind == "core":
            event_class = _local_name(binding_value(binding, "eventClass", ""))
            if event_class not in {"MajorEvent", "Incident"}:
                continue
            core = {
                "event": event_iri,
                "name": binding_value(binding, "label", "") or "(unnamed event)",
                "eventType": event_class,
                "startDate": _date_only(binding_value(binding, "startDate")),
                "endDate": _date_only(binding_value(binding, "endDate")),
            }
            if remark := binding_value(binding, "remarks", ""):
                if remark not in seen_remarks:
                    seen_remarks.add(remark)
                    remarks.append(remark)
        elif kind == "location" and resource:
            location_id = binding_value(binding, "id", "") or _local_name(resource)
            locations[resource] = EventDetailLocation(
                uri=resource,
                id=location_id,
                label=binding_value(binding, "label", "") or location_id,
            )
        elif kind == "disasterType" and resource:
            disaster_type_id = _local_name(resource)
            disaster_types[resource] = EventDetailDisasterType(
                uri=resource,
                id=disaster_type_id,
                label=binding_value(binding, "label", "") or disaster_type_id,
            )
        elif kind in {"majorEvent", "incident", "alternate"} and resource:
            target = {
                "majorEvent": major_events,
                "incident": incidents,
                "alternate": alternates,
            }[kind]
            target[resource] = _related_event(binding)
        elif kind == "source" and resource:
            source = sources.setdefault(
                resource,
                {
                    "uri": resource,
                    "reportName": binding_value(binding, "reportName", "")
                    or _local_name(resource),
                    "reportLink": binding_value(binding, "reportLink") or None,
                    "obtainedDate": binding_value(binding, "obtainedDate") or None,
                    "lastUpdateDate": binding_value(binding, "lastUpdateDate") or None,
                    "format": binding_value(binding, "format") or None,
                    "attributedTo": {},
                },
            )
            attributed_to = binding_value(binding, "attributedTo", "")
            if attributed_to:
                source["attributedTo"][attributed_to] = IriLabel(
                    uri=attributed_to,
                    label=binding_value(binding, "attributedToLabel", "")
                    or _local_name(attributed_to),
                )

    if core is None:
        raise ServiceError(404, "Event not found")

    source_items = []
    for source in sources.values():
        source["attributedTo"] = sorted(
            source["attributedTo"].values(),
            key=lambda item: item.label.casefold(),
        )
        source_items.append(EventDetailSource.model_validate(source))

    return EventDetailsResponse(
        **core,
        remarks=remarks,
        locations=sorted(locations.values(), key=lambda item: item.label.casefold()),
        disasterTypes=sorted(
            disaster_types.values(),
            key=lambda item: item.label.casefold(),
        ),
        majorEvents=sorted(major_events.values(), key=lambda item: item.startDate or ""),
        incidents=sorted(incidents.values(), key=lambda item: item.startDate or ""),
        alternates=sorted(alternates.values(), key=lambda item: item.startDate or ""),
        sources=sorted(source_items, key=lambda item: item.reportName.casefold()),
    )


async def get_event_impact(uri: str, impact: str) -> EventImpactResponse:
    event_iri = _validate_iri(uri, "uri")
    impact_class = await _resolve_impact_class(impact)
    result = await _execute_or_raise(_impact_query(event_iri, impact_class.uri))
    bindings = result.get("results", {}).get("bindings", [])

    return EventImpactResponse(
        event=event_iri,
        impact=impact_class,
        items=_build_impact_items(bindings),
    )


async def get_event_details(uri: str) -> EventDetailsResponse:
    event_iri = _validate_iri(uri, "uri")
    result = await _execute_or_raise(_event_details_query(event_iri))
    bindings = result.get("results", {}).get("bindings", [])
    return _build_event_details(event_iri, bindings)


async def get_disaster_organizations(uri: str) -> DisasterOrganizationsResponse:
    event_iri = _validate_iri(uri, "uri")
    result = await _execute_or_raise(_organizations_query(event_iri))
    bindings = result.get("results", {}).get("bindings", [])

    return DisasterOrganizationsResponse(
        event=event_iri,
        organizations=_build_organizations(bindings),
    )


async def get_disaster_sources(uri: str) -> DisasterSourcesResponse:
    event_iri = _validate_iri(uri, "uri")
    result = await _execute_or_raise(_sources_query(event_iri))
    bindings = result.get("results", {}).get("bindings", [])

    return DisasterSourcesResponse(
        event=event_iri,
        sources=_build_sources(bindings),
    )

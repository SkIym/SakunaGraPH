import asyncio
from typing import Any

from src.schemas.ontology import OntologyGraphResponse
from src.services.common import ServiceError
from src.services.ontology.utils import binding_value
from src.services.sparql import execute_sparql

_GRAPH_CLASSES_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:    <http://www.w3.org/2002/07/owl#>
PREFIX skos:   <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?class ?label ?definition WHERE {
    ?class a owl:Class .
    FILTER(STRSTARTS(STR(?class), "https://sakuna.ph/"))
    FILTER(isIRI(?class))
    OPTIONAL { ?class rdfs:label    ?label      }
    OPTIONAL { ?class skos:definition ?definition }
}
"""

_GRAPH_SUBCLASSOF_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?child ?parent WHERE {
    ?child rdfs:subClassOf ?parent .
    FILTER(STRSTARTS(STR(?child),  "https://sakuna.ph/"))
    FILTER(STRSTARTS(STR(?parent), "https://sakuna.ph/"))
    FILTER(isIRI(?child))
    FILTER(isIRI(?parent))
    FILTER(?child != ?parent)
}
"""

_GRAPH_OBJPROPS_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:    <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?prop ?label ?domain ?range WHERE {
    ?prop a owl:ObjectProperty .
    FILTER(STRSTARTS(STR(?prop), "https://sakuna.ph/"))
    FILTER NOT EXISTS {
        ?prop rdfs:subPropertyOf ?super .
        FILTER(?super != ?prop)
        FILTER(STRSTARTS(STR(?super), "https://sakuna.ph/"))
    }
    ?prop rdfs:domain ?domain .
    ?prop rdfs:range  ?range .
    FILTER(isIRI(?domain) && STRSTARTS(STR(?domain), "https://sakuna.ph/"))
    FILTER(isIRI(?range)  && STRSTARTS(STR(?range),  "https://sakuna.ph/"))
    OPTIONAL { ?prop rdfs:label ?label }
}
"""

_GRAPH_DATAPROPS_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:    <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?class ?propLabel ?range WHERE {
    ?prop a owl:DatatypeProperty .
    {
        ?prop rdfs:domain ?class .
        FILTER(isIRI(?class))
    } UNION {
        ?prop rdfs:domain/owl:unionOf/rdf:rest*/rdf:first ?class .
        FILTER(isIRI(?class))
    }
    FILTER(STRSTARTS(STR(?class), "https://sakuna.ph/"))
    FILTER NOT EXISTS {
        ?sub rdfs:subClassOf ?class .
        FILTER(?sub != ?class)
        FILTER(STRSTARTS(STR(?sub), "https://sakuna.ph/"))
        {
            ?prop rdfs:domain ?sub .
            FILTER(isIRI(?sub))
        } UNION {
            ?prop rdfs:domain/owl:unionOf/rdf:rest*/rdf:first ?sub .
            FILTER(isIRI(?sub))
        }
    }
    OPTIONAL { ?prop rdfs:label ?propLabel }
    OPTIONAL {
        ?prop rdfs:range ?range .
        FILTER(!STRSTARTS(STR(?range), "http://www.w3.org/2000/01/rdf-schema#"))
    }
}
"""

_CLASS_BLACKLIST: set[str] = {"DisasterTypeScheme"}

_NODE_GROUP: dict[str, str] = {
    "DisasterEvent": "core", "MajorEvent": "core", "Incident": "core",
    "Impact": "impact", "AffectedPopulation": "impact", "Casualties": "impact",
    "HousingDamage": "impact", "AgricultureDamage": "impact",
    "InfrastructureDamage": "impact", "DamageGeneral": "impact",
    "AirportDisruption": "impact", "FlightDisruption": "impact",
    "SeaportDisruption": "impact", "StrandedEvent": "impact",
    "PowerDisruption": "impact", "WaterDisruption": "impact",
    "CommunicationLineDisruption": "impact", "ClassSuspension": "impact",
    "WorkSuspension": "impact", "RoadAndBridgesDamage": "impact",
    "Response": "response", "Assistance": "response", "Relief": "response",
    "Recovery": "response", "DeclarationOfCalamity": "response",
    "Preparedness": "preparedness", "PreemptiveEvacuation": "preparedness",
    "Rescue": "preparedness",
    "Location": "location", "Country": "location", "IslandGroup": "location",
    "Region": "location", "Province": "location", "Municipality": "location",
    "City": "location", "SubMunicipality": "location", "Barangay": "location",
    "DisasterType": "type",
    "Source": "source",
}


def _range_label(range_iri: str | None) -> str:
    if not range_iri:
        return ""
    if "#" in range_iri:
        local = range_iri.rsplit("#", 1)[-1]
        return f"xsd:{local}" if "XMLSchema" in range_iri else local
    return range_iri.rsplit("/", 1)[-1]


def _build_graph(
    class_bindings: list[dict[Any, Any]],
    subclassof_bindings: list[dict[Any, Any]],
    objprop_bindings: list[dict[Any, Any]],
    dataprop_bindings: list[dict[Any, Any]],
) -> OntologyGraphResponse:
    data_props: dict[str, list[dict[Any, Any]]] = {}
    seen_dp: set[tuple[Any, Any, Any]] = set()
    for b in dataprop_bindings:
        class_iri = binding_value(b, "class")
        if not class_iri:
            continue
        class_local = class_iri.rsplit("/", 1)[-1]
        label = binding_value(b, "propLabel", "")
        rng = _range_label(binding_value(b, "range"))
        key = (class_local, label, rng)
        if key in seen_dp:
            continue
        seen_dp.add(key)
        data_props.setdefault(class_local, []).append({"label": label, "range": rng})

    nodes: list[dict[Any, Any]] = []
    for b in class_bindings:
        class_iri = binding_value(b, "class")
        if not class_iri:
            continue
        local = class_iri.rsplit("/", 1)[-1]
        if local in _CLASS_BLACKLIST:
            continue
        node: dict[Any, Any] = {
            "id": local,
            "label": binding_value(b, "label", local),
            "group": _NODE_GROUP.get(local, "source"),
            "definition": binding_value(b, "definition", ""),
        }
        props = data_props.get(local, [])
        if props:
            node["dataProperties"] = props
        nodes.append(node)

    links: list[dict[Any, Any]] = []
    for b in subclassof_bindings:
        child = binding_value(b, "child")
        parent = binding_value(b, "parent")
        if not (child and parent):
            continue
        links.append({
            "source": child.rsplit("/", 1)[-1],
            "target": parent.rsplit("/", 1)[-1],
            "type": "subClassOf",
            "label": "subClassOf",
        })

    seen_op: set[tuple[Any, Any, Any]] = set()
    for b in objprop_bindings:
        domain = binding_value(b, "domain")
        rng = binding_value(b, "range")
        label = binding_value(b, "label", "")
        if not (domain and rng):
            continue
        src = domain.rsplit("/", 1)[-1]
        tgt = rng.rsplit("/", 1)[-1]
        key = (src, tgt, label)
        if key in seen_op:
            continue
        seen_op.add(key)
        links.append({
            "source": src,
            "target": tgt,
            "type": "objectProperty",
            "label": label or "objectProperty",
        })

    for src, tgt, lbl in [
        ("DisasterEvent", "Source", "hasSource"),
        ("DisasterEvent", "Location", "hasLocation"),
        ("IslandGroup", "Country", "isPartOf"),
        ("Region", "IslandGroup", "isPartOf"),
        ("Province", "Region", "isPartOf"),
        ("Municipality", "Province", "isPartOf"),
        ("City", "Province", "isPartOf"),
        ("SubMunicipality", "Municipality", "isPartOf"),
        ("Barangay", "Municipality", "isPartOf"),
    ]:
        if not any(link["source"] == src and link["target"] == tgt for link in links):
            links.append({"source": src, "target": tgt, "type": "objectProperty", "label": lbl})

    return OntologyGraphResponse(nodes=nodes, links=links)


async def get_ontology_graph() -> OntologyGraphResponse:
    class_res, subclassof_res, objprop_res, dataprop_res = await asyncio.gather(
        execute_sparql(_GRAPH_CLASSES_QUERY),
        execute_sparql(_GRAPH_SUBCLASSOF_QUERY),
        execute_sparql(_GRAPH_OBJPROPS_QUERY),
        execute_sparql(_GRAPH_DATAPROPS_QUERY),
    )

    errors = [r for r in (class_res, subclassof_res, objprop_res, dataprop_res) if isinstance(r, str)]
    if errors:
        raise ServiceError(502, errors[0])

    return _build_graph(
        class_res.get("results", {}).get("bindings", []),
        subclassof_res.get("results", {}).get("bindings", []),
        objprop_res.get("results", {}).get("bindings", []),
        dataprop_res.get("results", {}).get("bindings", []),
    )

import asyncio
from typing import Any

from fastapi import APIRouter, HTTPException

from src.services.sparql_service import execute_sparql

router = APIRouter(prefix="/ontology", tags=["ontology"])


# ── helpers ────────────────────────────────────────────────────────────────────

def _val(binding: dict[Any, Any], key: str, default=None) -> str:
    """Extract the string value from a SPARQL result binding cell."""
    return binding.get(key, {}).get("value", default)


# ══════════════════════════════════════════════════════════════════════════════
# Core Ontology Graph
# ══════════════════════════════════════════════════════════════════════════════
# Nodes include data properties; links cover both subClassOf and object properties
# from any namespace whose domain AND range resolve to : classes.

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

# Only top-level : properties (no sub-properties, no external namespaces).
# Union-typed domain/range nodes fail the isIRI() filter, so properties like
# isPartOf are excluded — keeps the graph clean.
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

# Data properties grouped by their (possibly union) domain class.
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
    # RDFS scm-dom2 inference lifts every domain declaration up through rdfs:subClassOf,
    # so a property on :Casualties also appears on :Impact. Keep only the most-specific
    # (bottom-most) domain: exclude any class that has a : subclass which is also
    # declared as a domain for this property.
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
) -> dict[Any, Any]:
    # data properties keyed by class local-name → deduped list
    data_props: dict[str, list[dict[Any, Any]]] = {}
    seen_dp: set[tuple[Any, Any, Any]] = set()
    for b in dataprop_bindings:
        class_iri = _val(b, "class")
        if not class_iri:
            continue
        class_local = class_iri.rsplit("/", 1)[-1]
        label = _val(b, "propLabel", "")
        rng   = _range_label(_val(b, "range"))
        key   = (class_local, label, rng)
        if key in seen_dp:
            continue
        seen_dp.add(key)
        data_props.setdefault(class_local, []).append({"label": label, "range": rng})

    # nodes
    nodes: list[dict[Any, Any]] = []
    for b in class_bindings:
        class_iri = _val(b, "class")
        if not class_iri:
            continue
        local = class_iri.rsplit("/", 1)[-1]
        if local in _CLASS_BLACKLIST:
            continue
        node: dict[Any, Any] = {
            "id":         local,
            "label":      _val(b, "label", local),
            "group":      _NODE_GROUP.get(local, "source"),
            "definition": _val(b, "definition", ""),
        }
        props = data_props.get(local, [])
        if props:
            node["dataProperties"] = props
        nodes.append(node)

    # subClassOf links
    links: list[dict[Any, Any]] = []
    for b in subclassof_bindings:
        child  = _val(b, "child")
        parent = _val(b, "parent")
        if not (child and parent):
            continue
        links.append({
            "source": child.rsplit("/", 1)[-1],
            "target": parent.rsplit("/", 1)[-1],
            "type":   "subClassOf",
            "label":  "subClassOf",
        })

    # objectProperty links (deduplicated)
    seen_op: set[tuple[Any, Any, Any]] = set()
    for b in objprop_bindings:
        domain = _val(b, "domain")
        rng    = _val(b, "range")
        label  = _val(b, "label", "")
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
            "type":   "objectProperty",
            "label":  label or "objectProperty",
        })

    # hasSource / hasLocation use union domains so they won't appear from SPARQL.
    # Inject fallback links when nothing else covers them.
    for src, tgt, lbl in [
        ("DisasterEvent",   "Source",          "hasSource"),
        ("DisasterEvent",   "Location",        "hasLocation"),
        ("IslandGroup",     "Country",         "isPartOf"),
        ("Region",          "IslandGroup",     "isPartOf"),
        ("Province",        "Region",          "isPartOf"),
        ("Municipality",    "Province",        "isPartOf"),
        ("City",            "Province",        "isPartOf"),
        ("SubMunicipality", "Municipality",    "isPartOf"),
        ("Barangay",        "Municipality",    "isPartOf"),
    ]:
        if not any(lnk["source"] == src and lnk["target"] == tgt for lnk in links):
            links.append({"source": src, "target": tgt, "type": "objectProperty", "label": lbl})

    return {"nodes": nodes, "links": links}


@router.get("/graph")
async def get_ontology_graph():
    class_res, subclassof_res, objprop_res, dataprop_res = await asyncio.gather(
        execute_sparql(_GRAPH_CLASSES_QUERY),
        execute_sparql(_GRAPH_SUBCLASSOF_QUERY),
        execute_sparql(_GRAPH_OBJPROPS_QUERY),
        execute_sparql(_GRAPH_DATAPROPS_QUERY),
    )

    errors = [r for r in (class_res, subclassof_res, objprop_res, dataprop_res) if isinstance(r, str)]
    if errors:
        raise HTTPException(status_code=502, detail=errors[0])

    return _build_graph(
        class_res.get("results", {}).get("bindings", []),
        subclassof_res.get("results", {}).get("bindings", []),
        objprop_res.get("results", {}).get("bindings", []),
        dataprop_res.get("results", {}).get("bindings", []),
    )


# ══════════════════════════════════════════════════════════════════════════════
# Disaster Taxonomy Tree
# ══════════════════════════════════════════════════════════════════════════════
# DisasterType individuals are SKOS concepts stored as owl:NamedIndividual.
# Hierarchy uses skos:broader; top-level concepts (Natural, Technological)
# have no skos:broader and become direct children of the synthetic root node.

_TAXONOMY_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX skos:   <http://www.w3.org/2004/02/skos/core#>

SELECT ?concept ?label ?definition ?parent WHERE {
    ?concept a :DisasterType .
    ?concept skos:prefLabel ?label .
    OPTIONAL { ?concept skos:definition ?definition }
    OPTIONAL { ?concept skos:broader ?parent }
}
"""

# Group is assigned at certain "pivot" nodes and inherited downward.
# Children keep their parent's group unless they appear in this map.
_TAXONOMY_GROUP: dict[str, str] = {
    "Natural":             "natural",
    "Biological":          "biological",
    "Climatological":      "climatological",
    "Extraterrestrial":    "extraterrestrial",
    "Geophysical":         "geophysical",
    "Hydrological":        "hydrological",
    "Meteorological":      "meteorological",
    "Technological":       "tech",
    "ArmedConflict":       "armedconflict",
    "IndustrialAccident":  "industrial",
    "MiscellaneousAccident": "miscellaneous",
    "Transport":           "transport",
}

def _build_taxonomy_tree(bindings: list[dict[Any, Any]]) -> dict[Any, Any]:
    concepts: dict[str, dict[Any, Any]] = {}
    children_of: dict[str, list[str]] = {}

    for b in bindings:
        iri = _val(b, "concept")
        if not iri:
            continue
        local = iri.rsplit("/", 1)[-1]
        parent_iri = _val(b, "parent")

        if iri not in concepts:
            concepts[iri] = {
                "id":         local,
                "label":      _val(b, "label", local),
                "definition": _val(b, "definition", ""),
                "parent":     parent_iri,
            }
        if parent_iri:
            children_of.setdefault(parent_iri, []).append(iri)

    # Assign group top-down so children inherit from parent
    def assign_group(iri: str, inherited: str) -> None:
        node = concepts[iri]
        group = _TAXONOMY_GROUP.get(node["id"], inherited)
        node["group"] = group
        for child_iri in children_of.get(iri, []):
            assign_group(child_iri, group)

    top_level = [iri for iri, c in concepts.items() if not c["parent"]]
    for iri in top_level:
        assign_group(iri, "natural")

    def build_node(iri: str) -> dict[Any, Any]:
        n = concepts[iri]
        node = {
            "id":         n["id"],
            "label":      n["label"],
            "group":      n.get("group", "natural"),
            "definition": n["definition"],
        }
        kids = children_of.get(iri, [])
        if kids:
            node["children"] = [build_node(c) for c in kids]
        return node

    return {
        "id":         "root",
        "label":      "Disaster Types",
        "group":      "root",
        "definition": "SakunaGraPH Disaster Type Classification Scheme based on the Emergency Events Database (EM-DAT) classification.",
        "children":   [build_node(iri) for iri in top_level],
    }


@router.get("/taxonomy")
async def get_disaster_taxonomy():
    results = await execute_sparql(_TAXONOMY_QUERY)
    if isinstance(results, str):
        raise HTTPException(status_code=502, detail=results)

    bindings = results.get("results", {}).get("bindings", [])
    return _build_taxonomy_tree(bindings)


# ══════════════════════════════════════════════════════════════════════════════
# PSGC Location Graph
# ══════════════════════════════════════════════════════════════════════════════
# Only Regions, Provinces, and independent cities (HUC / ICC) are included.
# NCR municipalities (e.g. Pateros) are also included as city-level nodes
# because they report directly to the NCR region with no province in between.
#
# Node shapes:
#   Region:   { id, label, fullName, level:"Region",   island, population, psgcCode }
#   Province: { id, label,           level:"Province", island, incomeClass, population, psgcCode, regionId }
#   City:     { id, label,           level:"City",     island, population, psgcCode,
#               cityType:"HUC"|"ICC"|"Municipality", regionId, regionLabel, note? }
#
# Links: { source: child_code, target: region_code }

_PSGC_REGIONS_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos:   <http://www.w3.org/2004/02/skos/core#>

SELECT ?code ?label ?fullName ?population WHERE {
    ?r a :Region .
    ?r :psgcCode ?code .
    ?r rdfs:label     ?label .
    OPTIONAL { ?r skos:altLabel        ?fullName   }
    OPTIONAL { ?r :population2024 ?population }
}
"""

_PSGC_PROVINCES_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?code ?label ?population ?incomeClass ?regionCode WHERE {
    ?p a :Province .
    ?p :psgcCode ?code .
    ?p rdfs:label     ?label .
    ?p :isPartOf ?region .
    ?region a :Region .
    ?region :psgcCode ?regionCode .
    OPTIONAL { ?p :population2024      ?population  }
    OPTIONAL { ?p :incomeClassification ?incomeClass }
}
"""

# Covers HUC/ICC cities (isPartOf a Region directly) plus NCR municipalities.
_PSGC_CITIES_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:    <http://www.w3.org/2001/XMLSchema#>

SELECT ?code ?label ?population ?incomeClass ?cityClass ?regionCode ?regionLabel ?note WHERE {
    {
        ?c a :City .
        ?c :cityClass ?cityClass .
        FILTER(STR(?cityClass) = "HUC" || STR(?cityClass) = "ICC")
        ?c :isPartOf ?region .
        ?region a :Region .
    } UNION {
        # Pateros and any other municipality that reports directly to NCR
        ?c a :Municipality .
        BIND("Municipality" AS ?cityClass)
        ?c :isPartOf ?region .
        ?region :psgcCode "1300000000"^^xsd:string .
    }
    ?c :psgcCode ?code .
    ?c rdfs:label      ?label .
    ?region :psgcCode ?regionCode .
    ?region rdfs:label      ?regionLabel .
    OPTIONAL { ?c :population2024      ?population  }
    OPTIONAL { ?c :incomeClassification ?incomeClass }
    OPTIONAL { ?c rdfs:comment               ?note        }
}
"""

_ISLAND_LUZON    = {"01", "02", "03", "04", "05", "14", "17"}
_ISLAND_VISAYAS  = {"06", "07", "08", "18"}
_ISLAND_MINDANAO = {"09", "10", "11", "12", "16", "19"}

def _region_to_island(code: str) -> str:
    if code == "1300000000":
        return "NCR"
    prefix = code[:2]
    if prefix in _ISLAND_LUZON:
        return "Luzon"
    if prefix in _ISLAND_VISAYAS:
        return "Visayas"
    return "Mindanao"


def _build_psgc(
    region_bindings: list[dict[Any, Any]],
    province_bindings: list[dict[Any, Any]],
    city_bindings: list[dict[Any, Any]],
) -> dict[Any, Any]:
    nodes: list[dict[Any, Any]] = []
    links: list[dict[Any, Any]] = []

    for b in region_bindings:
        code  = _val(b, "code")
        label = _val(b, "label")
        nodes.append({
            "id":         code,
            "label":      label,
            "fullName":   _val(b, "fullName") or label,
            "level":      "Region",
            "island":     _region_to_island(code),
            "population": int(_val(b, "population", 0)),
            "psgcCode":   code,
        })

    for b in province_bindings:
        code        = _val(b, "code")
        region_code = _val(b, "regionCode")
        nodes.append({
            "id":          code,
            "label":       _val(b, "label"),
            "level":       "Province",
            "island":      _region_to_island(region_code),
            "incomeClass": _val(b, "incomeClass"),
            "population":  int(_val(b, "population", 0)),
            "psgcCode":    code,
            "regionId":    region_code,
        })
        links.append({"source": code, "target": region_code})

    for b in city_bindings:
        code        = _val(b, "code")
        region_code = _val(b, "regionCode")
        node: dict[Any, Any]  = {
            "id":          code,
            "label":       _val(b, "label"),
            "level":       "City",
            "island":      _region_to_island(region_code),
            "population":  int(_val(b, "population", 0)),
            "psgcCode":    code,
            "cityType":    _val(b, "cityClass"),
            "regionId":    region_code,
            "regionLabel": _val(b, "regionLabel"),
        }
        note = _val(b, "note")
        if note:
            node["note"] = note
        nodes.append(node)
        links.append({"source": code, "target": region_code})

    return {"nodes": nodes, "links": links}


@router.get("/psgc")
async def get_psgc_nodes():
    region_res, province_res, city_res = await asyncio.gather(
        execute_sparql(_PSGC_REGIONS_QUERY),
        execute_sparql(_PSGC_PROVINCES_QUERY),
        execute_sparql(_PSGC_CITIES_QUERY),
    )

    errors = [r for r in (region_res, province_res, city_res) if isinstance(r, str)]
    if errors:
        raise HTTPException(status_code=502, detail=errors[0])

    return _build_psgc(
        region_res.get("results", {}).get("bindings", []),
        province_res.get("results", {}).get("bindings", []),
        city_res.get("results", {}).get("bindings", []),
    )

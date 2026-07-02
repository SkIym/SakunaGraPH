import asyncio
from typing import Any

from src.services.common import ServiceError
from src.services.ontology.utils import binding_value
from src.services.sparql import execute_sparql

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

_ISLAND_LUZON = {"01", "02", "03", "04", "05", "14", "17"}
_ISLAND_VISAYAS = {"06", "07", "08", "18"}


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
        code = binding_value(b, "code")
        label = binding_value(b, "label")
        nodes.append({
            "id": code,
            "label": label,
            "fullName": binding_value(b, "fullName") or label,
            "level": "Region",
            "island": _region_to_island(code),
            "population": int(binding_value(b, "population", 0)),
            "psgcCode": code,
        })

    for b in province_bindings:
        code = binding_value(b, "code")
        region_code = binding_value(b, "regionCode")
        nodes.append({
            "id": code,
            "label": binding_value(b, "label"),
            "level": "Province",
            "island": _region_to_island(region_code),
            "incomeClass": binding_value(b, "incomeClass"),
            "population": int(binding_value(b, "population", 0)),
            "psgcCode": code,
            "regionId": region_code,
        })
        links.append({"source": code, "target": region_code})

    for b in city_bindings:
        code = binding_value(b, "code")
        region_code = binding_value(b, "regionCode")
        node: dict[Any, Any] = {
            "id": code,
            "label": binding_value(b, "label"),
            "level": "City",
            "island": _region_to_island(region_code),
            "population": int(binding_value(b, "population", 0)),
            "psgcCode": code,
            "cityType": binding_value(b, "cityClass"),
            "regionId": region_code,
            "regionLabel": binding_value(b, "regionLabel"),
        }
        note = binding_value(b, "note")
        if note:
            node["note"] = note
        nodes.append(node)
        links.append({"source": code, "target": region_code})

    return {"nodes": nodes, "links": links}


async def get_psgc_nodes() -> dict[Any, Any]:
    region_res, province_res, city_res = await asyncio.gather(
        execute_sparql(_PSGC_REGIONS_QUERY),
        execute_sparql(_PSGC_PROVINCES_QUERY),
        execute_sparql(_PSGC_CITIES_QUERY),
    )

    errors = [r for r in (region_res, province_res, city_res) if isinstance(r, str)]
    if errors:
        raise ServiceError(502, errors[0])

    return _build_psgc(
        region_res.get("results", {}).get("bindings", []),
        province_res.get("results", {}).get("bindings", []),
        city_res.get("results", {}).get("bindings", []),
    )

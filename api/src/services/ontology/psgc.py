import asyncio
from typing import Any

from src.schemas.ontology import (
    PsgcCitiesMunicipalitiesResponse,
    PsgcCityMunicipality,
    PsgcGraphResponse,
    PsgcProvince,
    PsgcProvincesResponse,
    PsgcRegion,
    PsgcRegionsResponse,
)
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

_PSGC_GRAPH_CITIES_QUERY = """
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

_PSGC_CITIES_MUNICIPALITIES_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?code ?label ?population ?incomeClass ?cityClass ?level
       ?parentCode ?parentLabel ?regionCode ?regionLabel ?note
WHERE {
    {
        ?c a :City .
        BIND("City" AS ?level)
        OPTIONAL { ?c :cityClass ?cityClass }
    } UNION {
        ?c a :Municipality .
        BIND("Municipality" AS ?level)
        BIND("Municipality" AS ?cityClass)
    }
    ?c :psgcCode ?code .
    ?c rdfs:label ?label .
    ?c :isPartOf ?parent .
    ?parent :psgcCode ?parentCode .
    ?parent rdfs:label ?parentLabel .
    ?parent :isPartOf* ?region .
    ?region a :Region .
    ?region :psgcCode ?regionCode .
    ?region rdfs:label ?regionLabel .
    OPTIONAL { ?c :population2024 ?population }
    OPTIONAL { ?c :incomeClassification ?incomeClass }
    OPTIONAL { ?c rdfs:comment ?note }
}
ORDER BY ?regionCode ?parentCode ?label
"""

# Barangays are intentionally disabled for now. The shape is kept here so the
# endpoint can be enabled when the graph can handle the much larger result set.
# _PSGC_BARANGAYS_QUERY = """
# PREFIX : <https://sakuna.ph/>
# PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
#
# SELECT ?code ?label ?population ?parentCode ?parentLabel WHERE {
#     ?b a :Barangay .
#     ?b :psgcCode ?code .
#     ?b rdfs:label ?label .
#     ?b :isPartOf ?parent .
#     ?parent :psgcCode ?parentCode .
#     ?parent rdfs:label ?parentLabel .
#     OPTIONAL { ?b :population2024 ?population }
# }
# """

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


def _int_binding(binding: dict[Any, Any], key: str) -> int:
    value = binding_value(binding, key, 0)
    if value in (None, ""):
        return 0
    return int(value)


def _result_bindings(result: dict[Any, Any]) -> list[dict[Any, Any]]:
    return result.get("results", {}).get("bindings", [])


async def _fetch_bindings(query: str) -> list[dict[Any, Any]]:
    result = await execute_sparql(query)
    if isinstance(result, str):
        raise ServiceError(502, result)
    return _result_bindings(result)


def _region_item(binding: dict[Any, Any]) -> PsgcRegion:
    code = binding_value(binding, "code", "")
    label = binding_value(binding, "label", "")
    return PsgcRegion(
        id=code,
        label=label,
        fullName=binding_value(binding, "fullName") or label,
        island=_region_to_island(code),
        population=_int_binding(binding, "population"),
        psgcCode=code,
    )


def _province_item(binding: dict[Any, Any]) -> PsgcProvince:
    code = binding_value(binding, "code", "")
    region_code = binding_value(binding, "regionCode", "")
    return PsgcProvince(
        id=code,
        label=binding_value(binding, "label", ""),
        island=_region_to_island(region_code),
        incomeClass=binding_value(binding, "incomeClass"),
        population=_int_binding(binding, "population"),
        psgcCode=code,
        regionId=region_code,
    )


def _city_municipality_item(binding: dict[Any, Any]) -> PsgcCityMunicipality:
    code = binding_value(binding, "code", "")
    region_code = binding_value(binding, "regionCode", "")
    level = binding_value(binding, "level", "City")
    return PsgcCityMunicipality(
        id=code,
        label=binding_value(binding, "label", ""),
        level=level,
        island=_region_to_island(region_code),
        population=_int_binding(binding, "population"),
        psgcCode=code,
        cityType=binding_value(binding, "cityClass") or level,
        incomeClass=binding_value(binding, "incomeClass"),
        regionId=region_code,
        regionLabel=binding_value(binding, "regionLabel"),
        parentId=binding_value(binding, "parentCode"),
        parentLabel=binding_value(binding, "parentLabel"),
        note=binding_value(binding, "note"),
    )


# def _barangay_item(binding: dict[Any, Any]) -> dict[Any, Any]:
#     code = binding_value(binding, "code", "")
#     return {
#         "id": code,
#         "label": binding_value(binding, "label", ""),
#         "level": "Barangay",
#         "population": _int_binding(binding, "population"),
#         "psgcCode": code,
#         "parentId": binding_value(binding, "parentCode"),
#         "parentLabel": binding_value(binding, "parentLabel"),
#     }


def _build_psgc(
    region_bindings: list[dict[Any, Any]],
    province_bindings: list[dict[Any, Any]],
    city_bindings: list[dict[Any, Any]],
) -> PsgcGraphResponse:
    nodes: list[PsgcRegion | PsgcProvince | PsgcCityMunicipality] = []
    links: list[dict[str, str]] = []

    for b in region_bindings:
        nodes.append(_region_item(b))

    for b in province_bindings:
        node = _province_item(b)
        nodes.append(node)
        links.append({"source": node.id, "target": node.regionId})

    for b in city_bindings:
        node = _city_municipality_item(b)
        nodes.append(node)
        links.append({"source": node.id, "target": node.regionId})

    return PsgcGraphResponse(nodes=nodes, links=links)


async def get_psgc_regions() -> PsgcRegionsResponse:
    bindings = await _fetch_bindings(_PSGC_REGIONS_QUERY)
    return PsgcRegionsResponse(
        regions=[_region_item(binding) for binding in bindings]
    )


async def get_psgc_provinces() -> PsgcProvincesResponse:
    bindings = await _fetch_bindings(_PSGC_PROVINCES_QUERY)
    return PsgcProvincesResponse(
        provinces=[_province_item(binding) for binding in bindings]
    )


async def get_psgc_cities_municipalities() -> PsgcCitiesMunicipalitiesResponse:
    bindings = await _fetch_bindings(_PSGC_CITIES_MUNICIPALITIES_QUERY)
    return PsgcCitiesMunicipalitiesResponse(
        citiesMunicipalities=[
            _city_municipality_item(binding) for binding in bindings
        ]
    )


# async def get_psgc_barangays() -> dict[Any, Any]:
#     bindings = await _fetch_bindings(_PSGC_BARANGAYS_QUERY)
#     return {"barangays": [_barangay_item(binding) for binding in bindings]}


async def get_psgc_nodes() -> PsgcGraphResponse:
    region_res, province_res, city_res = await asyncio.gather(
        execute_sparql(_PSGC_REGIONS_QUERY),
        execute_sparql(_PSGC_PROVINCES_QUERY),
        execute_sparql(_PSGC_GRAPH_CITIES_QUERY),
    )

    errors = [r for r in (region_res, province_res, city_res) if isinstance(r, str)]
    if errors:
        raise ServiceError(502, errors[0])

    return _build_psgc(
        _result_bindings(region_res),
        _result_bindings(province_res),
        _result_bindings(city_res),
    )

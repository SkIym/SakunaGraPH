from typing import Any

from src.schemas.ontology import TaxonomyNode
from src.services.common import ServiceError
from src.services.ontology.utils import binding_value
from src.services.sparql import execute_sparql

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

_TAXONOMY_GROUP: dict[str, str] = {
    "Natural": "natural",
    "Biological": "biological",
    "Climatological": "climatological",
    "Extraterrestrial": "extraterrestrial",
    "Geophysical": "geophysical",
    "Hydrological": "hydrological",
    "Meteorological": "meteorological",
    "Technological": "tech",
    "ArmedConflict": "armedconflict",
    "IndustrialAccident": "industrial",
    "MiscellaneousAccident": "miscellaneous",
    "Transport": "transport",
}


def _build_taxonomy_tree(bindings: list[dict[Any, Any]]) -> TaxonomyNode:
    concepts: dict[str, dict[Any, Any]] = {}
    children_of: dict[str, list[str]] = {}

    for b in bindings:
        iri = binding_value(b, "concept")
        if not iri:
            continue
        local = iri.rsplit("/", 1)[-1]
        parent_iri = binding_value(b, "parent")

        if iri not in concepts:
            concepts[iri] = {
                "id": local,
                "label": binding_value(b, "label", local),
                "definition": binding_value(b, "definition", ""),
                "parent": parent_iri,
            }
        if parent_iri:
            children_of.setdefault(parent_iri, []).append(iri)

    def assign_group(iri: str, inherited: str) -> None:
        node = concepts[iri]
        group = _TAXONOMY_GROUP.get(node["id"], inherited)
        node["group"] = group
        for child_iri in children_of.get(iri, []):
            assign_group(child_iri, group)

    top_level = [iri for iri, concept in concepts.items() if not concept["parent"]]
    for iri in top_level:
        assign_group(iri, "natural")

    def build_node(iri: str) -> TaxonomyNode:
        concept = concepts[iri]
        kids = children_of.get(iri, [])
        return TaxonomyNode(
            id=concept["id"],
            label=concept["label"],
            group=concept.get("group", "natural"),
            definition=concept["definition"],
            children=[build_node(child) for child in kids] if kids else None,
        )

    return TaxonomyNode(
        id="root",
        label="Disaster Types",
        group="root",
        definition=(
            "SakunaGraPH Disaster Type Classification Scheme based on the "
            "Emergency Events Database (EM-DAT) classification."
        ),
        children=[build_node(iri) for iri in top_level],
    )


async def get_disaster_taxonomy() -> TaxonomyNode:
    results = await execute_sparql(_TAXONOMY_QUERY)
    if isinstance(results, str):
        raise ServiceError(502, results)

    bindings = results.get("results", {}).get("bindings", [])
    return _build_taxonomy_tree(bindings)

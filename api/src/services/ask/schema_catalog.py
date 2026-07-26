import time
from typing import Any

from src.schemas.query_validation import SchemaCatalog, SchemaTerm, SchemaTermKind
from src.services.common import ServiceError
from src.services.ontology.utils import binding_value
from src.services.sparql import execute_sparql


SCHEMA_CATALOG_ERROR_CODE = "ask_schema_catalog"
_CACHE_TTL_SECONDS = 900.0
_cached_catalog: SchemaCatalog | None = None

_SCHEMA_CATALOG_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?term ?kind ?label ?domain ?range ?parent ?declaredType
WHERE {
  VALUES ?kind {
    owl:Class
    owl:ObjectProperty
    owl:DatatypeProperty
    owl:AnnotationProperty
    owl:NamedIndividual
  }
  ?term a ?kind .
  FILTER(STRSTARTS(STR(?term), "https://sakuna.ph/"))
  OPTIONAL { ?term (rdfs:label|skos:prefLabel) ?label }
  OPTIONAL { ?term rdfs:domain ?domain }
  OPTIONAL { ?term rdfs:range ?range }
  OPTIONAL { ?term (rdfs:subClassOf|rdfs:subPropertyOf) ?parent }
  OPTIONAL {
    ?term a ?declaredType .
    FILTER(?declaredType != ?kind)
  }
}
ORDER BY ?term
"""

_KINDS: dict[str, SchemaTermKind] = {
    "http://www.w3.org/2002/07/owl#Class": "class",
    "http://www.w3.org/2002/07/owl#ObjectProperty": "object_property",
    "http://www.w3.org/2002/07/owl#DatatypeProperty": "datatype_property",
    "http://www.w3.org/2002/07/owl#AnnotationProperty": "annotation_property",
    "http://www.w3.org/2002/07/owl#NamedIndividual": "individual",
}


def clear_schema_catalog_cache() -> None:
    global _cached_catalog
    _cached_catalog = None


def _local_name(iri: str) -> str:
    return iri.rstrip("/").rsplit("#", 1)[-1].rsplit("/", 1)[-1]


def _append_unique(target: list[str], value: str | None) -> None:
    if value and value not in target:
        target.append(value)


def _build_schema_catalog(bindings: list[dict[Any, Any]]) -> SchemaCatalog:
    terms: dict[str, SchemaTerm] = {}
    for binding in bindings:
        iri = binding_value(binding, "term", "")
        kind_iri = binding_value(binding, "kind", "")
        kind = _KINDS.get(kind_iri)
        if not iri or kind is None:
            continue
        term = terms.get(iri)
        if term is None:
            local = _local_name(iri)
            term = SchemaTerm(
                iri=iri,
                local_name=local,
                label=binding_value(binding, "label") or local,
                kind=kind,
            )
            terms[iri] = term
        _append_unique(term.domains, binding_value(binding, "domain"))
        _append_unique(term.ranges, binding_value(binding, "range"))
        _append_unique(term.parents, binding_value(binding, "parent"))
        _append_unique(term.types, binding_value(binding, "declaredType"))
    return SchemaCatalog(terms=terms, loaded_at_monotonic=time.monotonic())


async def get_schema_catalog() -> SchemaCatalog:
    global _cached_catalog
    now = time.monotonic()
    if (
        _cached_catalog is not None
        and now - _cached_catalog.loaded_at_monotonic <= _CACHE_TTL_SECONDS
    ):
        return _cached_catalog

    result = await execute_sparql(_SCHEMA_CATALOG_QUERY)
    if isinstance(result, str):
        raise ServiceError(
            502,
            f"Could not load the GraphDB schema catalog: {result}",
            code=SCHEMA_CATALOG_ERROR_CODE,
        )
    bindings = result.get("results", {}).get("bindings", [])
    catalog = _build_schema_catalog(bindings)
    if not catalog.terms:
        raise ServiceError(
            502,
            "GraphDB returned an empty SakunaGraPH schema catalog.",
            code=SCHEMA_CATALOG_ERROR_CODE,
        )
    _cached_catalog = catalog
    return catalog

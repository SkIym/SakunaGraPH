from fastapi import APIRouter, HTTPException

from src.schemas.ontology import (
    OntologyGraphResponse,
    PsgcCitiesMunicipalitiesResponse,
    PsgcGraphResponse,
    PsgcProvincesResponse,
    PsgcRegionsResponse,
    TaxonomyNode,
)
from src.services.common import ServiceError
from src.services.ontology import (
    get_disaster_taxonomy as get_disaster_taxonomy_service,
    get_ontology_graph as get_ontology_graph_service,
    # get_psgc_barangays as get_psgc_barangays_service,
    get_psgc_cities_municipalities as get_psgc_cities_municipalities_service,
    get_psgc_nodes as get_psgc_nodes_service,
    get_psgc_provinces as get_psgc_provinces_service,
    get_psgc_regions as get_psgc_regions_service,
)

router = APIRouter(prefix="/ontology", tags=["ontology"])


def _to_http_error(exc: ServiceError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.get(
    "/graph",
    response_model=OntologyGraphResponse,
    response_model_exclude_none=True,
)
async def get_ontology_graph() -> OntologyGraphResponse:
    try:
        return await get_ontology_graph_service()
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get(
    "/taxonomy",
    response_model=TaxonomyNode,
    response_model_exclude_none=True,
)
async def get_disaster_taxonomy() -> TaxonomyNode:
    try:
        return await get_disaster_taxonomy_service()
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get(
    "/psgc",
    response_model=PsgcGraphResponse,
    response_model_exclude_none=True,
)
async def get_psgc_nodes() -> PsgcGraphResponse:
    try:
        return await get_psgc_nodes_service()
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/psgc/regions", response_model=PsgcRegionsResponse)
async def get_psgc_regions() -> PsgcRegionsResponse:
    try:
        return await get_psgc_regions_service()
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get(
    "/psgc/provinces",
    response_model=PsgcProvincesResponse,
    response_model_exclude_none=True,
)
async def get_psgc_provinces() -> PsgcProvincesResponse:
    try:
        return await get_psgc_provinces_service()
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get(
    "/psgc/cities-municipalities",
    response_model=PsgcCitiesMunicipalitiesResponse,
    response_model_exclude_none=True,
)
async def get_psgc_cities_municipalities() -> PsgcCitiesMunicipalitiesResponse:
    try:
        return await get_psgc_cities_municipalities_service()
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


# @router.get("/psgc/barangays")
# async def get_psgc_barangays() -> PsgcBarangaysResponse:
#     try:
#         return await get_psgc_barangays_service()
#     except ServiceError as exc:
#         raise _to_http_error(exc) from exc

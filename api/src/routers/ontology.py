from typing import Any

from fastapi import APIRouter, HTTPException

from src.services.common import ServiceError
from src.services.ontology import (
    get_disaster_taxonomy as get_disaster_taxonomy_service,
    get_ontology_graph as get_ontology_graph_service,
    get_psgc_nodes as get_psgc_nodes_service,
)

router = APIRouter(prefix="/ontology", tags=["ontology"])


def _to_http_error(exc: ServiceError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.get("/graph")
async def get_ontology_graph() -> dict[Any, Any]:
    try:
        return await get_ontology_graph_service()
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/taxonomy")
async def get_disaster_taxonomy() -> dict[Any, Any]:
    try:
        return await get_disaster_taxonomy_service()
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/psgc")
async def get_psgc_nodes() -> dict[Any, Any]:
    try:
        return await get_psgc_nodes_service()
    except ServiceError as exc:
        raise _to_http_error(exc) from exc

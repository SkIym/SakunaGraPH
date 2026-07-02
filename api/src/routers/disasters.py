from typing import Any

from fastapi import APIRouter, HTTPException

from src.services.common import ServiceError
from src.services.disasters import (
    get_disaster_organizations,
    get_disaster_sources,
    get_event_impact,
)

router = APIRouter(tags=["disasters"])


def _to_http_error(exc: ServiceError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.get("/events/{uri:path}/{impact}")
async def event_impact(uri: str, impact: str) -> dict[str, Any]:
    try:
        return await get_event_impact(uri=uri, impact=impact)
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/disasters/{uri:path}/organizations")
async def disaster_organizations(uri: str) -> dict[str, Any]:
    try:
        return await get_disaster_organizations(uri=uri)
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/disasters/{uri:path}/sources")
async def disaster_sources(uri: str) -> dict[str, Any]:
    try:
        return await get_disaster_sources(uri=uri)
    except ServiceError as exc:
        raise _to_http_error(exc) from exc

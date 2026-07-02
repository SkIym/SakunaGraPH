from typing import Any

from fastapi import APIRouter, HTTPException, Query

from src.services.common import ServiceError
from src.services.map import (
    EventMode,
    EventScope,
    EventType,
    get_events,
    get_province_events,
    get_region_events,
)

router = APIRouter(prefix="/map", tags=["map"])


def _to_http_error(exc: ServiceError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.get("/events")
async def events(
    scope: EventScope,
    id: str,
    mode: EventMode = Query("major"),
    page: int = Query(1, ge=1),
) -> dict[str, Any]:
    try:
        return await get_events(scope=scope, id=id, mode=mode, page=page)
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/events/region/{psgc}")
async def region_events(
    psgc: str,
    event_type: EventType = Query("MajorEvent"),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
) -> dict[str, Any]:
    try:
        return await get_region_events(psgc=psgc, event_type=event_type, page=page, limit=limit)
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/events/province/{psgc}")
async def province_events(
    psgc: str,
    event_type: EventType = Query("MajorEvent"),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
) -> dict[str, Any]:
    try:
        return await get_province_events(psgc=psgc, event_type=event_type, page=page, limit=limit)
    except ServiceError as exc:
        raise _to_http_error(exc) from exc

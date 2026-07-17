from fastapi import APIRouter, HTTPException, Query

from src.schemas.map import EventMode, EventScope, MapEventsResponse
from src.services.common import ServiceError
from src.services.map import get_events

router = APIRouter(prefix="/map", tags=["map"])


def _to_http_error(exc: ServiceError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.get("/events", response_model=MapEventsResponse)
async def events(
    scope: EventScope,
    id: str,
    mode: EventMode = Query("major"),
    page: int = Query(1, ge=1),
) -> MapEventsResponse:
    try:
        return await get_events(scope=scope, id=id, mode=mode, page=page)
    except ServiceError as exc:
        raise _to_http_error(exc) from exc

from datetime import date

from fastapi import APIRouter, HTTPException, Query, Response

from src.schemas.analysis import (
    AnalysisEventsResponse,
    AnalysisEventSortBy,
    AnalysisEventType,
    AnalysisFilterOptionsResponse,
    AnalysisSortDirection,
)
from src.services.analysis import (
    get_analysis_events,
    get_analysis_events_export,
    get_filter_options as get_filter_options_service,
    make_analysis_filters,
)
from src.services.common import ServiceError

router = APIRouter(prefix="/analysis", tags=["analysis"])


def _to_http_error(exc: ServiceError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.get(
    "/filter-options",
    response_model=AnalysisFilterOptionsResponse,
    response_model_exclude_none=True,
)
async def get_filter_options() -> AnalysisFilterOptionsResponse:
    try:
        return await get_filter_options_service()
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/events/export.csv", response_class=Response)
async def export_events(
    event_type: AnalysisEventType = Query("all"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    location_ids: list[str] = Query(default_factory=list),
    disaster_types: list[str] = Query(default_factory=list),
    q: str | None = Query(None, max_length=200),
    sort_by: AnalysisEventSortBy = Query("startDate"),
    sort_dir: AnalysisSortDirection = Query("desc"),
) -> Response:
    try:
        filters = make_analysis_filters(
            event_type=event_type,
            start_date=start_date,
            end_date=end_date,
            location_ids=location_ids,
            disaster_types=disaster_types,
            q=q,
        )
        content = await get_analysis_events_export(
            filters=filters,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        return Response(
            content=content,
            media_type="text/csv",
            headers={
                "Content-Disposition": 'attachment; filename="sakunagraph-events.csv"'
            },
        )
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get(
    "/events",
    response_model=AnalysisEventsResponse,
    response_model_exclude_none=False,
)
async def events(
    event_type: AnalysisEventType = Query("all"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    location_ids: list[str] = Query(default_factory=list),
    disaster_types: list[str] = Query(default_factory=list),
    q: str | None = Query(None, max_length=200),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
    sort_by: AnalysisEventSortBy = Query("startDate"),
    sort_dir: AnalysisSortDirection = Query("desc"),
) -> AnalysisEventsResponse:
    try:
        filters = make_analysis_filters(
            event_type=event_type,
            start_date=start_date,
            end_date=end_date,
            location_ids=location_ids,
            disaster_types=disaster_types,
            q=q,
        )
        return await get_analysis_events(
            filters=filters,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
    except ServiceError as exc:
        raise _to_http_error(exc) from exc

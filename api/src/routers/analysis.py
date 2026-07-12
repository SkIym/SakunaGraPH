from datetime import date

from fastapi import APIRouter, HTTPException, Query, Response

from src.schemas.analysis import (
    AnalysisDamageAffectedResponse,
    AnalysisDamageHistogramResponse,
    AnalysisDisasterCountGroupBy,
    AnalysisDisasterCountsResponse,
    AnalysisDisasterRankingsResponse,
    AnalysisEventsResponse,
    AnalysisEventSortBy,
    AnalysisEventType,
    AnalysisFilterOptionsResponse,
    AnalysisRegionRankingsResponse,
    AnalysisSortDirection,
    AnalysisSummaryResponse,
    AnalysisVictimTrendsResponse,
)
from src.services.analysis import (
    get_damage_histogram,
    get_damage_vs_affected,
    get_disaster_counts,
    get_disaster_rankings,
    get_analysis_events,
    get_analysis_events_export,
    get_filter_options as get_filter_options_service,
    get_region_rankings,
    get_summary,
    get_victim_trends,
    make_analysis_filters,
)
from src.services.common import ServiceError

router = APIRouter(prefix="/analysis", tags=["analysis"])


def _to_http_error(exc: ServiceError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


def _make_filters(
    *,
    event_type: AnalysisEventType,
    start_date: date | None,
    end_date: date | None,
    location_ids: list[str],
    disaster_types: list[str],
    q: str | None,
    location_id: str | None = None,
    disaster_type: str | None = None,
):
    return make_analysis_filters(
        event_type=event_type,
        start_date=start_date,
        end_date=end_date,
        location_ids=[*location_ids, *([location_id] if location_id else [])],
        disaster_types=[
            *disaster_types,
            *([disaster_type] if disaster_type else []),
        ],
        q=q,
    )


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


@router.get("/summary", response_model=AnalysisSummaryResponse)
async def summary(
    event_type: AnalysisEventType = Query("all"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    location_ids: list[str] = Query(default_factory=list),
    disaster_types: list[str] = Query(default_factory=list),
    q: str | None = Query(None, max_length=200),
) -> AnalysisSummaryResponse:
    try:
        return await get_summary(
            _make_filters(
                event_type=event_type,
                start_date=start_date,
                end_date=end_date,
                location_ids=location_ids,
                disaster_types=disaster_types,
                q=q,
            )
        )
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/disaster-counts", response_model=AnalysisDisasterCountsResponse)
async def disaster_counts(
    event_type: AnalysisEventType = Query("all"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    location_ids: list[str] = Query(default_factory=list),
    disaster_types: list[str] = Query(default_factory=list),
    q: str | None = Query(None, max_length=200),
    group_by: AnalysisDisasterCountGroupBy = Query("taxonomy"),
) -> AnalysisDisasterCountsResponse:
    try:
        return await get_disaster_counts(
            _make_filters(
                event_type=event_type,
                start_date=start_date,
                end_date=end_date,
                location_ids=location_ids,
                disaster_types=disaster_types,
                q=q,
            ),
            group_by,
        )
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/victim-trends", response_model=AnalysisVictimTrendsResponse)
async def victim_trends(
    event_type: AnalysisEventType = Query("all"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    location_ids: list[str] = Query(default_factory=list),
    disaster_types: list[str] = Query(default_factory=list),
    q: str | None = Query(None, max_length=200),
    disaster_type: str | None = Query(None, max_length=120),
) -> AnalysisVictimTrendsResponse:
    try:
        return await get_victim_trends(
            _make_filters(
                event_type=event_type,
                start_date=start_date,
                end_date=end_date,
                location_ids=location_ids,
                disaster_types=disaster_types,
                disaster_type=disaster_type,
                q=q,
            )
        )
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/region-rankings", response_model=AnalysisRegionRankingsResponse)
async def region_rankings(
    event_type: AnalysisEventType = Query("all"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    location_ids: list[str] = Query(default_factory=list),
    disaster_types: list[str] = Query(default_factory=list),
    q: str | None = Query(None, max_length=200),
    disaster_type: str | None = Query(None, max_length=120),
) -> AnalysisRegionRankingsResponse:
    try:
        return await get_region_rankings(
            _make_filters(
                event_type=event_type,
                start_date=start_date,
                end_date=end_date,
                location_ids=location_ids,
                disaster_types=disaster_types,
                disaster_type=disaster_type,
                q=q,
            )
        )
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/disaster-rankings", response_model=AnalysisDisasterRankingsResponse)
async def disaster_rankings(
    event_type: AnalysisEventType = Query("all"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    location_ids: list[str] = Query(default_factory=list),
    disaster_types: list[str] = Query(default_factory=list),
    q: str | None = Query(None, max_length=200),
    location_id: str | None = Query(None, max_length=20),
) -> AnalysisDisasterRankingsResponse:
    try:
        return await get_disaster_rankings(
            _make_filters(
                event_type=event_type,
                start_date=start_date,
                end_date=end_date,
                location_ids=location_ids,
                disaster_types=disaster_types,
                location_id=location_id,
                q=q,
            )
        )
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/damage-histogram", response_model=AnalysisDamageHistogramResponse)
async def damage_histogram(
    event_type: AnalysisEventType = Query("all"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    location_ids: list[str] = Query(default_factory=list),
    disaster_types: list[str] = Query(default_factory=list),
    q: str | None = Query(None, max_length=200),
    bins: int = Query(10, ge=1, le=50),
    unit: str | None = Query(None, max_length=80),
) -> AnalysisDamageHistogramResponse:
    try:
        return await get_damage_histogram(
            _make_filters(
                event_type=event_type,
                start_date=start_date,
                end_date=end_date,
                location_ids=location_ids,
                disaster_types=disaster_types,
                q=q,
            ),
            bins=bins,
            unit=unit,
        )
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/damage-vs-affected", response_model=AnalysisDamageAffectedResponse)
async def damage_vs_affected(
    event_type: AnalysisEventType = Query("all"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    location_ids: list[str] = Query(default_factory=list),
    disaster_types: list[str] = Query(default_factory=list),
    q: str | None = Query(None, max_length=200),
) -> AnalysisDamageAffectedResponse:
    try:
        return await get_damage_vs_affected(
            _make_filters(
                event_type=event_type,
                start_date=start_date,
                end_date=end_date,
                location_ids=location_ids,
                disaster_types=disaster_types,
                q=q,
            )
        )
    except ServiceError as exc:
        raise _to_http_error(exc) from exc

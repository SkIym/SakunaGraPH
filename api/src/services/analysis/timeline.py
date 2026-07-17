import re
from collections import defaultdict
from datetime import date
from typing import Any

from src.schemas.analysis import (
    AnalysisCalendarItem,
    AnalysisCalendarResponse,
    AnalysisDisasterCount,
    AnalysisTimelineBucket,
    AnalysisTimelineCategoryStack,
    AnalysisTimelineCategoryStacksResponse,
    AnalysisTimelineDateEventsResponse,
)
from src.services.analysis.common import AnalysisFilters
from src.services.analysis.events import get_all_analysis_events
from src.services.common import ServiceError
from src.services.ontology import get_disaster_taxonomy

_DATE_PREFIX_RE = re.compile(r"^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$")


def _valid_date_prefix(value: str) -> str:
    match = _DATE_PREFIX_RE.fullmatch(value)
    if not match:
        raise ServiceError(422, "date_prefix must be YYYY, YYYY-MM, or YYYY-MM-DD")

    year, month, day = match.groups()
    if month is None:
        return year
    if day is None:
        try:
            date(int(year), int(month), 1)
        except ValueError:
            raise ServiceError(422, "date_prefix contains an invalid month") from None
        return f"{year}-{month}"
    try:
        date(int(year), int(month), int(day))
    except ValueError:
        raise ServiceError(422, "date_prefix contains an invalid date") from None
    return f"{year}-{month}-{day}"


def _calendar_items(
    events: list[Any],
    *,
    period_length: int,
    include_impacts: bool,
) -> list[AnalysisCalendarItem]:
    items: dict[str, AnalysisCalendarItem] = {}
    for event in events:
        period = event.startDate[:period_length]
        if not period:
            continue
        item = items.setdefault(
            period,
            AnalysisCalendarItem(
                period=period,
                count=0,
                dead=0 if include_impacts else None,
                injured=0 if include_impacts else None,
                missing=0 if include_impacts else None,
            ),
        )
        item.count += 1
        if include_impacts:
            item.dead = (item.dead or 0) + event.impact.dead
            item.injured = (item.injured or 0) + event.impact.injured
            item.missing = (item.missing or 0) + event.impact.missing
    return [items[period] for period in sorted(items)]


async def _taxonomy_groups() -> dict[str, tuple[str, str]]:
    taxonomy = await get_disaster_taxonomy()
    groups: dict[str, tuple[str, str]] = {}

    def visit(node: Any, group: tuple[str, str] | None) -> None:
        current_group = group
        if node.id != "root" and group is None:
            current_group = (node.id, node.label)
        if current_group is not None and node.id != "root":
            groups[node.id] = current_group
        for child in node.children or []:
            visit(child, current_group)

    for child in taxonomy.children or []:
        visit(child, None)
    return groups


async def get_calendar_years(
    filters: AnalysisFilters,
    *,
    include_impacts: bool,
) -> AnalysisCalendarResponse:
    return AnalysisCalendarResponse(
        items=_calendar_items(
            await get_all_analysis_events(filters),
            period_length=4,
            include_impacts=include_impacts,
        )
    )


async def get_calendar_months(
    filters: AnalysisFilters,
    *,
    year: int,
    include_impacts: bool,
) -> AnalysisCalendarResponse:
    prefix = f"{year:04d}-"
    events = [
        event for event in await get_all_analysis_events(filters)
        if event.startDate.startswith(prefix)
    ]
    return AnalysisCalendarResponse(
        items=_calendar_items(events, period_length=7, include_impacts=include_impacts)
    )


async def get_calendar_days(
    filters: AnalysisFilters,
    *,
    year: int,
    month: int,
    include_impacts: bool,
) -> AnalysisCalendarResponse:
    prefix = f"{year:04d}-{month:02d}-"
    events = [
        event for event in await get_all_analysis_events(filters)
        if event.startDate.startswith(prefix)
    ]
    return AnalysisCalendarResponse(
        items=_calendar_items(events, period_length=10, include_impacts=include_impacts)
    )


async def get_category_stacks(
    filters: AnalysisFilters,
    *,
    bucket: AnalysisTimelineBucket,
) -> AnalysisTimelineCategoryStacksResponse:
    events = await get_all_analysis_events(filters)
    groups = await _taxonomy_groups()
    counts: dict[str, dict[str, AnalysisDisasterCount]] = defaultdict(dict)

    for event in events:
        if len(event.startDate) < 7:
            continue
        period = event.startDate[:7] if bucket == "month_year" else event.startDate[5:7]
        seen: set[str] = set()
        for disaster_type in event.disasterTypes:
            category_id, category_label = groups.get(
                disaster_type.id,
                (disaster_type.id, disaster_type.label),
            )
            if category_id in seen:
                continue
            seen.add(category_id)
            category = counts[period].setdefault(
                category_id,
                AnalysisDisasterCount(
                    id=category_id,
                    label=category_label,
                    count=0,
                ),
            )
            category.count += 1

    return AnalysisTimelineCategoryStacksResponse(
        bucket=bucket,
        items=[
            AnalysisTimelineCategoryStack(
                period=period,
                categories=sorted(
                    categories.values(),
                    key=lambda item: (-item.count, item.label.casefold()),
                ),
            )
            for period, categories in sorted(counts.items())
        ],
    )


async def get_date_events(
    filters: AnalysisFilters,
    *,
    date_prefix: str,
) -> AnalysisTimelineDateEventsResponse:
    prefix = _valid_date_prefix(date_prefix)
    items = [
        event for event in await get_all_analysis_events(filters)
        if event.startDate.startswith(prefix)
    ]
    return AnalysisTimelineDateEventsResponse(date_prefix=prefix, items=items)

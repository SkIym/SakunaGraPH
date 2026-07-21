import json

from src.schemas.analysis import AnalysisEvent, AnalysisSummaryResponse
from src.schemas.ask_execution import (
    AskServiceRoute,
    DeterministicAskResult,
    QueryArtifact,
)
from src.schemas.entity_resolution import ResolvedAskPlan
from src.services.analysis import (
    get_analysis_events,
    get_disaster_rankings,
    get_region_rankings,
    get_summary,
    get_victim_trends,
)
from src.services.ask.query_compiler import analysis_filters_from_plan, compile_query
from src.services.disasters import get_disaster_sources, get_event_details


def select_service_route(resolved: ResolvedAskPlan) -> AskServiceRoute | None:
    plan = resolved.plan
    has_service_unsupported_scope = bool(resolved.events or resolved.organizations)
    if plan.intent == "event_details" and len(resolved.events) == 1:
        return "event_details"
    if plan.intent == "source_lookup" and len(resolved.events) == 1:
        return "event_sources"
    if has_service_unsupported_scope:
        return None
    if plan.intent == "list_events" and plan.group_by is None:
        return "analysis_events"
    if plan.intent == "event_count" and plan.group_by is None:
        return "analysis_event_count"
    if plan.intent == "impact_summary" and plan.group_by is None:
        return "analysis_summary"
    if (
        plan.intent == "victim_trend"
        and plan.group_by in {None, "year"}
        and plan.metric in {None, "dead", "injured", "missing"}
    ):
        return "analysis_victim_trends"
    if (
        plan.intent == "region_ranking"
        and plan.group_by in {None, "region"}
        and plan.metric in {None, "events"}
    ):
        return "analysis_region_rankings"
    if (
        plan.intent == "disaster_ranking"
        and plan.group_by in {None, "disaster_type"}
        and plan.metric in {None, "dead"}
    ):
        return "analysis_disaster_rankings"
    return None


def _event_row(event: AnalysisEvent) -> dict[str, str]:
    damage = [item.model_dump(mode="json") for item in event.impact.damageByUnit]
    return {
        "event": event.event,
        "eventName": event.eventName,
        "eventClass": event.eventType,
        "startDate": event.startDate,
        "endDate": event.endDate or "",
        "locations": ", ".join(item.label for item in event.locations),
        "disasterTypes": ", ".join(item.label for item in event.disasterTypes),
        "source": event.source or "",
        "dead": str(event.impact.dead),
        "injured": str(event.impact.injured),
        "missing": str(event.impact.missing),
        "affectedFamilies": str(event.impact.affectedFamilies),
        "affectedPersons": str(event.impact.affectedPersons),
        "damage": json.dumps(damage, ensure_ascii=False, separators=(",", ":")),
    }


def _summary_rows(summary: AnalysisSummaryResponse, metric: str | None) -> list[dict[str, str]]:
    field_names = {
        "dead": "dead",
        "injured": "injured",
        "missing": "missing",
        "affected_persons": "affectedPersons",
        "affected_families": "affectedFamilies",
        "events": "record_count",
    }
    if metric == "damage":
        return [
            {"metric": "damage", "total": str(item.amount), "unit": item.unit}
            for item in summary.damage
        ]
    if metric in field_names:
        return [{"metric": metric, "total": str(getattr(summary, field_names[metric]))}]
    rows = [
        {"metric": "events", "total": str(summary.record_count)},
        {"metric": "dead", "total": str(summary.dead)},
        {"metric": "injured", "total": str(summary.injured)},
        {"metric": "missing", "total": str(summary.missing)},
        {"metric": "affected_families", "total": str(summary.affectedFamilies)},
        {"metric": "affected_persons", "total": str(summary.affectedPersons)},
    ]
    rows.extend(
        {"metric": "damage", "total": str(item.amount), "unit": item.unit}
        for item in summary.damage
    )
    return rows


def _bounded_ranked_rows(
    rows: list[dict[str, str]],
    resolved: ResolvedAskPlan,
) -> list[dict[str, str]]:
    if resolved.plan.sort_direction == "asc":
        rows.reverse()
    return rows[: resolved.plan.limit]


def service_query_artifact(
    resolved: ResolvedAskPlan,
    route: AskServiceRoute,
) -> QueryArtifact:
    artifact = compile_query(resolved, origin="service")
    columns = {
        "analysis_events": [
            "event",
            "eventName",
            "eventClass",
            "startDate",
            "endDate",
            "locations",
            "disasterTypes",
            "source",
            "dead",
            "injured",
            "missing",
            "affectedFamilies",
            "affectedPersons",
            "damage",
        ],
        "analysis_event_count": ["total"],
        "analysis_summary": (
            ["metric", "total", "unit"]
            if resolved.plan.metric in {None, "damage"}
            else ["metric", "total"]
        ),
        "analysis_victim_trends": (
            ["group", "groupLabel", "total"]
            if resolved.plan.metric
            else ["year", "dead", "injured", "missing"]
        ),
        "analysis_region_rankings": ["group", "groupLabel", "total"],
        "analysis_disaster_rankings": ["group", "groupLabel", "total"],
        "event_details": [
            "event",
            "eventName",
            "eventClass",
            "startDate",
            "endDate",
            "locations",
            "disasterTypes",
            "remarks",
            "sources",
        ],
        "event_sources": ["source", "sourceLabel", "recordCount", "records"],
    }[route]
    return artifact.model_copy(
        update={
            "service_route": route,
            "expected_columns": columns,
        }
    )


async def execute_service_route(
    resolved: ResolvedAskPlan,
    artifact: QueryArtifact,
) -> DeterministicAskResult:
    route = artifact.service_route
    if route is None:
        raise ValueError("A service-backed query artifact requires service_route.")
    filters = analysis_filters_from_plan(resolved)
    truncated = False

    if route == "analysis_events":
        response = await get_analysis_events(
            filters=filters,
            page=1,
            page_size=resolved.plan.limit,
            sort_by="startDate",
            sort_dir=resolved.plan.sort_direction,
        )
        rows = [_event_row(event) for event in response.items]
        truncated = response.total > len(response.items)
    elif route == "analysis_event_count":
        response = await get_analysis_events(
            filters=filters,
            page=1,
            page_size=1,
            sort_by="startDate",
            sort_dir=resolved.plan.sort_direction,
        )
        rows = [{"total": str(response.total)}]
    elif route == "analysis_summary":
        response = await get_summary(filters)
        rows = _summary_rows(response, resolved.plan.metric)
    elif route == "analysis_victim_trends":
        response = await get_victim_trends(filters)
        if resolved.plan.metric:
            rows = [
                {
                    "group": str(item.year),
                    "groupLabel": str(item.year),
                    "total": str(getattr(item, resolved.plan.metric)),
                }
                for item in response.items
            ]
        else:
            rows = [
                {
                    "year": str(item.year),
                    "dead": str(item.dead),
                    "injured": str(item.injured),
                    "missing": str(item.missing),
                }
                for item in response.items
            ]
        truncated = len(rows) > resolved.plan.limit
        if resolved.plan.sort_direction == "desc":
            rows.reverse()
        rows = rows[: resolved.plan.limit]
    elif route == "analysis_region_rankings":
        response = await get_region_rankings(filters)
        all_rows = [
            {
                "group": item.id,
                "groupLabel": item.label,
                "total": str(item.count),
            }
            for item in response.items
        ]
        truncated = len(all_rows) > resolved.plan.limit
        rows = _bounded_ranked_rows(all_rows, resolved)
    elif route == "analysis_disaster_rankings":
        response = await get_disaster_rankings(filters)
        all_rows = [
            {
                "group": item.id,
                "groupLabel": item.label,
                "total": str(item.dead),
            }
            for item in response.items
        ]
        truncated = len(all_rows) > resolved.plan.limit
        rows = _bounded_ranked_rows(all_rows, resolved)
    elif route == "event_details":
        response = await get_event_details(resolved.events[0].iri)
        rows = [
            {
                "event": response.event,
                "eventName": response.name,
                "eventClass": response.eventType,
                "startDate": response.startDate or "",
                "endDate": response.endDate or "",
                "locations": json.dumps(
                    [item.model_dump(mode="json") for item in response.locations],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                "disasterTypes": json.dumps(
                    [item.model_dump(mode="json") for item in response.disasterTypes],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                "remarks": json.dumps(response.remarks, ensure_ascii=False),
                "sources": json.dumps(
                    [item.model_dump(mode="json") for item in response.sources],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            }
        ]
    elif route == "event_sources":
        response = await get_disaster_sources(resolved.events[0].iri)
        rows = [
            {
                "source": item.uri,
                "sourceLabel": item.label,
                "recordCount": str(len(item.records)),
                "records": json.dumps(
                    [record.model_dump(mode="json") for record in item.records],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            }
            for item in response.sources
        ]
        truncated = len(rows) > resolved.plan.limit
        rows = rows[: resolved.plan.limit]
    else:
        raise ValueError(f"Unknown service route: {route}.")

    return DeterministicAskResult(query=artifact, rows=rows, truncated=truncated)

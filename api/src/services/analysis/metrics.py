from collections import defaultdict
from math import isclose
import re
from typing import Any

from src.schemas.analysis import (
    AnalysisDamageAffectedPoint,
    AnalysisDamageAffectedResponse,
    AnalysisDamageAmount,
    AnalysisDamageHistogramBin,
    AnalysisDamageHistogramResponse,
    AnalysisDisasterCount,
    AnalysisDisasterCountGroupBy,
    AnalysisDisasterCountsResponse,
    AnalysisDisasterRanking,
    AnalysisDisasterRankingsResponse,
    AnalysisRegionRanking,
    AnalysisRegionRankingsResponse,
    AnalysisSummaryResponse,
    AnalysisVictimTrend,
    AnalysisVictimTrendsResponse,
)
from src.services.analysis.common import AnalysisFilters, SPARQL_PREFIXES, event_filter_where, local_name
from src.services.analysis.events import get_all_analysis_events
from src.services.common import ServiceError
from src.services.ontology import get_disaster_taxonomy
from src.services.ontology.utils import binding_value
from src.services.sparql import execute_sparql

_UNIT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9._~-]*$")


async def _execute_or_raise(query: str) -> dict[Any, Any]:
    result = await execute_sparql(query)
    if isinstance(result, str):
        raise ServiceError(502, result)
    return result


def _region_rankings_query(filters: AnalysisFilters) -> str:
    return SPARQL_PREFIXES + f"""
SELECT ?region ?label (COUNT(DISTINCT ?event) AS ?count)
WHERE {{
  {{
    SELECT DISTINCT ?event WHERE {{
      {event_filter_where(filters)}
    }}
  }}
  ?event :hasLocation ?location .
  ?location :isPartOf* ?region .
  ?region a :Region .
  OPTIONAL {{ ?region rdfs:label ?label }}
}}
GROUP BY ?region ?label
ORDER BY DESC(?count) ?label ?region
"""


def _damage_amounts(events: list[Any], unit: str | None = None) -> dict[str, list[float]]:
    amounts: dict[str, list[float]] = defaultdict(list)
    for event in events:
        for damage in event.impact.damageByUnit:
            if unit and damage.unit != unit:
                continue
            amounts[damage.unit].append(float(damage.amount))
    return amounts


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


async def get_summary(filters: AnalysisFilters) -> AnalysisSummaryResponse:
    events = await get_all_analysis_events(filters)
    damage_totals: dict[str, float] = defaultdict(float)
    summary = AnalysisSummaryResponse(record_count=len(events))
    for event in events:
        summary.dead += event.impact.dead
        summary.injured += event.impact.injured
        summary.missing += event.impact.missing
        summary.affectedFamilies += event.impact.affectedFamilies
        summary.affectedPersons += event.impact.affectedPersons
        for damage in event.impact.damageByUnit:
            damage_totals[damage.unit] += float(damage.amount)
    summary.damage = [
        AnalysisDamageAmount(unit=unit, amount=amount)
        for unit, amount in sorted(damage_totals.items())
    ]
    return summary


async def get_disaster_counts(
    filters: AnalysisFilters,
    group_by: AnalysisDisasterCountGroupBy,
) -> AnalysisDisasterCountsResponse:
    events = await get_all_analysis_events(filters)
    taxonomy_groups = await _taxonomy_groups() if group_by == "taxonomy" else {}
    counts: dict[str, AnalysisDisasterCount] = {}

    for event in events:
        seen: set[str] = set()
        for disaster_type in event.disasterTypes:
            item_id, label = disaster_type.id, disaster_type.label
            if group_by == "taxonomy":
                item_id, label = taxonomy_groups.get(item_id, (item_id, label))
            if item_id in seen:
                continue
            seen.add(item_id)
            item = counts.setdefault(
                item_id,
                AnalysisDisasterCount(id=item_id, label=label, count=0),
            )
            item.count += 1

    return AnalysisDisasterCountsResponse(
        group_by=group_by,
        items=sorted(counts.values(), key=lambda item: (-item.count, item.label.casefold())),
    )


async def get_victim_trends(filters: AnalysisFilters) -> AnalysisVictimTrendsResponse:
    events = await get_all_analysis_events(filters)
    trends: dict[int, AnalysisVictimTrend] = {}
    for event in events:
        year_text = event.startDate[:4]
        if not year_text.isdigit():
            continue
        year = int(year_text)
        trend = trends.setdefault(year, AnalysisVictimTrend(year=year))
        trend.dead += event.impact.dead
        trend.injured += event.impact.injured
        trend.missing += event.impact.missing
    return AnalysisVictimTrendsResponse(items=[trends[year] for year in sorted(trends)])


async def get_region_rankings(filters: AnalysisFilters) -> AnalysisRegionRankingsResponse:
    result = await _execute_or_raise(_region_rankings_query(filters))
    items: list[AnalysisRegionRanking] = []
    for binding in result.get("results", {}).get("bindings", []):
        region = binding_value(binding, "region", "")
        if not region:
            continue
        try:
            count = int(binding_value(binding, "count", "0"))
        except ValueError:
            raise ServiceError(502, "GraphDB returned an invalid region count") from None
        region_id = local_name(region)
        items.append(
            AnalysisRegionRanking(
                id=region_id,
                label=binding_value(binding, "label", "") or region_id,
                count=count,
            )
        )
    return AnalysisRegionRankingsResponse(items=items)


async def get_disaster_rankings(filters: AnalysisFilters) -> AnalysisDisasterRankingsResponse:
    events = await get_all_analysis_events(filters)
    rankings: dict[str, AnalysisDisasterRanking] = {}
    for event in events:
        for disaster_type in {item.id: item for item in event.disasterTypes}.values():
            ranking = rankings.setdefault(
                disaster_type.id,
                AnalysisDisasterRanking(
                    id=disaster_type.id,
                    label=disaster_type.label,
                ),
            )
            ranking.dead += event.impact.dead
    return AnalysisDisasterRankingsResponse(
        items=sorted(rankings.values(), key=lambda item: (-item.dead, item.label.casefold()))
    )


async def get_damage_histogram(
    filters: AnalysisFilters,
    *,
    bins: int,
    unit: str | None,
) -> AnalysisDamageHistogramResponse:
    if unit and not _UNIT_RE.fullmatch(unit):
        raise ServiceError(422, "unit must be a local QUDT unit id")
    events = await get_all_analysis_events(filters)
    histogram: list[AnalysisDamageHistogramBin] = []
    for current_unit, values in sorted(_damage_amounts(events, unit).items()):
        low, high = min(values), max(values)
        if isclose(low, high):
            histogram.append(
                AnalysisDamageHistogramBin(
                    unit=current_unit,
                    lowerBound=low,
                    upperBound=high,
                    count=len(values),
                )
            )
            continue

        width = (high - low) / bins
        counts = [0] * bins
        for value in values:
            index = min(int((value - low) / width), bins - 1)
            counts[index] += 1
        for index, count in enumerate(counts):
            histogram.append(
                AnalysisDamageHistogramBin(
                    unit=current_unit,
                    lowerBound=low + (index * width),
                    upperBound=high if index == bins - 1 else low + ((index + 1) * width),
                    count=count,
                )
            )
    return AnalysisDamageHistogramResponse(bins=histogram)


async def get_damage_vs_affected(filters: AnalysisFilters) -> AnalysisDamageAffectedResponse:
    events = await get_all_analysis_events(filters)
    points: list[AnalysisDamageAffectedPoint] = []
    for event in events:
        for damage in event.impact.damageByUnit:
            points.append(
                AnalysisDamageAffectedPoint(
                    event=event.event,
                    eventName=event.eventName,
                    unit=damage.unit,
                    damage=float(damage.amount),
                    affectedFamilies=event.impact.affectedFamilies,
                    affectedPersons=event.impact.affectedPersons,
                )
            )
    return AnalysisDamageAffectedResponse(
        items=sorted(points, key=lambda item: (item.unit, item.damage, item.event))
    )

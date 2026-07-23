import re
from collections.abc import Iterable
from urllib.parse import urlsplit

from src.schemas.ask_execution import QueryArtifact, QueryOrigin
from src.schemas.entity_resolution import ResolvedAskPlan, ResolvedEntity
from src.services.analysis.common import (
    SPARQL_PREFIXES,
    AnalysisFilters,
    event_filter_where,
    make_analysis_filters,
)
from src.services.common import ServiceError


ASK_COMPILATION_ERROR_CODE = "ask_compilation"

_IRI_FORBIDDEN_RE = re.compile(r'[\s<>"{}|^`]')
_EVENT_IMPACT_PROPERTIES = (
    ":hasAgricultureDamage",
    ":hasDamageGeneral",
    ":hasHousingDamage",
    ":hasInfrastructureDamage",
)
_DAMAGE_AMOUNT_PROPERTIES = (
    ":agriDamageAmount",
    ":commercialDamageAmount",
    ":crossSectoralDamageAmount",
    ":generalDamageAmount",
    ":housingDamageAmount",
    ":infraDamageAmount",
    ":socialDamageAmount",
)


def _compilation_error(detail: str) -> ServiceError:
    return ServiceError(422, detail, code=ASK_COMPILATION_ERROR_CODE)


def _validated_iri(iri: str) -> str:
    parsed = urlsplit(iri)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or _IRI_FORBIDDEN_RE.search(iri)
    ):
        raise _compilation_error(f"Resolved entity has an invalid IRI: {iri!r}.")
    return f"<{iri}>"


def _values(variable: str, entities: Iterable[ResolvedEntity]) -> str:
    iris = " ".join(_validated_iri(entity.iri) for entity in entities)
    return f"VALUES ?{variable} {{ {iris} }}"


def analysis_filters_from_plan(resolved: ResolvedAskPlan) -> AnalysisFilters:
    psgc_locations = [
        entity.id
        for entity in resolved.locations
        if re.fullmatch(r"\d{10}", entity.id)
    ]
    wildcard_locations = [
        entity.iri
        for entity in resolved.locations
        if not re.fullmatch(r"\d{10}", entity.id)
    ]
    return make_analysis_filters(
        event_type=resolved.plan.event_type,
        start_date=resolved.plan.start_date,
        end_date=resolved.plan.end_date,
        location_ids=psgc_locations,
        location_iris=wildcard_locations,
        disaster_types=[entity.id for entity in resolved.disaster_types],
    )


def _ensure_compilable(resolved: ResolvedAskPlan) -> None:
    if resolved.ambiguities:
        raise _compilation_error("Ambiguous entities must be clarified before compilation.")
    if resolved.warnings:
        raise _compilation_error(
            "Every requested entity must resolve before deterministic compilation: "
            + " ".join(resolved.warnings)
        )
    for entities in (
        resolved.locations,
        resolved.disaster_types,
        resolved.events,
        resolved.organizations,
        resolved.casualty_types,
    ):
        for entity in entities:
            _validated_iri(entity.iri)


def _base_where(resolved: ResolvedAskPlan) -> str:
    fragments: list[str] = []
    if resolved.events:
        fragments.append(_values("event", resolved.events))
    if resolved.organizations:
        fragments.append(_values("selectedOrganization", resolved.organizations))
    fragments.append(event_filter_where(analysis_filters_from_plan(resolved)))
    if resolved.organizations:
        fragments.append(
            """FILTER EXISTS {
  ?event prov:wasDerivedFrom+/prov:wasAttributedTo ?selectedOrganization .
}"""
        )
    return "\n".join(fragments)


def _group_pattern(group_by: str) -> str:
    patterns = {
        "year": """BIND(SUBSTR(STR(?startDate), 1, 4) AS ?group)
BIND(?group AS ?groupLabel)""",
        "month": """BIND(SUBSTR(STR(?startDate), 1, 7) AS ?group)
BIND(?group AS ?groupLabel)""",
        "location": """?event :hasLocation ?group .
OPTIONAL { ?group rdfs:label ?groupLabel }""",
        "region": """?event :hasLocation ?groupLocation .
?groupLocation :isPartOf* ?group .
?group a :Region .
OPTIONAL { ?group rdfs:label ?groupLabel }""",
        "disaster_type": """?event (:hasDisasterType|:hasDisasterSubtype) ?group .
OPTIONAL { ?group (skos:prefLabel|rdfs:label) ?groupLabel }""",
        "source": """?event prov:wasDerivedFrom+/prov:wasAttributedTo ?group .
OPTIONAL { ?group (skos:prefLabel|rdfs:label) ?groupLabel }""",
    }
    try:
        return patterns[group_by]
    except KeyError:
        raise _compilation_error(f"Unsupported grouping dimension: {group_by!r}.") from None


def _casualty_entity(resolved: ResolvedAskPlan, metric: str) -> ResolvedEntity:
    for entity in resolved.casualty_types:
        if entity.id.casefold() == metric.casefold():
            return entity
    raise _compilation_error(
        f"The controlled casualty entity for metric {metric!r} was not resolved."
    )


def _metric_pattern(
    resolved: ResolvedAskPlan,
    metric: str,
) -> tuple[str, str, bool]:
    if metric in {"dead", "injured", "missing"}:
        casualty = _casualty_entity(resolved, metric)
        return (
            """?event :hasCasualties ?metricRecord .
?metricRecord :isOfCasualtyType """
            + _validated_iri(casualty.iri)
            + """ ;
              :casualtyCount ?rawValue .""",
            "SUM(xsd:decimal(?rawValue))",
            False,
        )
    if metric == "affected_persons":
        return (
            """?event :hasAffectedPopulation ?metricRecord .
?metricRecord :affectedPersons ?rawValue .""",
            "SUM(xsd:decimal(?rawValue))",
            False,
        )
    if metric == "affected_families":
        return (
            """?event :hasAffectedPopulation ?metricRecord .
?metricRecord :affectedFamilies ?rawValue .""",
            "SUM(xsd:decimal(?rawValue))",
            False,
        )
    if metric == "damage":
        impact_properties = " ".join(_EVENT_IMPACT_PROPERTIES)
        damage_properties = " ".join(_DAMAGE_AMOUNT_PROPERTIES)
        return (
            f"""VALUES ?eventImpactProperty {{ {impact_properties} }}
VALUES ?damageProperty {{ {damage_properties} }}
?event ?eventImpactProperty ?metricRecord .
?metricRecord ?damageProperty ?measure .
?measure qudt:numericValue ?rawValue ;
         qudt:unit ?unit .""",
            "SUM(xsd:decimal(?rawValue))",
            True,
        )
    if metric == "events":
        return ("", "COUNT(DISTINCT ?event)", False)
    raise _compilation_error(f"Unsupported metric: {metric!r}.")


def _artifact(
    resolved: ResolvedAskPlan,
    sparql: str,
    columns: list[str],
    *,
    origin: QueryOrigin,
) -> QueryArtifact:
    entities = [
        entity.iri
        for group in (
            resolved.locations,
            resolved.disaster_types,
            resolved.events,
            resolved.organizations,
            resolved.casualty_types,
        )
        for entity in group
    ]
    return QueryArtifact(
        sparql=SPARQL_PREFIXES + sparql.strip() + "\n",
        origin=origin,
        projected_columns=columns,
        expected_columns=columns,
        expected_entities=entities,
        expected_metric=resolved.plan.metric,
        expected_group_by=resolved.plan.group_by,
        warnings=list(resolved.warnings),
    )


def _compile_event_listing(
    resolved: ResolvedAskPlan,
    *,
    origin: QueryOrigin,
) -> QueryArtifact:
    group_by = resolved.plan.group_by
    group_select = ""
    group_pattern = ""
    columns = ["event", "eventName", "eventClass", "startDate", "endDate"]
    if group_by:
        group_select = " ?group ?groupLabel"
        group_pattern = "\n" + _group_pattern(group_by)
        columns.extend(("group", "groupLabel"))
    direction = resolved.plan.sort_direction.upper()
    query = f"""
SELECT DISTINCT ?event ?eventName ?eventClass ?startDate ?endDate{group_select}
WHERE {{
{_base_where(resolved)}{group_pattern}
}}
ORDER BY {direction}(?startDate) ASC(STR(?event))
LIMIT {resolved.plan.limit}
"""
    return _artifact(resolved, query, columns, origin=origin)


def _compile_grouped_aggregate(
    resolved: ResolvedAskPlan,
    metric: str,
    group_by: str | None,
    *,
    origin: QueryOrigin,
) -> QueryArtifact:
    metric_pattern, aggregate, has_unit = _metric_pattern(resolved, metric)
    metric_clause = f"{metric_pattern}\n" if metric_pattern else ""
    if group_by:
        group_pattern = _group_pattern(group_by)
        unit_select = " ?unit" if has_unit else ""
        unit_group = " ?unit" if has_unit else ""
        columns = ["group", "groupLabel", "total"]
        if has_unit:
            columns.append("unit")
        query = f"""
SELECT ?group ?groupLabel ({aggregate} AS ?total){unit_select}
WHERE {{
{metric_clause}{_base_where(resolved)}
{group_pattern}
}}
GROUP BY ?group ?groupLabel{unit_group}
ORDER BY {resolved.plan.sort_direction.upper()}(?total) ASC(STR(?group))
LIMIT {resolved.plan.limit}
"""
        return _artifact(resolved, query, columns, origin=origin)

    unit_select = " ?unit" if has_unit else ""
    unit_group = "GROUP BY ?unit" if has_unit else ""
    unit_order = "ORDER BY ASC(STR(?unit))" if has_unit else ""
    unit_limit = f"LIMIT {resolved.plan.limit}" if has_unit else ""
    columns = ["total", *(("unit",) if has_unit else ())]
    query = f"""
SELECT ({aggregate} AS ?total){unit_select}
WHERE {{
{metric_clause}{_base_where(resolved)}
}}
{unit_group}
{unit_order}
{unit_limit}
"""
    return _artifact(resolved, query, columns, origin=origin)


def _compile_victim_trend(
    resolved: ResolvedAskPlan,
    *,
    origin: QueryOrigin,
) -> QueryArtifact:
    if resolved.plan.metric in {"dead", "injured", "missing"}:
        return _compile_grouped_aggregate(
            resolved,
            resolved.plan.metric,
            "year",
            origin=origin,
        )
    if resolved.plan.metric not in {None, "events"}:
        return _compile_grouped_aggregate(
            resolved,
            resolved.plan.metric,
            "year",
            origin=origin,
        )

    query = f"""
SELECT ?year
       (SUM(IF(?casualtyType = :Dead, xsd:decimal(?rawValue), 0)) AS ?dead)
       (SUM(IF(?casualtyType = :Injured, xsd:decimal(?rawValue), 0)) AS ?injured)
       (SUM(IF(?casualtyType = :Missing, xsd:decimal(?rawValue), 0)) AS ?missing)
WHERE {{
{_base_where(resolved)}
BIND(SUBSTR(STR(?startDate), 1, 4) AS ?year)
?event :hasCasualties ?metricRecord .
?metricRecord :isOfCasualtyType ?casualtyType ;
              :casualtyCount ?rawValue .
VALUES ?casualtyType {{ :Dead :Injured :Missing }}
}}
GROUP BY ?year
ORDER BY ASC(?year)
LIMIT {resolved.plan.limit}
"""
    return _artifact(
        resolved,
        query,
        ["year", "dead", "injured", "missing"],
        origin=origin,
    )


def _compile_full_summary(
    resolved: ResolvedAskPlan,
    *,
    origin: QueryOrigin,
) -> QueryArtifact:
    base = _base_where(resolved)
    impact_properties = " ".join(_EVENT_IMPACT_PROPERTIES)
    damage_properties = " ".join(_DAMAGE_AMOUNT_PROPERTIES)
    branches = [
        f"""{{
  SELECT ("events" AS ?metric) (COUNT(DISTINCT ?event) AS ?total)
  WHERE {{
{base}
  }}
}}""",
    ]
    for metric, casualty in (
        ("dead", ":Dead"),
        ("injured", ":Injured"),
        ("missing", ":Missing"),
    ):
        branches.append(
            f"""{{
  SELECT ("{metric}" AS ?metric) (SUM(xsd:decimal(?rawValue)) AS ?total)
  WHERE {{
{base}
    ?event :hasCasualties ?metricRecord .
    ?metricRecord :isOfCasualtyType {casualty} ;
                  :casualtyCount ?rawValue .
  }}
}}"""
        )
    for metric, property_name in (
        ("affected_families", ":affectedFamilies"),
        ("affected_persons", ":affectedPersons"),
    ):
        branches.append(
            f"""{{
  SELECT ("{metric}" AS ?metric) (SUM(xsd:decimal(?rawValue)) AS ?total)
  WHERE {{
{base}
    ?event :hasAffectedPopulation ?metricRecord .
    ?metricRecord {property_name} ?rawValue .
  }}
}}"""
        )
    branches.append(
        f"""{{
  SELECT ("damage" AS ?metric) (SUM(xsd:decimal(?rawValue)) AS ?total) ?unit
  WHERE {{
{base}
    VALUES ?eventImpactProperty {{ {impact_properties} }}
    VALUES ?damageProperty {{ {damage_properties} }}
    ?event ?eventImpactProperty ?metricRecord .
    ?metricRecord ?damageProperty ?measure .
    ?measure qudt:numericValue ?rawValue ;
             qudt:unit ?unit .
  }}
  GROUP BY ?unit
}}"""
    )
    query = """
SELECT ?metric ?total ?unit
WHERE {
""" + "\nUNION\n".join(branches) + """
}
ORDER BY ?metric ?unit
"""
    return _artifact(
        resolved,
        query,
        ["metric", "total", "unit"],
        origin=origin,
    )


def _compile_event_details(
    resolved: ResolvedAskPlan,
    *,
    origin: QueryOrigin,
) -> QueryArtifact:
    if len(resolved.events) != 1:
        raise _compilation_error("Event details require exactly one resolved event.")
    query = f"""
SELECT ?event ?eventName ?eventClass ?startDate ?endDate ?location ?locationLabel
       ?disasterType ?disasterTypeLabel
WHERE {{
{_values("event", resolved.events)}
?event a ?eventClass .
VALUES ?eventClass {{ :MajorEvent :Incident }}
OPTIONAL {{ ?event :eventName ?eventNameValue }}
OPTIONAL {{ ?event :incidentDescription ?incidentDescription }}
BIND(COALESCE(?eventNameValue, ?incidentDescription) AS ?eventName)
OPTIONAL {{ ?event :startDate ?startDate }}
OPTIONAL {{ ?event :endDate ?endDate }}
OPTIONAL {{
  ?event :hasLocation ?location .
  OPTIONAL {{ ?location rdfs:label ?locationLabel }}
}}
OPTIONAL {{
  ?event (:hasDisasterType|:hasDisasterSubtype) ?disasterType .
  OPTIONAL {{ ?disasterType (skos:prefLabel|rdfs:label) ?disasterTypeLabel }}
}}
}}
ORDER BY ?locationLabel ?disasterTypeLabel
LIMIT {resolved.plan.limit}
"""
    return _artifact(
        resolved,
        query,
        [
            "event",
            "eventName",
            "eventClass",
            "startDate",
            "endDate",
            "location",
            "locationLabel",
            "disasterType",
            "disasterTypeLabel",
        ],
        origin=origin,
    )


def _compile_source_lookup(
    resolved: ResolvedAskPlan,
    *,
    origin: QueryOrigin,
) -> QueryArtifact:
    event_scope = _base_where(resolved)
    query = f"""
SELECT DISTINCT ?event ?eventName ?source ?sourceLabel ?record ?recordLabel
WHERE {{
{event_scope}
?event prov:wasDerivedFrom+ ?record .
OPTIONAL {{ ?event :eventName ?eventName }}
OPTIONAL {{ ?record (rdfs:label|skos:prefLabel) ?recordLabel }}
OPTIONAL {{
  ?record prov:wasAttributedTo ?source .
  OPTIONAL {{ ?source (skos:prefLabel|rdfs:label) ?sourceLabel }}
}}
}}
ORDER BY ?event ?sourceLabel ?recordLabel
LIMIT {resolved.plan.limit}
"""
    return _artifact(
        resolved,
        query,
        ["event", "eventName", "source", "sourceLabel", "record", "recordLabel"],
        origin=origin,
    )


def compile_query(
    resolved: ResolvedAskPlan,
    *,
    origin: QueryOrigin = "compiler",
) -> QueryArtifact:
    """Compile supported Ask plans without incorporating user-authored query text."""
    _ensure_compilable(resolved)
    plan = resolved.plan
    if plan.intent == "open_graph_query":
        raise _compilation_error("Open graph questions require the constrained fallback.")
    if plan.intent == "list_events":
        return _compile_event_listing(resolved, origin=origin)
    if plan.intent == "event_count":
        return _compile_grouped_aggregate(
            resolved,
            "events",
            plan.group_by,
            origin=origin,
        )
    if plan.intent == "impact_summary":
        if plan.metric is None:
            return _compile_full_summary(resolved, origin=origin)
        return _compile_grouped_aggregate(
            resolved,
            plan.metric,
            plan.group_by,
            origin=origin,
        )
    if plan.intent == "victim_trend":
        return _compile_victim_trend(resolved, origin=origin)
    if plan.intent == "region_ranking":
        return _compile_grouped_aggregate(
            resolved,
            plan.metric or "events",
            "region",
            origin=origin,
        )
    if plan.intent == "disaster_ranking":
        return _compile_grouped_aggregate(
            resolved,
            plan.metric or "dead",
            "disaster_type",
            origin=origin,
        )
    if plan.intent == "event_details":
        return _compile_event_details(resolved, origin=origin)
    if plan.intent == "source_lookup":
        return _compile_source_lookup(resolved, origin=origin)
    raise _compilation_error(f"Unsupported Ask intent: {plan.intent!r}.")

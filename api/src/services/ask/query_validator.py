import re
from collections.abc import Iterator
from typing import Any

from pyparsing import ParseResults
from rdflib import URIRef
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parserutils import CompValue

from src.config import settings
from src.schemas.ask_execution import QueryArtifact
from src.schemas.entity_resolution import ResolvedAskPlan
from src.schemas.query_validation import QueryValidationReport, SchemaCatalog
from src.services.ask.schema_catalog import get_schema_catalog
from src.services.common import ServiceError
from src.services.sparql.policy import KNOWN_PREFIXES, analyze_select_query


ASK_QUERY_VALIDATION_ERROR_CODE = "ask_query_validation"
_PROJECT_NAMESPACE = "https://sakuna.ph/"
_LITERAL_OR_COMMENT_RE = re.compile(
    r'"""(?:\\.|(?!""").)*"""'
    r"|'''(?:\\.|(?!''').)*'''"
    r'|"(?:\\.|[^"\\])*"'
    r"|'(?:\\.|[^'\\])*'"
    r"|#[^\r\n]*",
    re.DOTALL,
)
_PREFIXED_NAME_RE = re.compile(
    r"(?<![\w?])(?P<prefix>[A-Za-z][\w.-]*)?:(?P<local>[A-Za-z0-9_][\w.-]*)"
)
_VARIABLE_TYPE_RE = re.compile(
    r"\?(?P<variable>[A-Za-z_][\w-]*)\s+(?:a|rdf:type)\s+:(?P<class>[A-Za-z0-9_][\w.-]*)",
    re.IGNORECASE,
)
_VARIABLE_PROPERTY_RE = re.compile(
    r"\?(?P<subject>[A-Za-z_][\w-]*)\s+:(?P<property>[A-Za-z0-9_][\w.-]*)\s+"
    r"(?P<object>\?[A-Za-z_][\w-]*|:[A-Za-z0-9_][\w.-]*)",
    re.IGNORECASE,
)
_ALLOWED_EXTERNAL_LOCALS: dict[str, set[str]] = {
    "rdf": {"type"},
    "rdfs": {"label", "comment", "domain", "range", "subClassOf", "subPropertyOf"},
    "owl": {
        "Class",
        "ObjectProperty",
        "DatatypeProperty",
        "AnnotationProperty",
        "NamedIndividual",
        "sameAs",
    },
    "skos": {"prefLabel", "altLabel", "definition", "broader"},
    "prov": {"alternateOf", "wasAttributedTo", "wasDerivedFrom", "Organization"},
    "xsd": {"decimal", "integer", "string", "date", "dateTime"},
    "qudt": {"numericValue", "unit"},
    "cur": {"PHP", "USD"},
}
_ALLOWED_EXTERNAL_IRIS = {
    f"{KNOWN_PREFIXES[prefix]}{local}"
    for prefix, locals_ in _ALLOWED_EXTERNAL_LOCALS.items()
    for local in locals_
}


def _validation_error(detail: str) -> ServiceError:
    return ServiceError(422, detail, code=ASK_QUERY_VALIDATION_ERROR_CODE)


def _walk(value: Any) -> Iterator[Any]:
    yield value
    if isinstance(value, CompValue):
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, (ParseResults, list, tuple)):
        for child in value:
            yield from _walk(child)
    elif isinstance(value, dict):
        for child in value.values():
            yield from _walk(child)


def _expected_entity_iris(resolved: ResolvedAskPlan) -> set[str]:
    return {
        entity.iri
        for entities in (
            resolved.locations,
            resolved.disaster_types,
            resolved.events,
            resolved.organizations,
            resolved.casualty_types,
        )
        for entity in entities
    }


def _expected_entity_tokens(resolved: ResolvedAskPlan) -> set[tuple[str, str]]:
    tokens: set[tuple[str, str]] = set()
    for entities in (
        resolved.locations,
        resolved.disaster_types,
        resolved.events,
        resolved.casualty_types,
    ):
        tokens.update(("", entity.id) for entity in entities)
    tokens.update(("org", entity.id) for entity in resolved.organizations)
    return tokens


def _catalog_by_local(catalog: SchemaCatalog) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for iri, term in catalog.terms.items():
        result.setdefault(term.local_name, []).append(iri)
    return result


def _query_vocabulary(
    query: str,
    catalog: SchemaCatalog,
    resolved: ResolvedAskPlan,
) -> list[str]:
    sanitized = _LITERAL_OR_COMMENT_RE.sub("", query)
    catalog_by_local = _catalog_by_local(catalog)
    expected_tokens = _expected_entity_tokens(resolved)
    validated: set[str] = set()

    for match in _PREFIXED_NAME_RE.finditer(sanitized):
        prefix = match.group("prefix") or ""
        local = match.group("local")
        if prefix in {"", "org"}:
            namespace = KNOWN_PREFIXES[prefix]
            iri = f"{namespace}{local}"
            if local not in catalog_by_local and (prefix, local) not in expected_tokens:
                raise _validation_error(f"Unknown SakunaGraPH vocabulary term: {prefix}:{local}.")
            validated.add(iri)
            continue
        allowed = _ALLOWED_EXTERNAL_LOCALS.get(prefix)
        if allowed is None or local not in allowed:
            raise _validation_error(
                f"External vocabulary term is not allowlisted: {prefix}:{local}."
            )
        validated.add(f"{KNOWN_PREFIXES[prefix]}{local}")

    parsed = parseQuery(query)
    expected_iris = _expected_entity_iris(resolved)
    for node in _walk(parsed[1]):
        if not isinstance(node, URIRef):
            continue
        iri = str(node)
        if iri.startswith(_PROJECT_NAMESPACE):
            if iri not in catalog.terms and iri not in expected_iris:
                raise _validation_error(f"Unknown SakunaGraPH IRI: <{iri}>.")
            validated.add(iri)
        elif iri not in _ALLOWED_EXTERNAL_IRIS:
            raise _validation_error(f"External IRI is not allowlisted: <{iri}>.")
        else:
            validated.add(iri)
    return sorted(validated)


def _is_compatible(actual: str, allowed: set[str], catalog: SchemaCatalog) -> bool:
    pending = [actual]
    visited: set[str] = set()
    while pending:
        current = pending.pop()
        if current in allowed:
            return True
        if current in visited:
            continue
        visited.add(current)
        term = catalog.terms.get(current)
        if term:
            pending.extend(term.parents)
    return False


def _validate_known_domains_and_ranges(query: str, catalog: SchemaCatalog) -> None:
    sanitized = _LITERAL_OR_COMMENT_RE.sub("", query)
    by_local = _catalog_by_local(catalog)
    variable_types: dict[str, set[str]] = {}
    for match in _VARIABLE_TYPE_RE.finditer(sanitized):
        class_iris = by_local.get(match.group("class"), [])
        variable_types.setdefault(match.group("variable"), set()).update(class_iris)

    for match in _VARIABLE_PROPERTY_RE.finditer(sanitized):
        property_iris = by_local.get(match.group("property"), [])
        properties = [catalog.terms[iri] for iri in property_iris]
        for property_ in properties:
            subject_types = variable_types.get(match.group("subject"), set())
            if property_.domains and subject_types and not any(
                _is_compatible(actual, set(property_.domains), catalog)
                for actual in subject_types
            ):
                raise _validation_error(
                    f"Property :{property_.local_name} is incompatible with the "
                    f"declared type of ?{match.group('subject')}."
                )

            object_ = match.group("object")
            if object_.startswith("?") and property_.ranges:
                object_types = variable_types.get(object_[1:], set())
                if object_types and not any(
                    _is_compatible(actual, set(property_.ranges), catalog)
                    for actual in object_types
                ):
                    raise _validation_error(
                        f"Property :{property_.local_name} has an incompatible range "
                        f"for {object_}."
                    )


def _entity_occurs(query: str, entity_iri: str, entity_id: str) -> bool:
    if f"<{entity_iri}>" in query:
        return True
    return bool(
        re.search(
            rf"(?<![\w.-])(?:[A-Za-z][\w.-]*)?:{re.escape(entity_id)}(?![\w.-])",
            query,
        )
    )


def _validate_plan_alignment(
    artifact: QueryArtifact,
    resolved: ResolvedAskPlan,
    projected_columns: list[str],
    has_aggregate: bool,
    limit: int | None,
) -> None:
    query = artifact.sparql
    for entities in (
        resolved.locations,
        resolved.disaster_types,
        resolved.events,
        resolved.organizations,
        resolved.casualty_types,
    ):
        for entity in entities:
            if not _entity_occurs(query, entity.iri, entity.id):
                raise _validation_error(
                    f"Compiled query does not contain resolved entity {entity.label!r}."
                )

    for date_value in (resolved.plan.start_date, resolved.plan.end_date):
        if date_value and date_value.isoformat() not in query:
            raise _validation_error(
                f"Compiled query does not contain requested date {date_value.isoformat()}."
            )

    intent = resolved.plan.intent
    aggregate_intents = {
        "event_count",
        "impact_summary",
        "victim_trend",
        "region_ranking",
        "disaster_ranking",
    }
    if intent in aggregate_intents:
        if not has_aggregate:
            raise _validation_error(f"Intent {intent!r} requires an aggregate query.")
    if resolved.plan.group_by and intent in {
        "event_count",
        "impact_summary",
        "victim_trend",
        "region_ranking",
        "disaster_ranking",
    }:
        if "GROUP BY" not in query.upper():
            raise _validation_error(
                f"Grouping by {resolved.plan.group_by!r} is missing from the query."
            )
    if intent in {"region_ranking", "disaster_ranking"} and "ORDER BY" not in query.upper():
        raise _validation_error("Ranking queries require deterministic ORDER BY.")
    if intent == "list_events" and limit is None:
        raise _validation_error("Event listings require a bounded LIMIT.")

    expected_projection = artifact.projected_columns or artifact.expected_columns
    missing = [column for column in expected_projection if column not in projected_columns]
    unexpected = [column for column in projected_columns if column not in expected_projection]
    if expected_projection and (missing or unexpected):
        raise _validation_error(
            "Projected columns do not match the query artifact; "
            f"missing={missing}, unexpected={unexpected}."
        )


async def validate_query_artifact(
    artifact: QueryArtifact,
    resolved: ResolvedAskPlan,
) -> QueryValidationReport:
    try:
        summary = analyze_select_query(
            artifact.sparql,
            max_length=settings.ask_sparql_max_length,
            max_triples=settings.ask_sparql_max_triples,
            max_optionals=settings.ask_sparql_max_optionals,
            max_unions=settings.ask_sparql_max_unions,
            max_subqueries=settings.ask_sparql_max_subqueries,
            row_limit=settings.ask_result_row_limit,
            require_bounded=True,
        )
    except ValueError as exc:
        raise _validation_error(str(exc)) from exc

    catalog = await get_schema_catalog()
    validated_terms = _query_vocabulary(artifact.sparql, catalog, resolved)
    _validate_known_domains_and_ranges(artifact.sparql, catalog)
    _validate_plan_alignment(
        artifact,
        resolved,
        summary.projected_columns,
        summary.has_aggregate,
        summary.limit,
    )
    return QueryValidationReport(summary=summary, validated_terms=validated_terms)

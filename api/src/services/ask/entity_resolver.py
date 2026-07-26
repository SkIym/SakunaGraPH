import asyncio
import re
import time
import unicodedata
from collections.abc import Callable, Iterable
from difflib import SequenceMatcher
from typing import Any

from src.schemas.ask_plan import AskPlan
from src.schemas.entity_resolution import (
    EntityAmbiguity,
    EntityCatalogEntry,
    EntityType,
    MatchType,
    ResolvedAskPlan,
    ResolvedEntity,
)
from src.services.common import ServiceError
from src.services.ontology.utils import binding_value
from src.services.sparql import execute_sparql


_CACHE_TTL_SECONDS = 900.0
_FUZZY_THRESHOLD = 0.86
_FUZZY_AMBIGUITY_MARGIN = 0.035
_catalog_cache: dict[str, tuple[float, list[EntityCatalogEntry]]] = {}

_LOCATION_CATALOG_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?entity ?code ?label ?altLabel ?levelLabel
       ?parent ?parentLabel ?region ?regionLabel
WHERE {
  VALUES (?level ?levelLabel) {
    (:IslandGroup "Island Group")
    (:Region "Region")
    (:Province "Province")
    (:City "City")
    (:Municipality "Municipality")
    (:Barangay "Barangay")
  }
  ?entity a ?level ;
          rdfs:label ?label .
  OPTIONAL { ?entity :psgcCode ?code }
  FILTER(?level = :IslandGroup || BOUND(?code))
  OPTIONAL { ?entity skos:altLabel ?altLabel }
  OPTIONAL {
    ?entity :isPartOf ?parent .
    OPTIONAL { ?parent rdfs:label ?parentLabel }
  }
  OPTIONAL {
    ?entity :isPartOf* ?region .
    ?region a :Region ;
            rdfs:label ?regionLabel .
  }
}
ORDER BY ?code
"""

_DISASTER_CATALOG_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?entity ?label ?altLabel ?definition ?parent ?parentLabel
WHERE {
  ?entity a :DisasterType ;
          skos:prefLabel ?label .
  OPTIONAL { ?entity skos:altLabel ?altLabel }
  OPTIONAL { ?entity skos:definition ?definition }
  OPTIONAL {
    ?entity skos:broader ?parent .
    OPTIONAL { ?parent (skos:prefLabel|rdfs:label) ?parentLabel }
  }
}
ORDER BY ?label
"""

_CASUALTY_CATALOG_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?entity ?label ?altLabel
WHERE {
  VALUES ?entity { :Dead :Injured :Missing }
  ?entity a :CasualtyType .
  OPTIONAL { ?entity (skos:prefLabel|rdfs:label) ?label }
  OPTIONAL { ?entity skos:altLabel ?altLabel }
}
"""

_EVENT_CATALOG_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?entity ?label ?altLabel
WHERE {
  ?entity a :DisasterEvent .
  OPTIONAL { ?entity :eventName ?label }
  OPTIONAL { ?entity (skos:altLabel|rdfs:label) ?altLabel }
}
ORDER BY ?label ?entity
"""

_ORGANIZATION_CATALOG_QUERY = """
PREFIX : <https://sakuna.ph/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX prov: <http://www.w3.org/ns/prov#>

SELECT DISTINCT ?entity ?label ?altLabel
WHERE {
  {
    ?record prov:wasAttributedTo ?entity .
  }
  UNION
  {
    ?entity a prov:Organization .
  }
  OPTIONAL { ?entity (skos:prefLabel|rdfs:label) ?label }
  OPTIONAL { ?entity skos:altLabel ?altLabel }
}
ORDER BY ?label ?entity
"""

_DISASTER_ALIASES: dict[str, tuple[str, ...]] = {
    "typhoon": ("TropicalCyclone",),
    "typhoons": ("TropicalCyclone",),
    "cyclone": ("TropicalCyclone",),
    "cyclones": ("TropicalCyclone",),
    "tropical storm": ("TropicalCyclone",),
    "tropical storms": ("TropicalCyclone",),
    "quake": ("Earthquake",),
    "quakes": ("Earthquake",),
    "landslides": ("Landslide",),
    "floods": ("Flood",),
    "droughts": ("Drought",),
    "wildfires": ("Wildfire",),
    "volcanic eruption": ("VolcanicActivity",),
    "volcanic eruptions": ("VolcanicActivity",),
}

_CASUALTY_ALIASES: dict[str, tuple[str, ...]] = {
    "dead": ("Dead",),
    "death": ("Dead",),
    "deaths": ("Dead",),
    "fatality": ("Dead",),
    "fatalities": ("Dead",),
    "killed": ("Dead",),
    "injured": ("Injured",),
    "injuries": ("Injured",),
    "injury": ("Injured",),
    "wounded": ("Injured",),
    "missing": ("Missing",),
    "unaccounted": ("Missing",),
    "unaccounted for": ("Missing",),
}

_CASUALTY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (
        alias,
        re.compile(rf"(?<!\w){re.escape(alias)}(?!\w)", re.IGNORECASE),
    )
    for alias in sorted(_CASUALTY_ALIASES, key=len, reverse=True)
)


def clear_entity_resolution_caches() -> None:
    """Clear API-owned in-memory catalogs (primarily for tests and operations)."""
    _catalog_cache.clear()


def _normalize(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    ascii_text = "".join(char for char in decomposed if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^\w]+", " ", ascii_text.casefold()).split())


def _local_name(iri: str) -> str:
    return iri.rstrip("/").rsplit("#", 1)[-1].rsplit("/", 1)[-1]


def _humanize_local(value: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", value).replace("_", " ")


def _unique(values: Iterable[str | None]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        cleaned = " ".join(value.split())
        key = cleaned.casefold()
        if cleaned and key not in seen:
            result.append(cleaned)
            seen.add(key)
    return result


def _admin_base(label: str, level: str) -> str | None:
    normalized = " ".join(label.split())
    patterns = (
        rf"^{re.escape(level)}\s+of\s+(.+)$",
        rf"^(.+?)\s+{re.escape(level)}$",
    )
    for pattern in patterns:
        match = re.match(pattern, normalized, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def _location_hierarchy_aliases(
    label: str,
    level: str,
    parent_label: str | None,
    region_label: str | None,
) -> list[str]:
    base = _admin_base(label, level)
    names = [label]
    if base:
        names.append(base)
    aliases: list[str | None] = [
        base,
        f"{label} {level}",
        f"{level} of {label}",
        f"{base} {level}" if base else None,
        f"{level} of {base}" if base else None,
    ]
    for name in names:
        aliases.extend(
            (
                f"{name}, {parent_label}" if parent_label else None,
                f"{name} {parent_label}" if parent_label else None,
                f"{name}, {region_label}" if region_label else None,
                f"{name} {region_label}" if region_label else None,
            )
        )
    return _unique(aliases)


def _rows(result: dict[Any, Any] | str, catalog_name: str) -> list[dict[Any, Any]]:
    if isinstance(result, str):
        raise ServiceError(502, f"Could not load the {catalog_name} catalog: {result}")
    return result.get("results", {}).get("bindings", [])


def _build_location_catalog(bindings: list[dict[Any, Any]]) -> list[EntityCatalogEntry]:
    grouped: dict[str, dict[str, Any]] = {}
    for binding in bindings:
        iri = binding_value(binding, "entity", "")
        code = binding_value(binding, "code", "")
        label = binding_value(binding, "label", "")
        if not iri or not label:
            continue
        identifier = code or _local_name(iri)
        if not identifier:
            continue
        item = grouped.setdefault(
            iri,
            {
                "iri": iri,
                "id": identifier,
                "label": label,
                "entity_type": "location",
                "aliases": [],
                "level": binding_value(binding, "levelLabel") or None,
                "parent_iri": binding_value(binding, "parent") or None,
                "parent_label": binding_value(binding, "parentLabel") or None,
                "region_iri": binding_value(binding, "region") or None,
                "region_label": binding_value(binding, "regionLabel") or None,
            },
        )
        alt_label = binding_value(binding, "altLabel")
        if alt_label:
            item["aliases"].append(alt_label)

    entries: list[EntityCatalogEntry] = []
    for item in grouped.values():
        level = item["level"] or "Location"
        entries.append(
            EntityCatalogEntry(
                **{
                    **item,
                    "aliases": _unique(item["aliases"]),
                    "hierarchy_aliases": _location_hierarchy_aliases(
                        item["label"],
                        level,
                        item["parent_label"],
                        item["region_label"],
                    ),
                }
            )
        )
    return entries


def _plural_aliases(label: str) -> list[str]:
    if not label or label.endswith("s"):
        return []
    if label.endswith("y") and len(label) > 1:
        return [f"{label[:-1]}ies"]
    return [f"{label}s"]


def _build_disaster_catalog(bindings: list[dict[Any, Any]]) -> list[EntityCatalogEntry]:
    grouped: dict[str, dict[str, Any]] = {}
    for binding in bindings:
        iri = binding_value(binding, "entity", "")
        label = binding_value(binding, "label", "")
        if not iri or not label:
            continue
        local = _local_name(iri)
        item = grouped.setdefault(
            iri,
            {
                "iri": iri,
                "id": local,
                "label": label,
                "entity_type": "disaster_type",
                "aliases": [_humanize_local(local), *_plural_aliases(label)],
                "parent_iri": binding_value(binding, "parent") or None,
                "parent_label": binding_value(binding, "parentLabel") or None,
                "definition": binding_value(binding, "definition") or None,
            },
        )
        alt_label = binding_value(binding, "altLabel")
        if alt_label:
            item["aliases"].append(alt_label)

    return [
        EntityCatalogEntry(**{**item, "aliases": _unique(item["aliases"])})
        for item in grouped.values()
    ]


def _build_named_catalog(
    bindings: list[dict[Any, Any]],
    entity_type: EntityType,
) -> list[EntityCatalogEntry]:
    grouped: dict[str, dict[str, Any]] = {}
    for binding in bindings:
        iri = binding_value(binding, "entity", "")
        if not iri:
            continue
        local = _local_name(iri)
        label = binding_value(binding, "label") or _humanize_local(local)
        item = grouped.setdefault(
            iri,
            {
                "iri": iri,
                "id": local,
                "label": label,
                "entity_type": entity_type,
                "aliases": [_humanize_local(local)],
            },
        )
        for key in ("label", "altLabel"):
            value = binding_value(binding, key)
            if value and value != item["label"]:
                item["aliases"].append(value)

    return [
        EntityCatalogEntry(**{**item, "aliases": _unique(item["aliases"])})
        for item in grouped.values()
    ]


async def _get_catalog(
    name: str,
    query: str,
    builder: Callable[[list[dict[Any, Any]]], list[EntityCatalogEntry]],
) -> list[EntityCatalogEntry]:
    cached = _catalog_cache.get(name)
    now = time.monotonic()
    if cached and now - cached[0] <= _CACHE_TTL_SECONDS:
        return cached[1]

    result = await execute_sparql(query)
    entries = builder(_rows(result, name))
    _catalog_cache[name] = (time.monotonic(), entries)
    return entries


async def _location_catalog() -> list[EntityCatalogEntry]:
    return await _get_catalog("PSGC location", _LOCATION_CATALOG_QUERY, _build_location_catalog)


async def _disaster_catalog() -> list[EntityCatalogEntry]:
    return await _get_catalog(
        "disaster taxonomy",
        _DISASTER_CATALOG_QUERY,
        _build_disaster_catalog,
    )


async def _casualty_catalog() -> list[EntityCatalogEntry]:
    return await _get_catalog(
        "casualty type",
        _CASUALTY_CATALOG_QUERY,
        lambda rows: _build_named_catalog(rows, "casualty_type"),
    )


async def _event_catalog() -> list[EntityCatalogEntry]:
    return await _get_catalog(
        "event",
        _EVENT_CATALOG_QUERY,
        lambda rows: _build_named_catalog(rows, "event"),
    )


async def _organization_catalog() -> list[EntityCatalogEntry]:
    return await _get_catalog(
        "organization",
        _ORGANIZATION_CATALOG_QUERY,
        lambda rows: _build_named_catalog(rows, "organization"),
    )


def _resolved(
    entry: EntityCatalogEntry,
    mention: str,
    match_type: MatchType,
    confidence: float,
) -> ResolvedEntity:
    return ResolvedEntity(
        iri=entry.iri,
        id=entry.id,
        label=entry.label,
        entity_type=entry.entity_type,
        mention=mention,
        match_type=match_type,
        confidence=round(confidence, 4),
    )


def _match_values(entry: EntityCatalogEntry) -> tuple[list[str], list[str]]:
    direct = [entry.label, *entry.aliases]
    return (_unique(direct), _unique(entry.hierarchy_aliases))


def _direct_matches(
    mention: str,
    entries: list[EntityCatalogEntry],
    entity_type: EntityType,
) -> list[ResolvedEntity]:
    key = _normalize(mention)
    id_matches = [entry for entry in entries if key == _normalize(entry.id)]
    if id_matches:
        return [_resolved(entry, mention, "exact", 1.0) for entry in id_matches]

    exact: list[tuple[EntityCatalogEntry, MatchType, float]] = []
    for entry in entries:
        if key == _normalize(entry.label):
            exact.append((entry, "exact", 1.0))
        elif any(key == _normalize(alias) for alias in entry.aliases):
            exact.append((entry, "alias", 0.98))
        elif any(key == _normalize(alias) for alias in entry.hierarchy_aliases):
            exact.append((entry, "hierarchy", 0.96))

    if entity_type != "location" and exact:
        best_confidence = max(item[2] for item in exact)
        exact = [item for item in exact if item[2] == best_confidence]
    return [_resolved(entry, mention, kind, score) for entry, kind, score in exact]


def _alias_matches(
    mention: str,
    entries: list[EntityCatalogEntry],
    aliases: dict[str, tuple[str, ...]],
) -> list[ResolvedEntity]:
    target_ids = aliases.get(_normalize(mention), ())
    target_keys = {_normalize(target) for target in target_ids}
    return [
        _resolved(entry, mention, "alias", 0.98)
        for entry in entries
        if _normalize(entry.id) in target_keys or _normalize(entry.label) in target_keys
    ]


def _fuzzy_matches(
    mention: str,
    entries: list[EntityCatalogEntry],
) -> list[ResolvedEntity]:
    key = _normalize(mention)
    if not key:
        return []

    scored: list[tuple[float, EntityCatalogEntry]] = []
    for entry in entries:
        direct, hierarchy = _match_values(entry)
        candidates = [*direct, *hierarchy]
        if key.isdigit():
            candidates.append(entry.id)
        score = max(
            (SequenceMatcher(None, key, _normalize(value)).ratio() for value in candidates),
            default=0.0,
        )
        if score >= _FUZZY_THRESHOLD:
            scored.append((score, entry))

    scored.sort(key=lambda item: (-item[0], item[1].label.casefold(), item[1].iri))
    if not scored:
        return []
    top_score = scored[0][0]
    close = [item for item in scored if top_score - item[0] <= _FUZZY_AMBIGUITY_MARGIN]
    return [_resolved(entry, mention, "fuzzy", score) for score, entry in close[:10]]


def resolve_mentions(
    mentions: list[str],
    entries: list[EntityCatalogEntry],
    entity_type: EntityType,
    *,
    aliases: dict[str, tuple[str, ...]] | None = None,
) -> tuple[list[ResolvedEntity], list[EntityAmbiguity], list[str]]:
    """Resolve mentions only to entries supplied by an approved GraphDB catalog."""
    resolved: list[ResolvedEntity] = []
    ambiguities: list[EntityAmbiguity] = []
    warnings: list[str] = []

    for mention in mentions:
        candidates = _direct_matches(mention, entries, entity_type)
        if not candidates and aliases:
            candidates = _alias_matches(mention, entries, aliases)
        if not candidates:
            candidates = _fuzzy_matches(mention, entries)

        if len(candidates) == 1:
            resolved.append(candidates[0])
        elif len(candidates) > 1:
            ambiguities.append(
                EntityAmbiguity(
                    mention=mention,
                    entity_type=entity_type,
                    reason=(
                        f"{mention!r} matches multiple {entity_type.replace('_', ' ')} "
                        "entities in GraphDB."
                    ),
                    candidates=candidates,
                )
            )
        else:
            warnings.append(
                f"No {entity_type.replace('_', ' ')} entity in GraphDB matched {mention!r}."
            )

    return resolved, ambiguities, warnings


def _casualty_mentions(question: str, plan: AskPlan) -> list[str]:
    by_target: dict[str, str] = {}
    for alias, pattern in _CASUALTY_PATTERNS:
        if match := pattern.search(question):
            target = _CASUALTY_ALIASES[alias][0]
            by_target.setdefault(target, match.group(0))
    if plan.metric in {"dead", "injured", "missing"}:
        target = plan.metric.capitalize()
        by_target.setdefault(target, plan.metric)
    return list(by_target.values())


def _extend_unique(target: list[ResolvedEntity], values: list[ResolvedEntity]) -> None:
    known = {entity.iri for entity in target}
    for value in values:
        if value.iri not in known:
            target.append(value)
            known.add(value.iri)


async def resolve_ask_plan(question: str, plan: AskPlan) -> ResolvedAskPlan:
    casualty_mentions = _casualty_mentions(question, plan)
    catalog_loaders: dict[str, Callable[[], Any]] = {}
    if plan.location_mentions:
        catalog_loaders["locations"] = _location_catalog
    if plan.disaster_type_mentions:
        catalog_loaders["disaster_types"] = _disaster_catalog
    if plan.event_mentions:
        catalog_loaders["events"] = _event_catalog
    if plan.organization_mentions:
        catalog_loaders["organizations"] = _organization_catalog
    if casualty_mentions:
        catalog_loaders["casualty_types"] = _casualty_catalog

    names = list(catalog_loaders)
    loaded = await asyncio.gather(*(catalog_loaders[name]() for name in names))
    catalogs = dict(zip(names, loaded, strict=True))

    resolved_plan = ResolvedAskPlan(plan=plan)
    requests: tuple[
        tuple[str, list[str], EntityType, dict[str, tuple[str, ...]] | None], ...
    ] = (
        ("locations", plan.location_mentions, "location", None),
        (
            "disaster_types",
            plan.disaster_type_mentions,
            "disaster_type",
            _DISASTER_ALIASES,
        ),
        ("events", plan.event_mentions, "event", None),
        ("organizations", plan.organization_mentions, "organization", None),
        ("casualty_types", casualty_mentions, "casualty_type", _CASUALTY_ALIASES),
    )

    for field, mentions, entity_type, aliases in requests:
        if not mentions:
            continue
        matches, ambiguities, warnings = resolve_mentions(
            mentions,
            catalogs[field],
            entity_type,
            aliases=aliases,
        )
        _extend_unique(getattr(resolved_plan, field), matches)
        resolved_plan.ambiguities.extend(ambiguities)
        resolved_plan.warnings.extend(warnings)

    return resolved_plan

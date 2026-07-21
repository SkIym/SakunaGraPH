from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.ask_plan import AskPlan


EntityType = Literal[
    "location",
    "disaster_type",
    "event",
    "organization",
    "source",
    "casualty_type",
]
MatchType = Literal["exact", "alias", "hierarchy", "fuzzy"]


class EntityCatalogEntry(BaseModel):
    """API-owned representation of one entity read from GraphDB."""

    model_config = ConfigDict(extra="forbid")

    iri: str = Field(min_length=1)
    id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    entity_type: EntityType
    aliases: list[str] = Field(default_factory=list)
    hierarchy_aliases: list[str] = Field(default_factory=list)
    level: str | None = None
    parent_iri: str | None = None
    parent_label: str | None = None
    region_iri: str | None = None
    region_label: str | None = None
    definition: str | None = None


class ResolvedEntity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    iri: str = Field(min_length=1)
    id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    entity_type: EntityType
    mention: str = Field(min_length=1)
    match_type: MatchType
    confidence: float = Field(ge=0.0, le=1.0)


class EntityAmbiguity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mention: str = Field(min_length=1)
    entity_type: EntityType
    reason: str = Field(min_length=1)
    candidates: list[ResolvedEntity] = Field(min_length=2, max_length=10)


class ResolvedAskPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plan: AskPlan
    locations: list[ResolvedEntity] = Field(default_factory=list)
    disaster_types: list[ResolvedEntity] = Field(default_factory=list)
    events: list[ResolvedEntity] = Field(default_factory=list)
    organizations: list[ResolvedEntity] = Field(default_factory=list)
    casualty_types: list[ResolvedEntity] = Field(default_factory=list)
    ambiguities: list[EntityAmbiguity] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

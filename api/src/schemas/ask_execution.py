from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


QueryOrigin = Literal["service", "compiler", "model_fallback"]
AskServiceRoute = Literal[
    "analysis_events",
    "analysis_event_count",
    "analysis_summary",
    "analysis_victim_trends",
    "analysis_region_rankings",
    "analysis_disaster_rankings",
    "event_details",
    "event_sources",
]


class QueryArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sparql: str
    origin: QueryOrigin
    service_route: AskServiceRoute | None = None
    expected_columns: list[str] = Field(default_factory=list)
    expected_entities: list[str] = Field(default_factory=list)
    expected_metric: str | None = None
    expected_group_by: str | None = None
    warnings: list[str] = Field(default_factory=list)


class DeterministicAskResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: QueryArtifact
    rows: list[dict[str, str]] = Field(default_factory=list)

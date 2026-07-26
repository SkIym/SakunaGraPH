import re
from datetime import date
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


AskIntent = Literal[
    "list_events",
    "list_disaster_types",
    "event_count",
    "impact_summary",
    "victim_trend",
    "region_ranking",
    "disaster_ranking",
    "event_details",
    "source_lookup",
    "open_graph_query",
]
EventType = Literal["major", "incidents", "all"]
AskMetric = Literal[
    "events",
    "dead",
    "injured",
    "missing",
    "affected_persons",
    "affected_families",
    "damage",
]
AskGroupBy = Literal[
    "year",
    "month",
    "region",
    "location",
    "disaster_type",
    "source",
]
SortDirection = Literal["asc", "desc"]
Mention = Annotated[str, Field(min_length=1, max_length=200)]

_SPARQL_TEXT_RE = re.compile(
    r"(?is)\b(?:SELECT|ASK|CONSTRUCT|DESCRIBE|INSERT|DELETE|PREFIX)\b[^{}]*\{"
)


class AskPlan(BaseModel):
    """Validated, query-language-free interpretation of an Ask question."""

    model_config = ConfigDict(extra="forbid")

    intent: AskIntent
    event_type: EventType = "all"
    location_mentions: list[Mention] = Field(default_factory=list, max_length=20)
    disaster_type_mentions: list[Mention] = Field(default_factory=list, max_length=20)
    event_mentions: list[Mention] = Field(default_factory=list, max_length=20)
    organization_mentions: list[Mention] = Field(default_factory=list, max_length=20)
    start_date: date | None = None
    end_date: date | None = None
    metric: AskMetric | None = None
    group_by: AskGroupBy | None = None
    sort_direction: SortDirection = "desc"
    limit: int = Field(default=25, ge=1, le=100)

    @field_validator(
        "location_mentions",
        "disaster_type_mentions",
        "event_mentions",
        "organization_mentions",
    )
    @classmethod
    def normalize_mentions(cls, mentions: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for mention in mentions:
            value = mention.strip()
            if not value:
                raise ValueError("Entity mentions cannot be blank.")
            if _SPARQL_TEXT_RE.search(value):
                raise ValueError("Entity mentions cannot contain SPARQL query text.")
            key = value.casefold()
            if key not in seen:
                normalized.append(value)
                seen.add(key)
        return normalized

    @model_validator(mode="after")
    def validate_date_order(self) -> "AskPlan":
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date.")
        return self

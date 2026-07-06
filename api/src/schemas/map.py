from typing import Literal

from pydantic import BaseModel, Field

EventMode = Literal["major", "incidents"]
EventScope = Literal["region", "province"]
EventType = Literal["MajorEvent", "Incident"]


class MapEvent(BaseModel):
    event: str
    eventName: str
    startDate: str
    locations: list[str] = Field(default_factory=list)
    disasterTypes: list[str] = Field(default_factory=list)
    alternates: list[str] = Field(default_factory=list)
    source: str | None = None


class MapEventsResponse(BaseModel):
    events: list[MapEvent]
    majorCount: int
    incidentCount: int

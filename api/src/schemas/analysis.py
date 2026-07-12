from typing import Literal

from pydantic import BaseModel, Field

from src.schemas.ontology import PsgcGraphResponse, TaxonomyNode


class AnalysisFilterOptionsResponse(BaseModel):
    locations: PsgcGraphResponse
    disasterTypes: TaxonomyNode


AnalysisEventType = Literal["major", "incidents", "all"]
AnalysisEventClass = Literal["MajorEvent", "Incident"]
AnalysisEventSortBy = Literal[
    "startDate",
    "endDate",
    "eventName",
    "eventType",
    "source",
]
AnalysisSortDirection = Literal["asc", "desc"]


class AnalysisEventFacet(BaseModel):
    id: str
    label: str


class AnalysisDamageAmount(BaseModel):
    amount: float
    unit: str


class AnalysisEventImpact(BaseModel):
    dead: int = 0
    injured: int = 0
    missing: int = 0
    affectedFamilies: int = 0
    affectedPersons: int = 0
    damageAmount: float | None = 0
    damageUnit: str | None = None
    damageByUnit: list[AnalysisDamageAmount] = Field(default_factory=list)


class AnalysisEvent(BaseModel):
    event: str
    eventName: str
    eventType: AnalysisEventClass
    startDate: str
    endDate: str | None = None
    locations: list[AnalysisEventFacet] = Field(default_factory=list)
    disasterTypes: list[AnalysisEventFacet] = Field(default_factory=list)
    source: str | None = None
    alternates: list[str] = Field(default_factory=list)
    impact: AnalysisEventImpact = Field(default_factory=AnalysisEventImpact)


class AnalysisEventsResponse(BaseModel):
    items: list[AnalysisEvent]
    page: int
    page_size: int
    total: int
    sort_by: AnalysisEventSortBy
    sort_dir: AnalysisSortDirection

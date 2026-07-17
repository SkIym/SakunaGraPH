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
AnalysisDisasterCountGroupBy = Literal["type", "taxonomy"]
AnalysisTimelineBucket = Literal["month_year", "month_of_year"]


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


class AnalysisSummaryResponse(BaseModel):
    record_count: int = 0
    dead: int = 0
    injured: int = 0
    missing: int = 0
    affectedFamilies: int = 0
    affectedPersons: int = 0
    damage: list[AnalysisDamageAmount] = Field(default_factory=list)


class AnalysisDisasterCount(BaseModel):
    id: str
    label: str
    count: int


class AnalysisDisasterCountsResponse(BaseModel):
    group_by: AnalysisDisasterCountGroupBy
    items: list[AnalysisDisasterCount] = Field(default_factory=list)


class AnalysisVictimTrend(BaseModel):
    year: int
    dead: int = 0
    injured: int = 0
    missing: int = 0


class AnalysisVictimTrendsResponse(BaseModel):
    items: list[AnalysisVictimTrend] = Field(default_factory=list)


class AnalysisRegionRanking(BaseModel):
    id: str
    label: str
    count: int


class AnalysisRegionRankingsResponse(BaseModel):
    items: list[AnalysisRegionRanking] = Field(default_factory=list)


class AnalysisDisasterRanking(BaseModel):
    id: str
    label: str
    dead: int = 0


class AnalysisDisasterRankingsResponse(BaseModel):
    items: list[AnalysisDisasterRanking] = Field(default_factory=list)


class AnalysisDamageHistogramBin(BaseModel):
    unit: str
    lowerBound: float
    upperBound: float
    count: int


class AnalysisDamageHistogramResponse(BaseModel):
    bins: list[AnalysisDamageHistogramBin] = Field(default_factory=list)


class AnalysisDamageAffectedPoint(BaseModel):
    event: str
    eventName: str
    unit: str
    damage: float
    affectedFamilies: int = 0
    affectedPersons: int = 0


class AnalysisDamageAffectedResponse(BaseModel):
    items: list[AnalysisDamageAffectedPoint] = Field(default_factory=list)


class AnalysisCalendarItem(BaseModel):
    period: str
    count: int
    dead: int | None = None
    injured: int | None = None
    missing: int | None = None


class AnalysisCalendarResponse(BaseModel):
    items: list[AnalysisCalendarItem] = Field(default_factory=list)


class AnalysisTimelineCategoryStack(BaseModel):
    period: str
    categories: list[AnalysisDisasterCount] = Field(default_factory=list)


class AnalysisTimelineCategoryStacksResponse(BaseModel):
    bucket: AnalysisTimelineBucket
    items: list[AnalysisTimelineCategoryStack] = Field(default_factory=list)


class AnalysisTimelineDateEventsResponse(BaseModel):
    date_prefix: str
    items: list[AnalysisEvent] = Field(default_factory=list)

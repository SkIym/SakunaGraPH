from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class IriLabel(BaseModel):
    uri: str
    label: str


class EventReference(BaseModel):
    uri: str
    name: str


class EventDetailLocation(BaseModel):
    uri: str
    id: str
    label: str


class EventDetailDisasterType(BaseModel):
    uri: str
    id: str
    label: str


class EventDetailRelatedEvent(BaseModel):
    uri: str
    name: str
    eventType: Literal["MajorEvent", "Incident"] | None = None
    startDate: str | None = None
    endDate: str | None = None


class EventDetailSource(BaseModel):
    uri: str
    reportName: str
    reportLink: str | None = None
    obtainedDate: str | None = None
    lastUpdateDate: str | None = None
    format: str | None = None
    attributedTo: list[IriLabel] = Field(default_factory=list)


class EventDetailsResponse(BaseModel):
    event: str
    name: str
    eventType: Literal["MajorEvent", "Incident"]
    startDate: str | None = None
    endDate: str | None = None
    remarks: list[str] = Field(default_factory=list)
    locations: list[EventDetailLocation] = Field(default_factory=list)
    disasterTypes: list[EventDetailDisasterType] = Field(default_factory=list)
    majorEvents: list[EventDetailRelatedEvent] = Field(default_factory=list)
    incidents: list[EventDetailRelatedEvent] = Field(default_factory=list)
    alternates: list[EventDetailRelatedEvent] = Field(default_factory=list)
    sources: list[EventDetailSource] = Field(default_factory=list)


class ImpactClass(BaseModel):
    uri: str
    id: str
    label: str
    definition: str


class ImpactValue(BaseModel):
    predicate: str
    label: str
    value: str
    valueType: str | None = None
    datatype: str | None = None
    lang: str | None = None
    unit: str | None = None


class ImpactItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    uri: str
    label: str
    class_: str = Field(alias="class")
    classLabel: str
    linkedFrom: EventReference
    linkProperty: IriLabel
    locations: list[IriLabel]
    values: list[ImpactValue]


class EventImpactResponse(BaseModel):
    event: str
    impact: ImpactClass
    items: list[ImpactItem]


class HolderReference(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    uri: str
    class_: str = Field(alias="class")
    classLabel: str


class OrganizationRole(BaseModel):
    property: IriLabel
    holder: HolderReference
    relatedEvent: EventReference


class DisasterOrganization(BaseModel):
    uri: str
    label: str
    roles: list[OrganizationRole]


class DisasterOrganizationsResponse(BaseModel):
    event: str
    organizations: list[DisasterOrganization]


class SourceRecord(BaseModel):
    uri: str
    label: str
    relatedEvent: EventReference


class DisasterSource(BaseModel):
    uri: str
    label: str
    records: list[SourceRecord]


class DisasterSourcesResponse(BaseModel):
    event: str
    sources: list[DisasterSource]

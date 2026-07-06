from pydantic import BaseModel, ConfigDict, Field


class IriLabel(BaseModel):
    uri: str
    label: str


class EventReference(BaseModel):
    uri: str
    name: str


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

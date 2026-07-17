from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field


class TaxonomyNode(BaseModel):
    id: str
    label: str
    group: str
    definition: str
    children: list[TaxonomyNode] | None = None


class OntologyDataProperty(BaseModel):
    label: str
    range: str


class OntologyNode(BaseModel):
    id: str
    label: str
    group: str
    definition: str
    dataProperties: list[OntologyDataProperty] | None = None


class OntologyLink(BaseModel):
    source: str
    target: str
    type: Literal["subClassOf", "objectProperty"]
    label: str


class OntologyGraphResponse(BaseModel):
    nodes: list[OntologyNode]
    links: list[OntologyLink]


class PsgcRegion(BaseModel):
    id: str
    label: str
    fullName: str
    level: Literal["Region"] = "Region"
    island: str
    population: int
    psgcCode: str


class PsgcProvince(BaseModel):
    id: str
    label: str
    level: Literal["Province"] = "Province"
    island: str
    incomeClass: str | None = None
    population: int
    psgcCode: str
    regionId: str


class PsgcCityMunicipality(BaseModel):
    id: str
    label: str
    level: Literal["City", "Municipality"]
    island: str
    population: int
    psgcCode: str
    cityType: str | None = None
    incomeClass: str | None = None
    regionId: str
    regionLabel: str
    parentId: str | None = None
    parentLabel: str | None = None
    note: str | None = None


class PsgcBarangay(BaseModel):
    id: str
    label: str
    level: Literal["Barangay"] = "Barangay"
    population: int
    psgcCode: str
    parentId: str | None = None
    parentLabel: str | None = None


PsgcGraphNode = Annotated[
    PsgcRegion | PsgcProvince | PsgcCityMunicipality,
    Field(discriminator="level"),
]


class PsgcLink(BaseModel):
    source: str
    target: str


class PsgcGraphResponse(BaseModel):
    nodes: list[PsgcGraphNode]
    links: list[PsgcLink]


class PsgcRegionsResponse(BaseModel):
    regions: list[PsgcRegion]


class PsgcProvincesResponse(BaseModel):
    provinces: list[PsgcProvince]


class PsgcCitiesMunicipalitiesResponse(BaseModel):
    citiesMunicipalities: list[PsgcCityMunicipality]


class PsgcBarangaysResponse(BaseModel):
    barangays: list[PsgcBarangay]

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


SchemaTermKind = Literal[
    "class",
    "object_property",
    "datatype_property",
    "annotation_property",
    "individual",
]


class SchemaTerm(BaseModel):
    model_config = ConfigDict(extra="forbid")

    iri: str
    local_name: str
    label: str
    kind: SchemaTermKind
    domains: list[str] = Field(default_factory=list)
    ranges: list[str] = Field(default_factory=list)
    parents: list[str] = Field(default_factory=list)
    types: list[str] = Field(default_factory=list)


class SchemaCatalog(BaseModel):
    model_config = ConfigDict(extra="forbid")

    terms: dict[str, SchemaTerm] = Field(default_factory=dict)
    loaded_at_monotonic: float


class ParsedQuerySummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query_form: Literal["SELECT"] = "SELECT"
    projected_columns: list[str] = Field(default_factory=list)
    component_names: list[str] = Field(default_factory=list)
    triple_pattern_count: int = 0
    optional_count: int = 0
    union_count: int = 0
    subquery_count: int = 0
    has_aggregate: bool = False
    limit: int | None = None


class QueryValidationReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: ParsedQuerySummary
    validated_terms: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ResultValidationReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    row_count: int = 0
    truncated: bool = False
    warnings: list[str] = Field(default_factory=list)

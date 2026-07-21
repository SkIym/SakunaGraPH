from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.ask_execution import AskServiceRoute, QueryArtifact, QueryOrigin
from src.schemas.entity_resolution import ResolvedAskPlan


EvidenceKind = Literal["result_set", "result", "aggregate", "event", "source"]


class AskResultTerm(BaseModel):
    """One validated RDF result term with its display and quantity metadata."""

    model_config = ConfigDict(extra="forbid")

    value: str
    display: str
    term_type: str = "literal"
    datatype: str | None = None
    language: str | None = None
    unit: str | None = None


class AskAnswerRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: int = Field(ge=0)
    values: dict[str, AskResultTerm] = Field(default_factory=dict)
    evidence_ids: list[str] = Field(default_factory=list)


class AskProvenance(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query_origin: QueryOrigin
    query_hash: str = Field(min_length=64, max_length=64)
    service_route: AskServiceRoute | None = None
    source_iris: list[str] = Field(default_factory=list)
    source_labels: list[str] = Field(default_factory=list)
    source_record_iris: list[str] = Field(default_factory=list)
    report_links: list[str] = Field(default_factory=list)


class AskEvidence(BaseModel):
    """A returned result row and the provenance needed to trace its claims."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=r"^E[1-9][0-9]*$")
    kind: EvidenceKind
    label: str = Field(min_length=1)
    row_index: int | None = Field(default=None, ge=0)
    uri: str | None = None
    values: dict[str, str] = Field(default_factory=dict)
    unit: str | None = None
    provenance: AskProvenance


class AskAnswerContext(BaseModel):
    """Compact, validated payload used to produce a grounded Ask answer."""

    model_config = ConfigDict(extra="forbid")

    question: str = Field(min_length=1)
    interpretation: ResolvedAskPlan
    query: QueryArtifact
    query_status: Literal["validated"] = "validated"
    columns: list[str] = Field(default_factory=list)
    rows: list[AskAnswerRow] = Field(default_factory=list)
    row_count: int = Field(default=0, ge=0)
    truncated: bool = False
    approximate: bool = False
    warnings: list[str] = Field(default_factory=list)
    evidence: list[AskEvidence] = Field(default_factory=list)

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SparqlRequest(BaseModel):
    query: str


class SparqlTerm(BaseModel):
    model_config = ConfigDict(extra="allow")

    value: Any
    type: str
    datatype: str | None = None


SparqlBinding = dict[str, SparqlTerm]


class SparqlHead(BaseModel):
    model_config = ConfigDict(extra="allow")

    vars: list[str] = Field(default_factory=list)


class SparqlResults(BaseModel):
    model_config = ConfigDict(extra="allow")

    bindings: list[SparqlBinding] = Field(default_factory=list)


class SparqlQueryResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    head: SparqlHead | dict[str, Any] | None = None
    results: SparqlResults | dict[str, Any] | None = None
    boolean: bool | None = None

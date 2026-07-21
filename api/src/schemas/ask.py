from enum import StrEnum

from pydantic import BaseModel, Field, field_validator

from src.schemas.entity_resolution import EntityAmbiguity, ResolvedAskPlan


ASK_QUERY_MAX_LENGTH = 2_000


class AskStatus(StrEnum):
    QUERY_READY = "query_ready"
    ANSWERED = "answered"
    NO_DATA = "no_data"
    GENERATION_FAILED = "generation_failed"
    VALIDATION_FAILED = "validation_failed"
    EXECUTION_FAILED = "execution_failed"
    NEEDS_DISAMBIGUATION = "needs_disambiguation"


class AskRequest(BaseModel):
    query: str = Field(min_length=1, max_length=ASK_QUERY_MAX_LENGTH)

    @field_validator("query")
    @classmethod
    def validate_query_text(cls, value: str) -> str:
        query = value.strip()
        if not query:
            raise ValueError("Query must contain non-whitespace characters.")
        return query


class AskResponse(BaseModel):
    status: AskStatus = AskStatus.ANSWERED
    sparql: str
    answer: str
    rows: list[dict[str, str]]
    interpretation: ResolvedAskPlan | None = None
    warnings: list[str] | None = None
    ambiguities: list[EntityAmbiguity] | None = None


class AskPreviewResponse(BaseModel):
    status: AskStatus = AskStatus.QUERY_READY
    sparql: str
    interpretation: ResolvedAskPlan | None = None
    warnings: list[str] | None = None
    ambiguities: list[EntityAmbiguity] | None = None


class AskErrorResponse(BaseModel):
    status: AskStatus
    detail: str

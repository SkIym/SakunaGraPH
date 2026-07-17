from pydantic import BaseModel


class AskRequest(BaseModel):
    query: str


class AskResponse(BaseModel):
    sparql: str
    answer: str
    rows: list[dict[str, str]]


class AskPreviewResponse(BaseModel):
    sparql: str

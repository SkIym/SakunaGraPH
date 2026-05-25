from typing import Any

from pydantic import BaseModel


class AskRequest(BaseModel):
    query: str


class AskResponse(BaseModel):
    sparql: str
    answer: str
    bindings: list[dict[Any, Any]]

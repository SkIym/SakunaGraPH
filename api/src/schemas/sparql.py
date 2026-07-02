from pydantic import BaseModel


class SparqlRequest(BaseModel):
    query: str

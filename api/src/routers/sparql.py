from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.services.common import ServiceError
from src.services.sparql import run_sparql_query

router = APIRouter(prefix="/sparql", tags=["sparql"])


class SparqlRequest(BaseModel):
    query: str


@router.post("", response_model=None)
async def post_sparql(request: SparqlRequest) -> dict[Any, Any] | JSONResponse:
    try:
        return await run_sparql_query(request.query)
    except ServiceError as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

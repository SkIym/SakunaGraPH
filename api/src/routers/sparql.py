from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from src.schemas.sparql import SparqlRequest
from src.services.common import ServiceError
from src.services.sparql import run_sparql_query

router = APIRouter(prefix="/sparql", tags=["sparql"])


@router.post("", response_model=None)
async def post_sparql(request: SparqlRequest) -> dict[Any, Any] | JSONResponse:
    try:
        return await run_sparql_query(request.query)
    except ServiceError as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

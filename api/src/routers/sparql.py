from fastapi import APIRouter
from fastapi.responses import JSONResponse

from src.schemas.sparql import SparqlQueryResponse, SparqlRequest
from src.services.common import ServiceError
from src.services.sparql import run_sparql_query

router = APIRouter(prefix="/sparql", tags=["sparql"])


@router.post("", response_model=SparqlQueryResponse, response_model_exclude_none=True)
async def post_sparql(request: SparqlRequest) -> SparqlQueryResponse | JSONResponse:
    try:
        return await run_sparql_query(request.query)
    except ServiceError as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

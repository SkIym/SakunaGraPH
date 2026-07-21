import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from src.schemas.ask import (
    AskErrorResponse,
    AskPreviewResponse,
    AskRequest,
    AskResponse,
    AskStatus,
)
from src.services.common import ServiceError
from src.services.ask import (
    ASK_COMPILATION_ERROR_CODE,
    ASK_DETERMINISTIC_EXECUTION_ERROR_CODE,
    PLANNER_VALIDATION_ERROR_CODE,
    ask_question,
    preview_question,
    stream_answer_events,
)
from src.services.llm import active_model_info, list_models_async
from src.services.sparql import SparqlCorrectionError, is_graphdb_error

router = APIRouter(tags=["ask"])


def _to_http_error(exc: ServiceError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


def _ask_failure_status(exc: ServiceError) -> AskStatus:
    if exc.code in {PLANNER_VALIDATION_ERROR_CODE, ASK_COMPILATION_ERROR_CODE}:
        return AskStatus.VALIDATION_FAILED
    if exc.code == ASK_DETERMINISTIC_EXECUTION_ERROR_CODE:
        return AskStatus.EXECUTION_FAILED
    if isinstance(exc, SparqlCorrectionError):
        if is_graphdb_error(exc.reason):
            return AskStatus.EXECUTION_FAILED
        return AskStatus.VALIDATION_FAILED
    return AskStatus.GENERATION_FAILED


def _ask_error_response(exc: ServiceError) -> JSONResponse:
    payload = AskErrorResponse(
        status=_ask_failure_status(exc),
        detail=exc.detail,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=payload.model_dump(mode="json"),
    )


async def _stream_with_errors(query: str) -> AsyncIterator[str]:
    try:
        async for event in stream_answer_events(query):
            yield event
    except ServiceError as exc:
        yield f"data: {json.dumps({'type': 'error', 'status': exc.status_code, 'ask_status': _ask_failure_status(exc), 'detail': exc.detail})}\n\n"


@router.get("/ask/model")
async def ask_model() -> dict[str, str]:
    try:
        return active_model_info()
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.get("/ask/models")
async def ask_models() -> dict[str, Any]:
    try:
        return {
            "active": active_model_info(),
            "models": await list_models_async(),
        }
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.post("/ask", response_model=AskResponse, response_model_exclude_none=True)
async def ask(request: AskRequest) -> AskResponse | JSONResponse:
    try:
        return await ask_question(request.query)
    except ServiceError as exc:
        return _ask_error_response(exc)


@router.post(
    "/ask/preview",
    response_model=AskPreviewResponse,
    response_model_exclude_none=True,
)
async def ask_preview(request: AskRequest) -> AskPreviewResponse | JSONResponse:
    try:
        return await preview_question(request.query)
    except ServiceError as exc:
        return _ask_error_response(exc)


@router.post("/ask/stream")
async def ask_stream(request: AskRequest) -> StreamingResponse:
    return StreamingResponse(
        _stream_with_errors(request.query),
        media_type="text/event-stream",
    )

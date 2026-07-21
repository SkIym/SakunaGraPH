import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from src.schemas.ask import AskPreviewResponse, AskRequest, AskResponse
from src.services.common import ServiceError
from src.services.ask import ask_question, preview_question, stream_answer_events
from src.services.llm import active_model_info, list_models_async

router = APIRouter(tags=["ask"])


def _to_http_error(exc: ServiceError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


async def _stream_with_errors(query: str) -> AsyncIterator[str]:
    try:
        async for event in stream_answer_events(query):
            yield event
    except ServiceError as exc:
        yield f"data: {json.dumps({'type': 'error', 'status': exc.status_code, 'detail': exc.detail})}\n\n"


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


@router.post("/ask", response_model=AskResponse)
async def ask(request: AskRequest) -> AskResponse:
    try:
        return await ask_question(request.query)
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.post("/ask/preview", response_model=AskPreviewResponse)
async def ask_preview(request: AskRequest) -> AskPreviewResponse:
    try:
        return await preview_question(request.query)
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.post("/ask/stream")
async def ask_stream(request: AskRequest) -> StreamingResponse:
    return StreamingResponse(
        _stream_with_errors(request.query),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
        },
    )

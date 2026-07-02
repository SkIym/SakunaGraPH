from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from src.schemas.ask import AskPreviewResponse, AskRequest, AskResponse
from src.services.common import ServiceError
from src.services.ask import ask_question, preview_question, stream_answer_events

router = APIRouter(tags=["ask"])


def _to_http_error(exc: ServiceError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.post("/ask", response_model=AskResponse)
async def ask(request: AskRequest) -> AskResponse:
    try:
        return AskResponse(**await ask_question(request.query))
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.post("/ask/preview", response_model=AskPreviewResponse)
async def ask_preview(request: AskRequest) -> AskPreviewResponse:
    try:
        return AskPreviewResponse(**await preview_question(request.query))
    except ServiceError as exc:
        raise _to_http_error(exc) from exc


@router.post("/ask/stream")
async def ask_stream(request: AskRequest) -> StreamingResponse:
    return StreamingResponse(
        stream_answer_events(request.query),
        media_type="text/event-stream",
    )

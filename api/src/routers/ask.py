import json

import google as genai
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from src.config import settings
from src.schemas.ask import AskRequest, AskResponse
from src.services.answer_service import build_grounding_prompt, ground_answer
from src.services.ontology_context import load_ontology_context
from src.services.sparql_service import sparql_with_correction

router = APIRouter(tags=["ask"])

_ontology_context = load_ontology_context()


@router.post("/ask", response_model=AskResponse)
async def ask(request: AskRequest) -> AskResponse:
    sparql, raw_results = await sparql_with_correction(request.query, _ontology_context)
    bindings = raw_results.get("results", {}).get("bindings", [])
    answer = await ground_answer(request.query, raw_results)
    return AskResponse(sparql=sparql, answer=answer, bindings=bindings)


@router.post("/ask/stream")
async def ask_stream(request: AskRequest) -> StreamingResponse:
    async def event_generator():
        sparql, raw_results = await sparql_with_correction(request.query, _ontology_context)
        bindings = raw_results.get("results", {}).get("bindings", [])

        yield f"data: {json.dumps({'type': 'meta', 'sparql': sparql, 'bindings': bindings})}\n\n"

        prompt = build_grounding_prompt(request.query, raw_results)
        model = genai.GenerativeModel(settings.gemini_model) # type: ignore
        response = await model.generate_content_async(prompt, stream=True)
        async for chunk in response:
            if chunk.text:
                yield f"data: {json.dumps({'type': 'token', 'text': chunk.text})}\n\n"

        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")

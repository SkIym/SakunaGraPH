import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

from src.config import settings
from src.services.common import ServiceError


def _base_url() -> str:
    return settings.local_llm_base_url.rstrip("/")


def _endpoint_url(path: str) -> str:
    path = path.strip()
    if path.startswith(("http://", "https://")):
        return path
    return f"{_base_url()}/{path.lstrip('/')}"


def _chat_url() -> str:
    return _endpoint_url(settings.local_llm_chat_path)


def _model_name() -> str:
    model = settings.local_llm_model.strip()
    if not model:
        raise ServiceError(503, "LOCAL_LLM_MODEL is not configured.")
    return model


def _timeout() -> httpx.Timeout:
    return httpx.Timeout(settings.local_llm_timeout)


def _request_body(prompt: str, *, stream: bool) -> dict[str, Any]:
    return {
        "model": _model_name(),
        "input": prompt,
        "stream": stream,
        "store": settings.local_llm_store,
    }


def _api_error_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except json.JSONDecodeError:
        detail = response.text[:500]
    else:
        detail = payload.get("error") or payload.get("message") or str(payload)
    return f"Local LLM API returned {response.status_code}: {detail}"


def _raise_for_api_error(response: httpx.Response) -> None:
    if response.status_code >= 400:
        raise ServiceError(502, _api_error_message(response))


async def _raise_for_stream_api_error(response: httpx.Response) -> None:
    if response.status_code < 400:
        return

    content = await response.aread()
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        detail = content.decode(errors="replace")[:500]
    else:
        detail = payload.get("error") or payload.get("message") or str(payload)
    raise ServiceError(502, f"Local LLM API returned {response.status_code}: {detail}")


def _output_message_text(item: dict[str, Any]) -> str:
    if item.get("type") != "message":
        return ""
    content = item.get("content")
    return content if isinstance(content, str) else ""


def _extract_text(payload: dict[str, Any]) -> str:
    output = payload.get("output")
    if isinstance(output, list):
        text = "".join(
            _output_message_text(item)
            for item in output
            if isinstance(item, dict)
        )
        if text:
            return text

    raise ServiceError(502, f"Local LLM response contained no message output: {payload}")


def _connection_error(exc: httpx.HTTPError) -> ServiceError:
    return ServiceError(
        502,
        (
            f"Local LLM request failed: {exc}. "
            f"Check that the local model server is running at {_base_url()} "
            f"and that model '{_model_name()}' is installed."
        ),
    )


def generate_text(prompt: str) -> str:
    try:
        with httpx.Client(timeout=_timeout()) as client:
            response = client.post(
                _chat_url(),
                json=_request_body(prompt, stream=False),
            )
    except httpx.HTTPError as exc:
        raise _connection_error(exc) from exc

    _raise_for_api_error(response)
    return _extract_text(response.json())


async def generate_text_async(prompt: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=_timeout()) as client:
            response = await client.post(
                _chat_url(),
                json=_request_body(prompt, stream=False),
            )
    except httpx.HTTPError as exc:
        raise _connection_error(exc) from exc

    _raise_for_api_error(response)
    return _extract_text(response.json())


async def stream_text_async(prompt: str) -> AsyncIterator[str]:
    try:
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream(
                "POST",
                _chat_url(),
                json=_request_body(prompt, stream=True),
            ) as response:
                await _raise_for_stream_api_error(response)

                async for line in response.aiter_lines():
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith("data:"):
                        line = line.removeprefix("data:").strip()
                    if line == "[DONE]":
                        break

                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise ServiceError(502, "Local LLM stream returned invalid JSON.") from exc

                    if error := payload.get("error"):
                        raise ServiceError(502, f"Local LLM stream error: {error}")

                    output = payload.get("output")
                    text = ""
                    if isinstance(output, list):
                        text = "".join(
                            _output_message_text(item)
                            for item in output
                            if isinstance(item, dict)
                        )
                    if text:
                        yield text

                    if payload.get("done") or payload.get("type") == "done":
                        break
    except httpx.HTTPError as exc:
        raise _connection_error(exc) from exc


async def list_models_async() -> list[dict[str, Any]]:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{_base_url()}/api/tags")
    except httpx.HTTPError as exc:
        raise _connection_error(exc) from exc

    _raise_for_api_error(response)
    payload = response.json()
    models = payload.get("models", [])
    if not isinstance(models, list):
        raise ServiceError(502, f"Local LLM models response was invalid: {payload}")
    return models


def active_model_info() -> dict[str, str]:
    return {
        "provider": "local",
        "baseUrl": _base_url(),
        "chatUrl": _chat_url(),
        "model": _model_name(),
    }

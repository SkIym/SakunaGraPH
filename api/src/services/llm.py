import asyncio
import logging
import time
from collections.abc import AsyncIterator, Iterator, Mapping
from functools import lru_cache
from typing import Any
from weakref import WeakKeyDictionary

import boto3
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectTimeoutError,
    CredentialRetrievalError,
    NoCredentialsError,
    NoRegionError,
    PartialCredentialsError,
    ReadTimeoutError,
    UnknownCredentialError,
)

from src.config import settings
from src.services.common import ServiceError

logger = logging.getLogger(__name__)

_CONFIGURATION_ERROR_CODES = {
    "AccessDeniedException",
    "InvalidSignatureException",
    "ResourceNotFoundException",
    "UnrecognizedClientException",
    "ValidationException",
}
_THROTTLING_ERROR_CODES = {
    "ModelNotReadyException",
    "ServiceQuotaExceededException",
    "ServiceUnavailableException",
    "ThrottlingException",
}
_TIMEOUT_ERROR_CODES = {"ModelTimeoutException", "RequestTimeout", "RequestTimeoutException"}
_STREAM_ERROR_KEYS = {
    "accessDeniedException": "AccessDeniedException",
    "internalServerException": "InternalServerException",
    "modelStreamErrorException": "ModelStreamErrorException",
    "modelTimeoutException": "ModelTimeoutException",
    "resourceNotFoundException": "ResourceNotFoundException",
    "serviceUnavailableException": "ServiceUnavailableException",
    "throttlingException": "ThrottlingException",
    "validationException": "ValidationException",
}

_loop_limiters: WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    tuple[int, asyncio.Semaphore],
] = WeakKeyDictionary()


def _region() -> str:
    region = settings.aws_region.strip()
    if not region:
        raise ServiceError(503, "AWS_REGION is not configured for Amazon Bedrock.")
    return region


def _model_id() -> str:
    model_id = settings.bedrock_model_id.strip()
    if not model_id:
        raise ServiceError(503, "BEDROCK_MODEL_ID is not configured.")
    return model_id


def _endpoint_url() -> str | None:
    endpoint_url = settings.bedrock_endpoint_url
    if endpoint_url is None:
        return None
    return endpoint_url.strip().rstrip("/") or None


@lru_cache(maxsize=8)
def _cached_client(
    region: str,
    endpoint_url: str | None,
    connect_timeout: float,
    read_timeout: float,
    max_attempts: int,
    max_concurrency: int,
) -> Any:
    client_options: dict[str, Any] = {
        "region_name": region,
        "config": Config(
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            max_pool_connections=max_concurrency,
            retries={"mode": "standard", "total_max_attempts": max_attempts},
        ),
    }
    if endpoint_url:
        client_options["endpoint_url"] = endpoint_url
    return boto3.client("bedrock-runtime", **client_options)


def _client() -> Any:
    return _cached_client(
        _region(),
        _endpoint_url(),
        settings.bedrock_connect_timeout,
        settings.bedrock_read_timeout,
        settings.bedrock_max_attempts,
        settings.bedrock_max_concurrency,
    )


def _limiter() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    limit = settings.bedrock_max_concurrency
    configured = _loop_limiters.get(loop)
    if configured is None or configured[0] != limit:
        configured = (limit, asyncio.Semaphore(limit))
        _loop_limiters[loop] = configured
    return configured[1]


def _request(prompt: str) -> dict[str, Any]:
    return {
        "modelId": _model_id(),
        "messages": [
            {
                "role": "user",
                "content": [{"text": prompt}],
            }
        ],
        "inferenceConfig": {
            "maxTokens": settings.bedrock_max_tokens,
            "temperature": settings.bedrock_temperature,
            "topP": settings.bedrock_top_p,
        },
    }


def _response_metadata(response: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = response.get("ResponseMetadata")
    return metadata if isinstance(metadata, Mapping) else {}


def _request_id(response: Mapping[str, Any]) -> str | None:
    request_id = _response_metadata(response).get("RequestId")
    return request_id if isinstance(request_id, str) else None


def _retry_count(response: Mapping[str, Any]) -> int:
    retry_count = _response_metadata(response).get("RetryAttempts", 0)
    return retry_count if isinstance(retry_count, int) else 0


def _log_success(
    operation: str,
    response: Mapping[str, Any],
    started_at: float,
    *,
    usage: Mapping[str, Any] | None = None,
    metrics: Mapping[str, Any] | None = None,
) -> None:
    logger.info(
        "Amazon Bedrock request completed",
        extra={
            "bedrock_operation": operation,
            "bedrock_model_id": _model_id(),
            "aws_region": _region(),
            "aws_request_id": _request_id(response),
            "latency_ms": round((time.monotonic() - started_at) * 1_000, 2),
            "bedrock_reported_latency_ms": (metrics or {}).get("latencyMs"),
            "bedrock_input_tokens": (usage or {}).get("inputTokens"),
            "bedrock_output_tokens": (usage or {}).get("outputTokens"),
            "bedrock_retry_count": _retry_count(response),
        },
    )


def _error_code(exc: ClientError) -> str:
    code = exc.response.get("Error", {}).get("Code", "ClientError")
    return code if isinstance(code, str) else "ClientError"


def _error_request_id(exc: ClientError) -> str | None:
    request_id = exc.response.get("ResponseMetadata", {}).get("RequestId")
    return request_id if isinstance(request_id, str) else None


def _bedrock_error(exc: Exception, operation: str) -> ServiceError:
    if isinstance(
        exc,
        (
            CredentialRetrievalError,
            NoCredentialsError,
            NoRegionError,
            PartialCredentialsError,
            UnknownCredentialError,
        ),
    ):
        status_code = 503
        code = type(exc).__name__
        detail = (
            "Amazon Bedrock credentials or Region are unavailable. "
            "Configure the AWS SDK credential chain and AWS_REGION."
        )
        request_id = None
    elif isinstance(exc, (ConnectTimeoutError, ReadTimeoutError)):
        status_code = 504
        code = type(exc).__name__
        detail = f"Amazon Bedrock timed out ({code})."
        request_id = None
    elif isinstance(exc, ClientError):
        code = _error_code(exc)
        request_id = _error_request_id(exc)
        if code in _CONFIGURATION_ERROR_CODES:
            status_code = 503
            detail = f"Amazon Bedrock is not configured for model '{_model_id()}' ({code})."
        elif code in _THROTTLING_ERROR_CODES:
            status_code = 503
            detail = f"Amazon Bedrock is temporarily unavailable ({code})."
        elif code in _TIMEOUT_ERROR_CODES:
            status_code = 504
            detail = f"Amazon Bedrock timed out ({code})."
        else:
            status_code = 502
            detail = f"Amazon Bedrock request failed ({code})."
    else:
        status_code = 502
        code = type(exc).__name__
        detail = f"Amazon Bedrock request failed ({code})."
        request_id = None

    logger.warning(
        "Amazon Bedrock request failed",
        extra={
            "bedrock_operation": operation,
            "bedrock_model_id": settings.bedrock_model_id.strip() or None,
            "aws_region": settings.aws_region.strip() or None,
            "aws_request_id": request_id,
            "bedrock_error_class": code,
        },
    )
    return ServiceError(status_code, detail)


def _extract_text(response: Mapping[str, Any]) -> str:
    output = response.get("output")
    if not isinstance(output, Mapping):
        raise ServiceError(502, "Amazon Bedrock response did not contain an output object.")

    message = output.get("message")
    if not isinstance(message, Mapping):
        raise ServiceError(502, "Amazon Bedrock response did not contain a message.")

    content = message.get("content")
    if not isinstance(content, list):
        raise ServiceError(502, "Amazon Bedrock response message content was invalid.")

    parts = [block.get("text") for block in content if isinstance(block, Mapping)]
    text = "".join(part for part in parts if isinstance(part, str))
    if not text:
        raise ServiceError(502, "Amazon Bedrock response contained no text output.")
    return text


def _invoke(prompt: str) -> str:
    started_at = time.monotonic()
    try:
        response = _client().converse(**_request(prompt))
    except (BotoCoreError, ClientError) as exc:
        raise _bedrock_error(exc, "Converse") from exc

    if not isinstance(response, Mapping):
        raise ServiceError(502, "Amazon Bedrock returned an invalid response.")
    text = _extract_text(response)
    usage = response.get("usage")
    metrics = response.get("metrics")
    _log_success(
        "Converse",
        response,
        started_at,
        usage=usage if isinstance(usage, Mapping) else None,
        metrics=metrics if isinstance(metrics, Mapping) else None,
    )
    return text


def generate_text(prompt: str) -> str:
    return _invoke(prompt)


async def generate_text_async(prompt: str) -> str:
    async with _limiter():
        return await asyncio.to_thread(_invoke, prompt)


def _next_event(events: Iterator[dict[str, Any]]) -> dict[str, Any] | None:
    try:
        return next(events)
    except StopIteration:
        return None


def _stream_error(event: Mapping[str, Any]) -> ServiceError | None:
    for key, code in _STREAM_ERROR_KEYS.items():
        if key not in event:
            continue
        if code in _CONFIGURATION_ERROR_CODES:
            return ServiceError(503, f"Amazon Bedrock stream is not configured ({code}).")
        if code in _THROTTLING_ERROR_CODES:
            return ServiceError(503, f"Amazon Bedrock stream is temporarily unavailable ({code}).")
        if code in _TIMEOUT_ERROR_CODES:
            return ServiceError(504, f"Amazon Bedrock stream timed out ({code}).")
        return ServiceError(502, f"Amazon Bedrock stream failed ({code}).")
    return None


def _stream_delta(event: Mapping[str, Any]) -> str:
    content_delta = event.get("contentBlockDelta")
    if not isinstance(content_delta, Mapping):
        return ""
    delta = content_delta.get("delta")
    if not isinstance(delta, Mapping):
        raise ServiceError(502, "Amazon Bedrock stream returned an invalid content delta.")
    text = delta.get("text")
    return text if isinstance(text, str) else ""


async def _close_stream(stream: Any) -> None:
    close = getattr(stream, "close", None)
    if callable(close):
        await asyncio.to_thread(close)


async def stream_text_async(prompt: str) -> AsyncIterator[str]:
    if not settings.bedrock_streaming:
        yield await generate_text_async(prompt)
        return

    async with _limiter():
        started_at = time.monotonic()
        try:
            response = await asyncio.to_thread(
                lambda: _client().converse_stream(**_request(prompt))
            )
        except (BotoCoreError, ClientError) as exc:
            raise _bedrock_error(exc, "ConverseStream") from exc

        if not isinstance(response, Mapping):
            raise ServiceError(502, "Amazon Bedrock returned an invalid stream response.")
        stream = response.get("stream")
        if stream is None:
            raise ServiceError(502, "Amazon Bedrock response did not contain a stream.")

        events = iter(stream)
        received_text = False
        usage: Mapping[str, Any] | None = None
        metrics: Mapping[str, Any] | None = None
        try:
            while True:
                try:
                    event = await asyncio.to_thread(_next_event, events)
                except (BotoCoreError, ClientError) as exc:
                    raise _bedrock_error(exc, "ConverseStream") from exc
                if event is None:
                    break
                if not isinstance(event, Mapping):
                    raise ServiceError(502, "Amazon Bedrock stream returned an invalid event.")
                if stream_error := _stream_error(event):
                    raise stream_error

                text = _stream_delta(event)
                if text:
                    received_text = True
                    yield text

                metadata = event.get("metadata")
                if isinstance(metadata, Mapping):
                    event_usage = metadata.get("usage")
                    event_metrics = metadata.get("metrics")
                    usage = event_usage if isinstance(event_usage, Mapping) else usage
                    metrics = event_metrics if isinstance(event_metrics, Mapping) else metrics
        finally:
            await _close_stream(stream)

        if not received_text:
            raise ServiceError(502, "Amazon Bedrock stream contained no text output.")
        _log_success(
            "ConverseStream",
            response,
            started_at,
            usage=usage,
            metrics=metrics,
        )


async def list_models_async() -> list[dict[str, str]]:
    model = _model_id()
    return [
        {
            "name": model,
            "model": model,
            "provider": "amazon-bedrock",
            "region": _region(),
        }
    ]


def active_model_info() -> dict[str, str]:
    info = {
        "provider": "amazon-bedrock",
        "region": _region(),
        "model": _model_id(),
        "api": "Converse",
        "streamApi": "ConverseStream" if settings.bedrock_streaming else "Converse",
    }
    if endpoint_url := _endpoint_url():
        info["endpointUrl"] = endpoint_url
    return info

import unittest
from typing import Any
from unittest.mock import patch

from botocore.exceptions import ClientError, NoCredentialsError, ReadTimeoutError

from src.config import settings
from src.services.common import ServiceError
from src.services.llm import (
    _cached_client,
    _client,
    active_model_info,
    generate_text,
    generate_text_async,
    list_models_async,
    stream_text_async,
)


def _response(text: str = "Bedrock answer") -> dict[str, Any]:
    return {
        "output": {
            "message": {
                "role": "assistant",
                "content": [{"text": text}],
            }
        },
        "usage": {"inputTokens": 4, "outputTokens": 2, "totalTokens": 6},
        "metrics": {"latencyMs": 25},
        "ResponseMetadata": {"RequestId": "request-1", "RetryAttempts": 0},
    }


class _Client:
    def __init__(
        self,
        *,
        response: dict[str, Any] | None = None,
        stream_response: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.response = response or _response()
        self.stream_response = stream_response
        self.error = error
        self.converse_request: dict[str, Any] | None = None
        self.stream_request: dict[str, Any] | None = None

    def converse(self, **request: Any) -> dict[str, Any]:
        self.converse_request = request
        if self.error:
            raise self.error
        return self.response

    def converse_stream(self, **request: Any) -> dict[str, Any]:
        self.stream_request = request
        if self.error:
            raise self.error
        if self.stream_response is None:
            raise AssertionError("A stream response was not configured")
        return self.stream_response


class _Stream:
    def __init__(self, events: list[dict[str, Any]]) -> None:
        self.events = events
        self.closed = False

    def __iter__(self):
        return iter(self.events)

    def close(self) -> None:
        self.closed = True


class BedrockLlmTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.settings = [
            patch.object(settings, "aws_region", "ap-southeast-1"),
            patch.object(settings, "bedrock_model_id", "test.model-v1:0"),
            patch.object(settings, "bedrock_streaming", True),
        ]
        for setting in self.settings:
            setting.start()

    def tearDown(self) -> None:
        _cached_client.cache_clear()
        for setting in reversed(self.settings):
            setting.stop()

    def test_client_uses_regional_runtime_endpoint_and_bounded_retries(self) -> None:
        sentinel = object()
        _cached_client.cache_clear()

        with patch("src.services.llm.boto3.client", return_value=sentinel) as factory:
            client = _client()

        self.assertIs(client, sentinel)
        factory.assert_called_once()
        service_name = factory.call_args.args[0]
        options = factory.call_args.kwargs
        self.assertEqual(service_name, "bedrock-runtime")
        self.assertEqual(options["region_name"], "ap-southeast-1")
        self.assertEqual(options["config"].connect_timeout, 10.0)
        self.assertEqual(options["config"].read_timeout, 120.0)
        self.assertEqual(options["config"].retries["mode"], "standard")
        self.assertEqual(options["config"].retries["total_max_attempts"], 3)

    async def test_async_generation_uses_bedrock_converse_contract(self) -> None:
        client = _Client(response=_response("Part one. Part two."))

        with patch("src.services.llm._client", return_value=client):
            answer = await generate_text_async("Use validated evidence only.")

        self.assertEqual(answer, "Part one. Part two.")
        self.assertEqual(client.converse_request["modelId"], "test.model-v1:0")
        self.assertEqual(
            client.converse_request["messages"],
            [{"role": "user", "content": [{"text": "Use validated evidence only."}]}],
        )
        self.assertEqual(client.converse_request["inferenceConfig"]["temperature"], 0.0)

    async def test_stream_generation_yields_text_deltas_and_closes_body(self) -> None:
        stream = _Stream(
            [
                {"messageStart": {"role": "assistant"}},
                {"contentBlockDelta": {"delta": {"text": "Graph"}}},
                {"contentBlockDelta": {"delta": {"text": " answer"}}},
                {
                    "metadata": {
                        "usage": {"inputTokens": 2, "outputTokens": 2},
                        "metrics": {"latencyMs": 12},
                    }
                },
            ]
        )
        client = _Client(
            stream_response={
                "stream": stream,
                "ResponseMetadata": {"RequestId": "request-2", "RetryAttempts": 1},
            }
        )

        with patch("src.services.llm._client", return_value=client):
            chunks = [chunk async for chunk in stream_text_async("Question")]

        self.assertEqual(chunks, ["Graph", " answer"])
        self.assertTrue(stream.closed)
        self.assertEqual(client.stream_request["modelId"], "test.model-v1:0")

    async def test_stream_error_is_mapped_and_body_is_closed(self) -> None:
        stream = _Stream([{"throttlingException": {"message": "do not expose"}}])
        client = _Client(stream_response={"stream": stream})

        with patch("src.services.llm._client", return_value=client):
            with self.assertRaises(ServiceError) as raised:
                _ = [chunk async for chunk in stream_text_async("Question")]

        self.assertEqual(raised.exception.status_code, 503)
        self.assertNotIn("do not expose", raised.exception.detail)
        self.assertTrue(stream.closed)

    async def test_non_streaming_fallback_uses_converse(self) -> None:
        client = _Client(response=_response("Single response"))

        with (
            patch.object(settings, "bedrock_streaming", False),
            patch("src.services.llm._client", return_value=client),
        ):
            chunks = [chunk async for chunk in stream_text_async("Question")]

        self.assertEqual(chunks, ["Single response"])
        self.assertIsNotNone(client.converse_request)

    def test_missing_credentials_fail_closed(self) -> None:
        client = _Client(error=NoCredentialsError())

        with patch("src.services.llm._client", return_value=client):
            with self.assertRaises(ServiceError) as raised:
                generate_text("Question")

        self.assertEqual(raised.exception.status_code, 503)
        self.assertIn("credential", raised.exception.detail.lower())

    def test_throttling_error_becomes_temporary_service_failure(self) -> None:
        error = ClientError(
            {
                "Error": {"Code": "ThrottlingException", "Message": "secret detail"},
                "ResponseMetadata": {"RequestId": "request-3"},
            },
            "Converse",
        )
        client = _Client(error=error)

        with patch("src.services.llm._client", return_value=client):
            with self.assertRaises(ServiceError) as raised:
                generate_text("Question")

        self.assertEqual(raised.exception.status_code, 503)
        self.assertNotIn("secret detail", raised.exception.detail)

    def test_sdk_read_timeout_becomes_gateway_timeout(self) -> None:
        client = _Client(error=ReadTimeoutError(endpoint_url="https://bedrock.example"))

        with patch("src.services.llm._client", return_value=client):
            with self.assertRaises(ServiceError) as raised:
                generate_text("Question")

        self.assertEqual(raised.exception.status_code, 504)

    async def test_model_metadata_exposes_only_the_pinned_model(self) -> None:
        self.assertEqual(
            active_model_info(),
            {
                "provider": "amazon-bedrock",
                "region": "ap-southeast-1",
                "model": "test.model-v1:0",
                "api": "Converse",
                "streamApi": "ConverseStream",
            },
        )
        self.assertEqual(
            await list_models_async(),
            [
                {
                    "name": "test.model-v1:0",
                    "model": "test.model-v1:0",
                    "provider": "amazon-bedrock",
                    "region": "ap-southeast-1",
                }
            ],
        )

    def test_invalid_response_is_rejected(self) -> None:
        client = _Client(response={"output": {"message": {"content": []}}})

        with patch("src.services.llm._client", return_value=client):
            with self.assertRaises(ServiceError) as raised:
                generate_text("Question")

        self.assertEqual(raised.exception.status_code, 502)
        self.assertIn("no text", raised.exception.detail.lower())


if __name__ == "__main__":
    unittest.main()

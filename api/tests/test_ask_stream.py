import json
import unittest
from collections.abc import AsyncIterator
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.main import app


async def _stream_events(query: str) -> AsyncIterator[str]:
    yield f"data: {json.dumps({'type': 'meta', 'sparql': '', 'rows': [], 'requestId': query})}\n\n"
    yield f"data: {json.dumps({'type': 'token', 'text': 'Answer'})}\n\n"
    yield f"data: {json.dumps({'type': 'done'})}\n\n"


class AskStreamRouterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def test_stream_is_sse_and_disables_proxy_buffering(self) -> None:
        with patch("src.routers.ask.stream_answer_events", new=_stream_events):
            response = self.client.post("/api/ask/stream", json={"query": "request-1"})

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers["content-type"].startswith("text/event-stream"))
        self.assertEqual(response.headers["cache-control"], "no-cache, no-transform")
        self.assertEqual(response.headers["x-accel-buffering"], "no")
        self.assertIn('"type": "meta"', response.text)
        self.assertIn('"type": "token"', response.text)
        self.assertIn('"type": "done"', response.text)


if __name__ == "__main__":
    unittest.main()

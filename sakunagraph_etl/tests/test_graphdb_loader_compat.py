from pathlib import Path
import tempfile
import unittest

import requests

from sakunagraph_etl.io import graphdb


class FakeResponse:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.text = "invalid RDF" if fail else ""

    def raise_for_status(self) -> None:
        if self.fail:
            error = requests.HTTPError("request failed")
            error.response = self
            raise error


class PutSession:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[dict[str, object]] = []

    def put(self, url: str, **kwargs):
        self.calls.append({
            "url": url,
            "graph": kwargs["params"]["graph"],
            "content_type": kwargs["headers"]["Content-Type"],
            "timeout": kwargs["timeout"],
            "payload": kwargs["data"].read(),
        })
        return FakeResponse(fail=self.fail)


class GraphDbLoaderCompatibilityTests(unittest.TestCase):
    def test_nested_event_scope_selects_only_that_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            ontology = root / "ontology"
            ndrrmc = root / "rdf" / "events" / "ndrrmc"
            dromic = root / "rdf" / "events" / "dromic"
            ontology.mkdir()
            ndrrmc.mkdir(parents=True)
            dromic.mkdir(parents=True)
            expected = ndrrmc / "ndrrmc-1.ttl"
            expected.write_text("", encoding="utf-8")
            (dromic / "dromic-2026.ttl").write_text("", encoding="utf-8")

            selected = graphdb.discover_scope(
                "events/ndrrmc",
                root / "rdf",
                ontology,
            )

        self.assertEqual(selected, [expected])

    def test_nested_scope_rejects_parent_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaisesRegex(graphdb.LoaderError, "relative RDF subtree"):
                graphdb.discover_scope("../events", root, root / "ontology")

    def test_cli_accepts_nested_scope_and_legacy_timeout(self) -> None:
        args = graphdb.parse_args(["--scope", "events/ndrrmc"])

        self.assertEqual(args.scope, ["events/ndrrmc"])
        self.assertEqual(args.timeout, 3_600)

    def test_graph_store_replacement_bundles_files_in_one_put(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = root / "first.ttl"
            second = root / "second.ttl"
            first.write_bytes(b"<urn:first> <urn:p> <urn:o> .")
            second.write_bytes(b"<urn:second> <urn:p> <urn:o> .")
            context = "https://sakuna.ph/events/ndrrmc"
            session = PutSession()

            graphdb.replace_context(
                session,
                graphdb.graph_store_url("http://graphdb:7200", "sakuna graph"),
                context,
                [
                    graphdb.LoadTarget(first, context),
                    graphdb.LoadTarget(second, context),
                ],
                3_600,
            )

        self.assertEqual(len(session.calls), 1)
        call = session.calls[0]
        self.assertEqual(
            call["url"],
            "http://graphdb:7200/repositories/sakuna%20graph/rdf-graphs/service",
        )
        self.assertEqual(call["graph"], context)
        self.assertEqual(call["content_type"], "text/turtle")
        self.assertEqual(call["timeout"], 3_600)
        self.assertIn(b"<urn:first>", call["payload"])
        self.assertIn(b"<urn:second>", call["payload"])

    def test_failed_graph_store_put_is_reported_as_replacement_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "event.ttl"
            path.write_bytes(b"<urn:event> <urn:p> <urn:o> .")
            context = "https://sakuna.ph/events/ndrrmc"

            with self.assertRaisesRegex(
                graphdb.LoaderError,
                "Could not replace context",
            ):
                graphdb.replace_context(
                    PutSession(fail=True),
                    "http://graphdb/repositories/test/rdf-graphs/service",
                    context,
                    [graphdb.LoadTarget(path, context)],
                    30,
                )


if __name__ == "__main__":
    unittest.main()

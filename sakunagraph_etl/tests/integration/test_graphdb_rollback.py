import os
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

import requests
from rdflib import Graph, Literal, URIRef

from sakunagraph_etl.io.graphdb import (
    LoadTarget,
    LoaderError,
    clear_context,
    graphdb_url,
    load_target,
    replace_context,
    transactions_url,
    validate_repository,
)


HOST = os.getenv("SAKUNA_TEST_GRAPHDB_HOST")
REPOSITORY = os.getenv("SAKUNA_TEST_GRAPHDB_REPOSITORY")


@unittest.skipUnless(
    HOST and REPOSITORY,
    "set SAKUNA_TEST_GRAPHDB_HOST and SAKUNA_TEST_GRAPHDB_REPOSITORY",
)
class GraphDbRollbackIntegrationTests(unittest.TestCase):
    """Exercise rollback against an isolated, disposable named graph."""

    def test_invalid_replacement_keeps_previous_named_graph(self) -> None:
        assert HOST is not None
        assert REPOSITORY is not None
        context = f"https://sakuna.ph/tests/rollback/{uuid4()}"
        subject = URIRef(f"{context}/sentinel")
        predicate = URIRef("https://sakuna.ph/testValue")
        session = requests.Session()
        username = os.getenv("GRAPHDB_USERNAME")
        if username:
            session.auth = (username, os.getenv("GRAPHDB_PASSWORD", ""))

        endpoint = graphdb_url(HOST, REPOSITORY)
        try:
            validate_repository(session, HOST, REPOSITORY)
            with tempfile.TemporaryDirectory() as temp:
                initial_path = Path(temp) / "initial.ttl"
                initial_graph = Graph()
                initial_graph.add((subject, predicate, Literal("active")))
                initial_graph.serialize(initial_path, format="turtle")
                load_target(session, endpoint, LoadTarget(initial_path, context), 30)

                invalid_path = Path(temp) / "invalid.ttl"
                invalid_path.write_text("<broken Turtle", encoding="utf-8")
                with self.assertRaises(LoaderError):
                    replace_context(
                        session,
                        transactions_url(HOST, REPOSITORY),
                        context,
                        [LoadTarget(invalid_path, context)],
                        30,
                    )

            response = session.get(
                endpoint,
                params={"context": f"<{context}>"},
                headers={"Accept": "text/turtle"},
                timeout=30,
            )
            response.raise_for_status()
            active_graph = Graph().parse(data=response.text, format="turtle")
            self.assertIn((subject, predicate, Literal("active")), active_graph)
        finally:
            try:
                clear_context(session, endpoint, context, 30)
            finally:
                session.close()


if __name__ == "__main__":
    unittest.main()

from types import SimpleNamespace
import unittest

from rdflib import Graph

from sakunagraph_etl.rdf.publication import (
    GraphDbPublisher,
    GraphPublisher,
    PublicationMode,
    PublicationResult,
)
from sakunagraph_etl.rdf.validation import (
    ShaclValidationService,
    ValidationService,
)


class FakeValidator:
    def validate_graph(self, graph, **kwargs):
        return SimpleNamespace(
            conforms=True,
            label=kwargs["label"],
            results_text="Conforms",
            data_triples=len(graph),
            validation_triples=len(graph),
        )


class FakePublisher:
    def publish(self, targets, *, mode=PublicationMode.APPEND):
        materialized = tuple(targets)
        return PublicationResult(len(materialized), (), mode)


class ServiceInterfaceTests(unittest.TestCase):
    def test_shacl_adapter_normalizes_legacy_results(self) -> None:
        service = ShaclValidationService(FakeValidator())

        outcome = service.validate(Graph(), label="empty graph")

        self.assertIsInstance(service, ValidationService)
        self.assertTrue(outcome.conforms)
        self.assertEqual(outcome.label, "empty graph")
        self.assertEqual(outcome.details, "Conforms")

    def test_graph_publisher_is_structural(self) -> None:
        publisher = FakePublisher()

        self.assertIsInstance(publisher, GraphPublisher)
        self.assertEqual(publisher.publish(()).published_files, 0)

    def test_graphdb_adapter_rejects_invalid_timeout_without_network(self) -> None:
        with self.assertRaisesRegex(ValueError, "timeout must be positive"):
            GraphDbPublisher(host="http://localhost:7200", repository="test", timeout=0)

    def test_graphdb_adapter_accepts_empty_publication_without_network(self) -> None:
        publisher = GraphDbPublisher(host="http://localhost:7200", repository="test")

        result = publisher.publish(())

        self.assertIsInstance(publisher, GraphPublisher)
        self.assertEqual(result, PublicationResult(0, (), PublicationMode.APPEND))


if __name__ == "__main__":
    unittest.main()

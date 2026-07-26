from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

import requests
from rdflib import URIRef

from pipeline import build_alignment as legacy_alignment
from pipeline import load_graphdb as legacy_graphdb
from semantic_processing import event_resolver as legacy_resolution
from semantic_processing import location_matcher_single as legacy_single_location
from semantic_processing import location_matcher_v2 as legacy_hierarchical_location
from semantic_processing import org_resolver as legacy_organizations
from sakunagraph_etl.enrichment import locations
from sakunagraph_etl.enrichment.organizations import OrgResolver
from sakunagraph_etl.io import graphdb
from sakunagraph_etl.rdf.publication import (
    GraphDbPublisher,
    PublicationMode,
    PublicationTarget,
    PublicationValidationError,
    publication_validation_required,
)
from sakunagraph_etl.resolution import blocking, clustering, features, registry, scoring
from sakunagraph_etl.resolution.models import DisasterEvent
from sakunagraph_etl.resolution import job as alignment_job


class FakeResponse:
    def __init__(
        self,
        *,
        headers: dict[str, str] | None = None,
        error: requests.RequestException | None = None,
        text: str = "",
    ) -> None:
        self.headers = headers or {}
        self.error = error
        self.text = text

    def raise_for_status(self) -> None:
        if self.error is not None:
            self.error.response = self
            raise self.error


class ReplacementSession:
    def __init__(
        self,
        *,
        fail: bool = False,
        fail_graph: str | None = None,
    ) -> None:
        self.fail = fail
        self.fail_graph = fail_graph
        self.calls: list[tuple[str, str, str | None]] = []
        self.payload = b""

    def put(self, url: str, **kwargs):
        graph = kwargs.get("params", {}).get("graph")
        self.calls.append(("PUT", url, graph))
        self.payload = kwargs["data"].read()
        if self.fail or graph == self.fail_graph:
            return FakeResponse(
                error=requests.HTTPError("request failed"),
                text="invalid RDF",
            )
        return FakeResponse()


class NoNetworkSession:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str):
        def unexpected(*args, **kwargs):
            self.calls.append(name)
            raise AssertionError(f"network method called: {name}")

        return unexpected


class FailedValidationService:
    def validate(self, graph, **kwargs):
        return SimpleNamespace(conforms=False, details="fixture violation")


class Stage5ResolutionTests(unittest.TestCase):
    def test_resolution_roles_are_package_owned_and_legacy_compatible(self) -> None:
        self.assertIs(legacy_resolution.load_all_sources, features.load_all_sources)
        self.assertIs(
            legacy_resolution.generate_candidate_pairs,
            blocking.generate_candidate_pairs,
        )
        self.assertIs(legacy_resolution.score_pair, scoring.score_pair)
        self.assertIs(legacy_resolution.build_clusters, clustering.build_clusters)
        self.assertIs(legacy_resolution.save_registry, registry.save_registry)
        self.assertIs(legacy_alignment.run, alignment_job.run)

    def test_cluster_identifier_is_independent_of_pair_order(self) -> None:
        events = [
            DisasterEvent(URIRef(f"https://sakuna.ph/{source}/event"), source)
            for source in ("gda", "emdat", "ndrrmc")
        ]
        match = SimpleNamespace(is_match=True)
        forward = [(events[0], events[1], match), (events[1], events[2], match)]
        reverse = [(events[2], events[1], match), (events[1], events[0], match)]

        forward_clusters = clustering.build_clusters(forward)
        reverse_clusters = clustering.build_clusters(reverse)

        self.assertEqual(len(forward_clusters), 1)
        self.assertEqual(forward_clusters[0][0], reverse_clusters[0][0])
        self.assertEqual(forward_clusters[0][1], reverse_clusters[0][1])
        with tempfile.TemporaryDirectory() as temp:
            forward_path = Path(temp) / "forward.json"
            reverse_path = Path(temp) / "reverse.json"
            registry.save_registry(forward_clusters, forward_path)
            registry.save_registry(reverse_clusters, reverse_path)
            self.assertEqual(
                forward_path.read_text(encoding="utf-8"),
                reverse_path.read_text(encoding="utf-8"),
            )

    def test_registry_merge_is_deterministic_and_keeps_untouched_clusters(self) -> None:
        events = [
            DisasterEvent(URIRef(f"https://sakuna.ph/{source}/event"), source)
            for source in ("gda", "emdat", "ndrrmc")
        ]
        untouched_member = "https://sakuna.ph/dromic/untouched"
        existing = {
            "https://sakuna.graph/common/z-cluster": frozenset({str(events[0].uri)}),
            "https://sakuna.graph/common/a-cluster": frozenset({str(events[2].uri)}),
            "https://sakuna.graph/common/untouched": frozenset({untouched_member}),
        }
        match = SimpleNamespace(is_match=True)
        pairs = [(events[0], events[1], match), (events[1], events[2], match)]

        forward, forward_new = clustering.expand_clusters(pairs, existing)
        reverse, reverse_new = clustering.expand_clusters(list(reversed(pairs)), existing)

        self.assertEqual(forward, reverse)
        self.assertEqual(forward_new, reverse_new)
        clusters = {
            str(canonical): {str(member) for member in members}
            for canonical, members in forward
        }
        self.assertEqual(
            clusters["https://sakuna.graph/common/a-cluster"],
            {str(event.uri) for event in events},
        )
        self.assertEqual(
            clusters["https://sakuna.graph/common/untouched"],
            {untouched_member},
        )
        self.assertNotIn("https://sakuna.graph/common/z-cluster", clusters)


class Stage5EnrichmentTests(unittest.TestCase):
    def test_location_strategies_share_one_interface(self) -> None:
        single = locations.get_location_service(locations.LocationStrategy.SINGLE)
        hierarchical = locations.get_location_service("hierarchical")

        self.assertIsInstance(single, locations.LocationResolutionService)
        self.assertIsInstance(hierarchical, locations.LocationResolutionService)
        self.assertEqual(single.match_cell(""), [])
        self.assertIs(
            legacy_single_location.match_location,
            locations.match_location,
        )
        self.assertIs(
            legacy_hierarchical_location.LOCATION_MATCHER,
            locations.LOCATION_MATCHER,
        )

    def test_organization_service_and_legacy_facade_share_implementation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            registry_path = Path(temp) / "organizations.json"
            registry_path.write_text(
                '{"OCD": ["Office of Civil Defense"]}',
                encoding="utf-8",
            )
            resolver = OrgResolver(registry_path)

            self.assertEqual(
                resolver.resolve("Office of Civil Defense"),
                URIRef("https://sakuna.ph/org/OCD"),
            )
        self.assertIs(legacy_organizations.OrgResolver, OrgResolver)


class Stage5PublicationTests(unittest.TestCase):
    def test_graphdb_replacement_bundles_context_in_one_put(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            first_path = Path(temp) / "first.ttl"
            second_path = Path(temp) / "second.ttl"
            first_path.write_text(
                "<https://sakuna.ph/event> <https://sakuna.ph/name> \"Event\" .\n",
                encoding="utf-8",
            )
            second_path.write_text(
                "<https://sakuna.ph/other> <https://sakuna.ph/name> \"Other\" .\n",
                encoding="utf-8",
            )
            context = "https://sakuna.ph/events/test"
            session = ReplacementSession()

            graphdb.replace_context(
                session,
                "http://graphdb/repositories/test/rdf-graphs/service",
                context,
                [
                    graphdb.LoadTarget(first_path, context),
                    graphdb.LoadTarget(second_path, context),
                ],
                30,
            )

        self.assertEqual(
            session.calls,
            [(
                "PUT",
                "http://graphdb/repositories/test/rdf-graphs/service",
                context,
            )],
        )
        self.assertIn(b"https://sakuna.ph/event", session.payload)
        self.assertIn(b"https://sakuna.ph/other", session.payload)

    def test_failed_graph_store_put_reports_replacement_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            ttl_path = Path(temp) / "event.ttl"
            ttl_path.write_text(
                "<https://sakuna.ph/event> <https://sakuna.ph/name> \"Event\" .\n",
                encoding="utf-8",
            )
            target = graphdb.LoadTarget(ttl_path, "https://sakuna.ph/events/test")
            session = ReplacementSession(fail=True)

            with self.assertRaisesRegex(graphdb.LoaderError, "Could not replace"):
                graphdb.replace_context(
                    session,
                    "http://graphdb/repositories/test/rdf-graphs/service",
                    target.context,
                    [target],
                    30,
                )

        self.assertEqual(len(session.calls), 1)

    def test_publication_attempts_other_contexts_after_one_put_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            failed_path = root / "failed.ttl"
            successful_path = root / "successful.ttl"
            failed_path.write_text("<urn:failed> <urn:p> <urn:o> .", encoding="utf-8")
            successful_path.write_text(
                "<urn:successful> <urn:p> <urn:o> .",
                encoding="utf-8",
            )
            failed_graph = "https://sakuna.ph/events/failed"
            successful_graph = "https://sakuna.ph/events/successful"
            session = ReplacementSession(fail_graph=failed_graph)
            publisher = GraphDbPublisher(
                host="http://graphdb",
                repository="test",
                session=session,
                validate_connection=False,
            )

            with self.assertRaisesRegex(
                graphdb.LoaderError,
                "1 GraphDB publication request",
            ):
                publisher.publish(
                    [
                        PublicationTarget(failed_path, failed_graph),
                        PublicationTarget(successful_path, successful_graph),
                    ],
                    mode=PublicationMode.REPLACE,
                )

        self.assertEqual(
            {graph for _, _, graph in session.calls},
            {failed_graph, successful_graph},
        )

    def test_shacl_failure_prevents_all_graphdb_requests(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            ttl_path = Path(temp) / "event.ttl"
            ttl_path.write_text(
                "<https://sakuna.ph/event> <https://sakuna.ph/name> \"Event\" .\n",
                encoding="utf-8",
            )
            session = NoNetworkSession()
            publisher = GraphDbPublisher(
                host="http://graphdb",
                repository="test",
                session=session,
                require_validation=True,
                validation_service=FailedValidationService(),
            )

            with self.assertRaisesRegex(PublicationValidationError, "SHACL"):
                publisher.publish(
                    [PublicationTarget(ttl_path, "https://sakuna.ph/events/test")],
                    mode=PublicationMode.REPLACE,
                )

        self.assertEqual(session.calls, [])

    def test_invalid_turtle_prevents_all_graphdb_requests(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            ttl_path = Path(temp) / "broken.ttl"
            ttl_path.write_text("this is not Turtle", encoding="utf-8")
            session = NoNetworkSession()
            publisher = GraphDbPublisher(
                host="http://graphdb",
                repository="test",
                session=session,
                require_validation=True,
                validation_service=FailedValidationService(),
            )

            with self.assertRaisesRegex(PublicationValidationError, "Invalid Turtle"):
                publisher.publish(
                    [PublicationTarget(ttl_path, "https://sakuna.ph/events/test")],
                    mode=PublicationMode.REPLACE,
                )

        self.assertEqual(session.calls, [])

    def test_production_profiles_cannot_disable_validation(self) -> None:
        self.assertFalse(publication_validation_required("local"))
        self.assertTrue(publication_validation_required("onprem"))
        self.assertTrue(publication_validation_required("cloud"))
        with self.assertRaises(PublicationValidationError):
            publication_validation_required("cloud", False)

    def test_legacy_graphdb_module_reexports_package_implementation(self) -> None:
        self.assertIs(legacy_graphdb.replace_context, graphdb.replace_context)
        self.assertIs(legacy_graphdb.main, graphdb.main)


if __name__ == "__main__":
    unittest.main()

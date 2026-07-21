import unittest

from rdflib import URIRef

from sakunagraph_etl.rdf.iris import event_uri, mint_canonical_iri, sub_iri


class IriTests(unittest.TestCase):
    def test_event_iri_is_deterministic_and_source_scoped(self) -> None:
        first = event_uri("dromic", "event-123")

        self.assertEqual(first, event_uri("dromic", "event-123"))
        self.assertNotEqual(first, event_uri("ndrrmc", "event-123"))

    def test_canonical_iri_is_independent_of_member_order(self) -> None:
        members = frozenset({"https://sakuna.ph/a", "https://sakuna.ph/b"})

        self.assertEqual(
            mint_canonical_iri(members),
            mint_canonical_iri(frozenset(reversed(sorted(members)))),
        )

    def test_sub_iri_keeps_event_scope(self) -> None:
        event = URIRef("https://sakuna.ph/dromic/event")

        self.assertEqual(
            sub_iri(event, "casualties", "2"),
            URIRef("https://sakuna.ph/dromic/event/casualties/2"),
        )


if __name__ == "__main__":
    unittest.main()

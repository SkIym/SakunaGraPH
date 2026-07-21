"""Shared RDF namespaces and graph construction helpers."""

from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS, XSD

SKG = Namespace("https://sakuna.ph/")
BAW = Namespace("https://raw.githubusercontent.com/beAWARE-project/ontology/master/beAWARE_ontology#")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
PROV = Namespace("http://www.w3.org/ns/prov#")
QUDT = Namespace("http://qudt.org/schema/qudt/")
CUR = Namespace("http://qudt.org/vocab/currency/")
ORG = Namespace("https://sakuna.ph/org/")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")


def create_graph() -> Graph:
    graph = Graph()
    graph.bind("", SKG)
    graph.bind("baw", BAW)
    graph.bind("xsd", XSD)
    graph.bind("qudt", QUDT)
    graph.bind("prov", PROV)
    graph.bind("owl", OWL)
    graph.bind("rdfs", RDFS)
    graph.bind("geo", GEO)
    return graph


def add_monetary(
    graph: Graph,
    subject: URIRef,
    predicate: URIRef,
    value: float,
    unit: URIRef,
) -> None:
    """Attach a QUDT ``QuantityValue`` blank node to a subject."""
    node = BNode()
    graph.add((subject, predicate, node))
    graph.add((node, RDF.type, QUDT.QuantityValue))
    graph.add((node, QUDT.numericValue, Literal(value, datatype=XSD.decimal)))
    graph.add((node, QUDT.unit, unit))


__all__ = [
    "BAW",
    "CUR",
    "GEO",
    "ORG",
    "PROV",
    "QUDT",
    "SKG",
    "SKOS",
    "Graph",
    "add_monetary",
    "create_graph",
]

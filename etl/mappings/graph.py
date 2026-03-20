from rdflib import Graph, Namespace, URIRef, BNode, RDF, RDFS, Literal, XSD, OWL

SKG = Namespace("https://sakuna.ph/")
PROV = Namespace("http://www.w3.org/ns/prov#")
QUDT   = Namespace("http://qudt.org/schema/qudt/")
CUR    = Namespace("http://qudt.org/vocab/currency/")
ORG = Namespace("https://sakuna.ph/org/")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")

def create_graph() -> Graph:
    g = Graph()
    g.bind("", SKG)
    g.bind("xsd", "http://www.w3.org/2001/XMLSchema#")
    g.bind("qudt", QUDT)
    g.bind("prov", PROV)
    g.bind("owl", OWL)
    g.bind("rdfs", RDFS)
    g.bind("geo", GEO)
    return g

def add_monetary(g: Graph, subject: URIRef, predicate: URIRef, value: float, unit: URIRef) -> None:
    """Attach a QUDT QuantityValue blank node to subject via predicate."""
    node = BNode()
    g.add((subject, predicate,         node))
    g.add((node,    RDF.type,          QUDT.QuantityValue))
    g.add((node,    QUDT.numericValue, Literal(value, datatype=XSD.decimal)))
    g.add((node,    QUDT.unit,         unit))
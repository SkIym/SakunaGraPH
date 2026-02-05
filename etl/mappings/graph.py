from rdflib import Graph, Namespace

SKG = Namespace("https://sakuna.ph/")

def create_graph() -> Graph:
    g = Graph()
    g.bind("", SKG)
    g.bind("xsd", "http://www.w3.org/2001/XMLSchema#")
    return g

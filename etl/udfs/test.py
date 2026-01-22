from rdflib import Graph, RDF, Namespace, URIRef

SKG = Namespace("https://sakuna.ph/")

lg = Graph()
lg.parse("triples/psgc_rdf.ttl")

for s, p, o in lg.triples((None, RDF.type, URIRef(SKG["Region"]))):
    print(f"{s} is a region")

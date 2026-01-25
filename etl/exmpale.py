from rdflib import Graph, Namespace, RDF

g = Graph()
g.parse("triples/gda.nt", format="nt")

SKNPH = Namespace("https://sakuna.ph/")

query = """
PREFIX : <https://sakuna.ph/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT (COUNT(?e) AS ?total)
WHERE { ?e a :MajorEvent . }
"""

results = g.query(query)

for row in results:
    event = row["total"]
    
    # Treat everything as string to avoid ValueError
    print(event)

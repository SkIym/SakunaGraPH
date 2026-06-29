from pathlib import Path

from pyshacl import validate_each


ROOT_DIR = Path(__file__).resolve().parents[2]

DATA_GRAPH_PATTERNS = [
    ROOT_DIR / "data/rdf/events/gda/*.ttl",
    ROOT_DIR / "data/rdf/psgc/psgc.ttl",
    ROOT_DIR / "data/rdf/orgs/orgs.ttl",
    ROOT_DIR / "data/rdf/prov/prov.ttl",
    ROOT_DIR / "data/rdf/resolution/alignments.ttl",
]

data_graphs = [
    str(path)
    for pattern in DATA_GRAPH_PATTERNS
    for path in sorted(pattern.parent.glob(pattern.name))
]
shacl_graph = str(ROOT_DIR / "ontology/shapes/shapes.ttl")
ont_graph = str(ROOT_DIR / "ontology/sakunagraph.ttl")

if not data_graphs:
    raise FileNotFoundError("No RDF data graphs found under data/rdf.")

results = validate_each(
    data_graphs,
    shacl_graph=shacl_graph,
    ont_graph=ont_graph,
    inference=None,
    abort_on_first=False,
    allow_infos=False,
    allow_warnings=False,
    meta_shacl=False,
    advanced=False,
    js=False,
    debug=False,
)


for graph_id, (conforms, _results_graph, results_text) in results.items():
    print(graph_id, conforms)
    if not conforms:
        print(results_text)

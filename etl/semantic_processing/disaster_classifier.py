from sentence_transformers import SentenceTransformer, util
from typing import List, Tuple
from rdflib import Graph, RDF, Namespace, URIRef, RDFS, SKOS
from rdflib.namespace import DefinedNamespace

SKG = Namespace("https://sakuna.ph/")

LEAF_CLASS_QUERY = """
SELECT DISTINCT ?c ?defi WHERE {
    ?c skos:inScheme skg:DisasterTypeScheme ;
       skos:definition ?defi .
    FILTER NOT EXISTS {
        ?child skos:broader ?c .
    }
}
"""


ONTOLOGY_PATH = "../ontology/sakunagraph.ttl"
INIT_NS: dict[str, Namespace | type[DefinedNamespace]] = {"rdfs": RDFS, "skg": SKG, "skos": SKOS}


def _uri_to_label(graph: Graph, uri: URIRef) -> str:
    return graph.namespace_manager.normalizeUri(uri).split(":", 1)[1]


def _load_classes(graph: Graph) -> dict[str, str]:
    """Load leaf DisasterType classes + WaveAction, keyed by definition."""
    classes: dict[str, str] = {}

    for row in graph.query(LEAF_CLASS_QUERY, initNs=INIT_NS):
        label = _uri_to_label(graph, row.c)
        classes[row.defi.value] = label

    return classes


class DisasterClassifier:
    def __init__(self, model_name: str, ontology_path: str = ONTOLOGY_PATH):
        self.model = SentenceTransformer(model_name)

        g = Graph()
        g.parse(ontology_path)

        self.classes = _load_classes(g)
        self.class_keys = list(self.classes.keys())
        self.class_labels = list(self.classes.values())
        self.embeddings = self.model.encode(self.class_keys, convert_to_tensor=True)

    def classify(self, texts: List[str]) -> List[Tuple[str, float]]:
        """
        Classify a list of incident descriptions.
        Returns a list of (predicted_class_label, similarity_score) tuples.
        """
        text_embeddings = self.model.encode(texts, convert_to_tensor=True)
        scores = util.cos_sim(text_embeddings, self.embeddings)

        return [
            (self.class_labels[row.argmax().item()], row.max().item())
            for row in scores
        ]


DISASTER_CLASSIFIER = DisasterClassifier(
    model_name="all-mpnet-base-v2",
)
from sentence_transformers import SentenceTransformer, util
from typing import List, Tuple
from rdflib import Graph, RDF, Namespace, URIRef, RDFS, SKOS
from rdflib.namespace import DefinedNamespace

from .classification_rules import CLASSIFICATION_RULES, ClassificationRule

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
    def __init__(self, model_name: str, ontology_path: str = ONTOLOGY_PATH,
                 rules: list[ClassificationRule] | None = None):
        self.model = SentenceTransformer(model_name)
        self.rules = rules if rules is not None else CLASSIFICATION_RULES

        g = Graph()
        g.parse(ontology_path)

        self.classes = _load_classes(g)
        self.class_keys = list(self.classes.keys())
        self.class_labels = list(self.classes.values())
        self.embeddings = self.model.encode(self.class_keys, convert_to_tensor=True)

    def _rule_match(self, text: str) -> str | None:
        text_lower = text.lower()
        for rule in self.rules:
            tokens, label = rule[0], rule[1]
            context: list[str] | None = rule[2] if len(rule) > 2 else None
            if any(t.lower() in text_lower for t in tokens):
                if context is None or any(c.lower() in text_lower for c in context):
                    return label
        return None

    def classify(self, texts: List[str]) -> List[Tuple[str, float]]:
        """
        Classify a list of incident descriptions.
        Rule-matched texts return (label, 1.0); the rest go through the transformer.
        Returns a list of (predicted_class_label, similarity_score) tuples.
        """
        results: dict[int, tuple[str, float]] = {}
        need_transformer: list[int] = []

        for i, text in enumerate(texts):
            label = self._rule_match(text)
            if label:
                results[i] = (label, 1.0)
            else:
                need_transformer.append(i)

        if need_transformer:
            batch = [texts[i] for i in need_transformer]
            batch_embeddings = self.model.encode(batch, convert_to_tensor=True)
            scores = util.cos_sim(batch_embeddings, self.embeddings)
            for j, i in enumerate(need_transformer):
                row = scores[j]
                results[i] = (self.class_labels[row.argmax().item()], row.max().item())

        return [results[i] for i in range(len(texts))]


DISASTER_CLASSIFIER = DisasterClassifier(
    model_name="all-mpnet-base-v2",
)
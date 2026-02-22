from sentence_transformers import SentenceTransformer, util
from typing import List, Tuple
from rdflib import Graph, RDF, Namespace, URIRef, RDFS, SKOS

class DisasterClassifier:
    def __init__(self, model_name: str):
        # Load model 
        self.model = SentenceTransformer(model_name)

        # Load classes
        # with open(classes_path, "r", encoding="utf-8") as f:
        #     self.classes: dict[str, str] = json.load(f)

        # Load classes
        SKG = Namespace("https://sakuna.ph/")
        g = Graph()
        g.parse("../ontology/sakunagraph.ttl")

        self.classes: dict[str, str] = {}

        q = """
        SELECT DISTINCT ?c ?defi WHERE {
        ?c rdfs:subClassOf+ skg:DisasterType ;
            skos:definition ?defi ;
        FILTER NOT EXISTS {
            ?child rdfs:subClassOf ?c .
        }
        }
        """
        for row in g.query(q, initNs={"rdfs": RDFS, "skg": SKG, "skos": SKOS}):
            # print(g.namespace_manager.normalizeUri(row.c).split(":", 1)[1], row.defi)

            clsf = g.namespace_manager.normalizeUri(row.c).split(":", 1)[1]
            self.classes[row.defi.value] = clsf

        # Precompute embeddings for class names
        self.class_keys = list(self.classes.keys())
        self.class_labels = list(self.classes.values())
        self.embeddings = self.model.encode(self.class_keys, convert_to_tensor=True)

    def classify(self, texts: List[str]) -> List[Tuple[str, float]]:
        """
        Classify a list of incident types.
        Returns a list of (predicted_class_label, similarity_score) tuples.
        """
        # Encode all texts at once
        text_embeddings = self.model.encode(texts, convert_to_tensor=True)

        # Compute cosine similarities in one shot
        # scores = util.dot_score(text_embeddings, self.embeddings)
        scores = util.cos_sim(text_embeddings, self.embeddings)  

        # Find best class per text
        results = []
        for i, score_vector in enumerate(scores):
            best_idx = score_vector.argmax().item()
            results.append((self.class_labels[best_idx], score_vector[best_idx].item()))
        return results

DISASTER_CLASSIFIER = DisasterClassifier(
    # model_name="all-MiniLM-L6-v2",
    model_name="all-mpnet-base-v2",
    # model_name="multi-qa-mpnet-base-dot-v1",
)


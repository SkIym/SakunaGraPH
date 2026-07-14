from sentence_transformers import SentenceTransformer, util
from typing import List, Tuple
from pathlib import Path
from rdflib import Graph, RDF, Namespace, URIRef, RDFS, SKOS
from rdflib.namespace import DefinedNamespace

from .classification_rules import (
    CLASSIFICATION_RULES,
    ClassificationRule,
    labels_are_ambiguous,
    ambiguous_candidate_set,
    LABEL_TO_GROUP
)

SKG = Namespace("https://sakuna.ph/")
ROOT_DIR = Path(__file__).resolve().parents[2]

LEAF_CLASS_QUERY = """
SELECT DISTINCT ?c ?defi WHERE {
    ?c skos:inScheme skg:DisasterTypeScheme ;
       skos:note ?defi .
    FILTER NOT EXISTS {
        ?child skos:broader ?c .
    }
}
"""

ONTOLOGY_PATH = ROOT_DIR / "ontology" / "disaster_type_scheme.ttl"
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
    def __init__(
        self,
        model_name: str,
        ontology_path: str | Path = ONTOLOGY_PATH,
        rules: list[ClassificationRule] | None = None,
    ):
        self.model = SentenceTransformer(model_name)
        self.rules = rules if rules is not None else CLASSIFICATION_RULES
        ontology_path = Path(ontology_path)

        g = Graph()
        g.parse(str(ontology_path))

        self.classes = _load_classes(g)
        if not self.classes:
            raise ValueError(
                "No disaster type labels were loaded from "
                f"{Path(ontology_path).resolve()}. Check the skos:inScheme/skos:note query."
            )

        self.class_keys = list(self.classes.keys())
        self.class_labels = list(self.classes.values())
        self.embeddings = self.model.encode(self.class_keys, convert_to_tensor=True)

        # Pre-build a definition-key index keyed by label for fast candidate lookup
        self._label_to_idx: dict[str, int] = {
            label: i for i, label in enumerate(self.class_labels)
        }

    # ── Rule layer ────────────────────────────────────────────────────────────

    def _rule_candidates(self, text: str) -> list[str]:
        """
        Return ALL labels whose rules fire on *text* (no early exit).
        Preserves insertion order; deduplicates.
        """
        text_lower = text.lower()
        seen: set[str] = set()
        candidates: list[str] = []

        for rule in self.rules:
            tokens, label = rule[0], rule[1]
            context: list[str] | None = rule[2] if len(rule) > 2 else None

            if label in seen:
                continue
            if not any(t.lower() in text_lower for t in tokens):
                continue
            if context is not None and not any(c.lower() in text_lower for c in context):
                continue

            seen.add(label)
            candidates.append(label)

        return candidates

    # ── Transformer layer ─────────────────────────────────────────────────────

    def _transformer_pick(
        self, text: str, restrict_to: list[str] | None = None
    ) -> tuple[str, float]:
        """
        Run the transformer over *restrict_to* labels (or all labels if None).
        Returns (label, cosine_similarity).
        """
        if restrict_to:
            indices = [
                self._label_to_idx[lbl]
                for lbl in restrict_to
                if lbl in self._label_to_idx
            ]
            if not indices:
                # Fallback: none of the candidates exist in the ontology — use full space
                indices = list(range(len(self.class_labels)))
        else:
            indices = list(range(len(self.class_labels)))

        if not indices:
            raise ValueError("Disaster classifier has no ontology labels to score.")

        candidate_embeddings = self.embeddings[indices]
        text_embedding = self.model.encode([text], convert_to_tensor=True)
        scores = util.cos_sim(text_embedding, candidate_embeddings)[0]

        best_local = scores.argmax().item()
        best_idx = indices[best_local]
        return self.class_labels[best_idx], scores[best_local].item()

    # ── Classification routing ────────────────────────────────────────────────

    def _route(self, text: str) -> tuple[str, float]:
        """
        Routing logic for a single text:

        1. Collect all firing rule candidates.
        2. If no candidates → transformer over full label space.
        3. If exactly one candidate and it is NOT in any ambiguity group
           → hard rule win (score 1.0).
        4. If candidates are ambiguous (all share a group) → transformer
           restricted to the union of their ambiguity groups.
        5. If candidates span unrelated groups (e.g. Fire + Flood — unusual
           but possible) → transformer restricted to the union of fired labels.
        """
        candidates = self._rule_candidates(text)

        # Case 2: no rules fired
        if not candidates:
            return self._transformer_pick(text, restrict_to=None)

        # Case 3: single unambiguous candidate
        if len(candidates) == 1 or candidates[0] not in LABEL_TO_GROUP:
                return candidates[0], 1.0

        # Case 4 & 5: ambiguous candidates — transformer resolves
        if labels_are_ambiguous(candidates):
            restrict = ambiguous_candidate_set(candidates)
        else:
            # Mixed groups or single ambiguous label — restrict to fired set + their siblings
            restrict = ambiguous_candidate_set(candidates) or candidates

        return self._transformer_pick(text, restrict_to=restrict)

    # ── Public API ────────────────────────────────────────────────────────────

    def classify(self, texts: List[str]) -> List[Tuple[str, float]]:
        """
        Classify a list of incident descriptions.

        Returns a list of (predicted_class_label, confidence_score) tuples.
        Score is 1.0 for hard rule wins, cosine similarity otherwise.
        """
        return [self._route(text) for text in texts]


DISASTER_CLASSIFIER = DisasterClassifier(
    model_name="all-mpnet-base-v2",
)

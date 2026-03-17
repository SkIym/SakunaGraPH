"""Extract and resolve agency/organization names to canonical IRIs.

Splits concatenated organization strings (from NDRRMC, GDA, EM-DAT sources)
into individual names and resolves them to canonical slugs for IRI generation.

Supports two extraction modes:
  - Rule-based splitting (split_and_resolve): regex split + fuzzy matching
  - NER-based extraction (extract_orgs_ner): spaCy NER for ORG entities

Uses a global JSON registry (org_registry.json) shared across all sources.
Unknown organizations discovered by NER are auto-inserted into the registry.
"""

import json
import re
from pathlib import Path

from thefuzz import fuzz

_REGISTRY_PATH = Path(__file__).parent / "org_registry.json"

# Splitter pattern: commas, semicolons, " and ", slashes, newlines
_SPLIT_PATTERN = re.compile(r'[,;\n/]+|\band\b', re.IGNORECASE)

FUZZY_THRESHOLD = 79


def _load_registry(path: Path = _REGISTRY_PATH) -> dict[str, list[str]]:
    """Load the org registry JSON. Returns {slug: [aliases...]}."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _build_alias_map(registry: dict[str, list[str]]) -> dict[str, str]:
    """Build a flat alias→slug lookup from the registry."""
    alias_map: dict[str, str] = {}
    for slug, aliases in registry.items():
        alias_map[slug] = slug
        for alias in aliases:
            alias_map[alias] = slug
    return alias_map


def _slugify(name: str) -> str:
    """Generate a slug from an org name for new registry entries."""
    # Use acronym if it looks like one (all caps, short)
    stripped = name.strip()
    if stripped.isupper() and len(stripped) <= 10 and " " not in stripped:
        return stripped
    # Otherwise, take first letters of each word
    words = re.findall(r'[A-Z][a-z]*|[A-Z]+', stripped)
    if words:
        acronym = "".join(w[0] for w in words).upper()
        if len(acronym) >= 2:
            return acronym
    # Fallback: sanitize the name
    return re.sub(r'[^A-Za-z0-9]+', '_', stripped).strip('_')


class OrgResolver:
    """Resolve organization names to canonical slugs.

    Backed by a global JSON registry that persists across sources.
    """

    def __init__(
        self,
        registry_path: Path = _REGISTRY_PATH,
        threshold: int = FUZZY_THRESHOLD,
    ):
        self.registry_path = registry_path
        self.threshold = threshold
        self._nlp = None

        # Load registry and build lookup
        self._registry = _load_registry(registry_path)
        self._aliases = _build_alias_map(self._registry)
        self._keys_lower = {k.lower(): v for k, v in self._aliases.items()}

    def _reload(self):
        """Reload the registry from disk (after external changes)."""
        self._registry = _load_registry(self.registry_path)
        self._aliases = _build_alias_map(self._registry)
        self._keys_lower = {k.lower(): v for k, v in self._aliases.items()}

    @property
    def nlp(self):
        """Lazy-load spaCy model for NER."""
        if self._nlp is None:
            import spacy
            self._nlp = spacy.load("en_core_web_sm")
        return self._nlp

    # ── Registry management ──────────────────────────────────────────

    def insert(self, slug: str, aliases: list[str] | None = None) -> str:
        """Insert a new org into the registry if it doesn't exist.

        Returns the slug (existing or newly created).
        """
        # Check if already exists (by slug or alias)
        existing = self._keys_lower.get(slug.lower())
        if existing:
            return existing

        # Add to registry
        all_aliases = [slug] + (aliases or [])
        self._registry[slug] = all_aliases
        for alias in all_aliases:
            self._aliases[alias] = slug
            self._keys_lower[alias.lower()] = slug

        self._save()
        return slug

    def _save(self):
        """Persist the current registry to disk."""
        with open(self.registry_path, "w", encoding="utf-8") as f:
            json.dump(self._registry, f, indent=4, ensure_ascii=False)

    # ── Splitting ────────────────────────────────────────────────────

    def split_orgs(self, text: str) -> list[str]:
        """Split a concatenated org string into individual names."""
        if not text:
            return []
        parts = _SPLIT_PATTERN.split(str(text))
        return [p.strip() for p in parts if p and p.strip()]

    # ── Resolution ───────────────────────────────────────────────────

    def resolve(self, name: str) -> str | None:
        """Resolve a single org name to its canonical slug.

        Returns None if no match is found above threshold.
        """
        if not name or not name.strip():
            return None

        name_stripped = name.strip()

        # Exact match (case-insensitive)
        exact = self._keys_lower.get(name_stripped.lower())
        if exact:
            return exact

        # Fuzzy match
        best_score = 0
        best_slug = None
        for key_lower, slug in self._keys_lower.items():
            score = fuzz.token_sort_ratio(name_stripped.lower(), key_lower)
            if score > best_score:
                best_score = score
                best_slug = slug

        if best_score >= self.threshold:
            return best_slug

        return None

    def split_and_resolve(self, text: str) -> list[str]:
        """Split text into org names and resolve each. Returns list of canonical slugs."""
        results = []
        seen = set()
        for name in self.split_orgs(text):
            slug = self.resolve(name)
            if slug and slug not in seen:
                results.append(slug)
                seen.add(slug)
        return results

    # ── NER-based extraction ─────────────────────────────────────────

    def extract_orgs_ner(self, text: str, auto_insert: bool = True) -> list[str]:
        """Extract organizations from text using spaCy NER.

        Uses spaCy's named entity recognition to find ORG entities,
        then resolves each against the registry. Unknown orgs are
        optionally auto-inserted into the registry.

        Args:
            text: Free-text input to extract orgs from.
            auto_insert: If True, insert unrecognized ORG entities
                into the registry with a generated slug.

        Returns:
            List of canonical slugs (deduplicated, order-preserved).
        """
        if not text or not text.strip():
            return []

        doc = self.nlp(text)
        results = []
        seen = set()

        for ent in doc.ents:
            if ent.label_ != "ORG":
                continue

            org_text = ent.text.strip()
            if not org_text:
                continue

            # Try to resolve against existing registry
            slug = self.resolve(org_text)

            if slug is None and auto_insert:
                slug = _slugify(org_text)
                self.insert(slug, aliases=[org_text])

            if slug and slug not in seen:
                results.append(slug)
                seen.add(slug)

        return results


# Module-level singleton
ORG_RESOLVER = OrgResolver()

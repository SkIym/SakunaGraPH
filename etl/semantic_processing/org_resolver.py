"""
org_resolver.py
~~~~~~~~~~~~~~~~
Resolve organization name strings to canonical IRIs backed by org_registry.json.

Resolution order:
  1. Exact match (case-insensitive) against slug and all aliases
  2. Fuzzy match (thefuzz token_sort_ratio) against the same pool
  3. None  -- no match above threshold
"""

import json
import re
from pathlib import Path

from rdflib import URIRef
from thefuzz import fuzz

_REGISTRY_PATH = "../constants/org_registry.json"
_ORG_BASE      = "https://sakuna.ph/org/"

_SPLIT_PATTERN = re.compile(r'[,;\n/]+|\band\b', re.IGNORECASE)

FUZZY_THRESHOLD = 79


def _to_iri(slug: str) -> URIRef:
    return URIRef(_ORG_BASE + slug)


def _load_registry(path: Path) -> dict[str, list[str]]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _build_pool(registry: dict[str, list[str]]) -> list[tuple[str, str]]:
    """Flat list of (label_lower, slug) covering slugs and all aliases."""
    pool = []
    for slug, aliases in registry.items():
        pool.append((slug.lower(), slug))
        for alias in aliases:
            pool.append((alias.lower(), slug))
    return pool


class OrgResolver:
    def __init__(
        self,
        registry_path: Path = _REGISTRY_PATH,
        threshold: int = FUZZY_THRESHOLD,
    ):
        self.registry_path = registry_path
        self.threshold     = threshold
        self._pool         = _build_pool(_load_registry(registry_path))

    def reload(self) -> None:
        """Reload the registry from disk."""
        self._pool = _build_pool(_load_registry(self.registry_path))

    def split_orgs(self, text: str) -> list[str]:
        """Split a concatenated org string into individual names."""
        if not text:
            return []
        parts = _SPLIT_PATTERN.split(str(text))
        return [p.strip() for p in parts if p and p.strip()]

    def resolve(self, name: str) -> URIRef | None:
        """Resolve a single org name to its canonical IRI.

        Returns None if no match is found above threshold.
        """
        if not name or not name.strip():
            return None

        name_lower = name.strip().lower()

        # 1. Exact match
        for label_lower, slug in self._pool:
            if label_lower == name_lower:
                return _to_iri(slug)

        # 2. Fuzzy match
        best_score, best_slug = 0, None
        for label_lower, slug in self._pool:
            score = fuzz.token_sort_ratio(name_lower, label_lower)
            if score > best_score:
                best_score = score
                best_slug  = slug

        if best_score >= self.threshold and best_slug:
            return _to_iri(best_slug)

        return None

    def split_and_resolve(self, text: str) -> list[URIRef]:
        """Split text into org names and resolve each.

        Returns a deduplicated list of canonical org IRIs.
        """
        results: list[URIRef] = []
        seen:    set[URIRef]  = set()
        for name in self.split_orgs(text):
            iri = self.resolve(name)
            if iri is not None and iri not in seen:
                results.append(iri)
                seen.add(iri)
        return results


ORG_RESOLVER = OrgResolver()
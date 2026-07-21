"""Persist and query deterministic cross-source cluster registries."""

from ._resolver import get_known_pairs, load_registry, save_registry

__all__ = ["get_known_pairs", "load_registry", "save_registry"]

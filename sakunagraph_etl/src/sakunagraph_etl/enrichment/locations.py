"""Unified location-resolution interface with source-specific strategies."""

from __future__ import annotations

from enum import Enum
from typing import Protocol, runtime_checkable

from . import _locations_single as single
from ._locations_hierarchical import LOCATION_MATCHER, LocationMatcher


class LocationStrategy(str, Enum):
    SINGLE = "single"
    HIERARCHICAL = "hierarchical"


@runtime_checkable
class LocationResolutionService(Protocol):
    def match_cell(self, value: str, *, threshold: int = 80) -> list[str]: ...


class SingleLocationService:
    """Resolve cells independently against the complete PSGC label pool."""

    def match_cell(self, value: str, *, threshold: int = 80) -> list[str]:
        match = single.match_location(value, threshold=threshold)
        return [] if match is None else [str(match.iri)]


class HierarchicalLocationService:
    """Resolve region/province/municipality hierarchies using the v2 matcher."""

    def __init__(self, matcher: LocationMatcher = LOCATION_MATCHER) -> None:
        self.matcher = matcher

    def match_cell(self, value: str, *, threshold: int = 80) -> list[str]:
        del threshold  # the established hierarchical strategy owns its thresholds
        return self.matcher.match_cell(value)


SINGLE_LOCATION_SERVICE = SingleLocationService()
HIERARCHICAL_LOCATION_SERVICE = HierarchicalLocationService()


def get_location_service(
    strategy: str | LocationStrategy,
) -> LocationResolutionService:
    selected = strategy if isinstance(strategy, LocationStrategy) else LocationStrategy(strategy)
    if selected is LocationStrategy.SINGLE:
        return SINGLE_LOCATION_SERVICE
    return HIERARCHICAL_LOCATION_SERVICE


LocationMatch = single.LocationMatch
canonicalize_column = single.canonicalize_column
match_location = single.match_location

__all__ = [
    "HIERARCHICAL_LOCATION_SERVICE",
    "HierarchicalLocationService",
    "LOCATION_MATCHER",
    "LocationMatch",
    "LocationMatcher",
    "LocationResolutionService",
    "LocationStrategy",
    "SINGLE_LOCATION_SERVICE",
    "SingleLocationService",
    "canonicalize_column",
    "get_location_service",
    "match_location",
]

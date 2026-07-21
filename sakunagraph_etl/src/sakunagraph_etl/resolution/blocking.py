"""Generate deterministic cross-source candidate blocks."""

from ._resolver import (
    BLOCKING_DATE_TOLERANCE_DAYS,
    BLOCKING_LOCATION_PREFIX_LEN,
    assign_blocking_keys,
    blocking_stats,
    generate_blocking_keys,
    generate_candidate_pairs,
    location_token,
    normalize_text,
    year_windows,
)

__all__ = [
    "BLOCKING_DATE_TOLERANCE_DAYS",
    "BLOCKING_LOCATION_PREFIX_LEN",
    "assign_blocking_keys",
    "blocking_stats",
    "generate_blocking_keys",
    "generate_candidate_pairs",
    "location_token",
    "normalize_text",
    "year_windows",
]

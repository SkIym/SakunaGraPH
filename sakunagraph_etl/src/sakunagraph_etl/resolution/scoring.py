"""Score candidate event pairs using the established resolution gates."""

from ._resolver import (
    DATE_HARD_GATE_DAYS,
    DATE_HARD_GATE_DAYS_INCIDENT,
    LABEL_FUZZY_THRESHOLD,
    PSGC_MATCH_LEVELS,
    ScoreBreakdown,
    score_all_pairs,
    score_pair,
)

__all__ = [
    "DATE_HARD_GATE_DAYS",
    "DATE_HARD_GATE_DAYS_INCIDENT",
    "LABEL_FUZZY_THRESHOLD",
    "PSGC_MATCH_LEVELS",
    "ScoreBreakdown",
    "score_all_pairs",
    "score_pair",
]

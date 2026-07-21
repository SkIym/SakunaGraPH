import calendar
import re
from datetime import date, datetime
from typing import Any


_MONTHS = {
    name.casefold(): number
    for number, name in enumerate(calendar.month_name)
    if name
}
_MONTHS.update({
    name.casefold(): number
    for number, name in enumerate(calendar.month_abbr)
    if name
})
_MONTH_PATTERN = "|".join(
    sorted((re.escape(name) for name in _MONTHS), key=len, reverse=True)
)
_ISO_DATE_RE = re.compile(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)")
_MONTH_RANGE_RE = re.compile(
    rf"\b({_MONTH_PATTERN})\s+(?:through|thru|to|until|-)\s+"
    rf"({_MONTH_PATTERN})\s+(19\d{{2}}|20\d{{2}}|2100)\b",
    re.IGNORECASE,
)
_MONTH_YEAR_RE = re.compile(
    rf"\b({_MONTH_PATTERN})\s+(19\d{{2}}|20\d{{2}}|2100)\b",
    re.IGNORECASE,
)
_YEAR_RANGE_RE = re.compile(
    r"\b(19\d{2}|20\d{2}|2100)\s+(?:through|thru|to|until|-)\s+"
    r"(19\d{2}|20\d{2}|2100)\b",
    re.IGNORECASE,
)
_YEAR_ONWARD_RE = re.compile(
    r"\b(?:from|since|after)\s+(?:the\s+year\s+)?"
    r"(19\d{2}|20\d{2}|2100)\s*(?:onwards?|forward)?\b",
    re.IGNORECASE,
)
_TEMPORAL_YEAR_RE = re.compile(
    r"\b(?:in|during|throughout|for|within)\s+(?:the\s+year\s+)?"
    r"(19\d{2}|20\d{2}|2100)\b|"
    r"\b(?:year)\s+(19\d{2}|20\d{2}|2100)\b",
    re.IGNORECASE,
)

_METRIC_ALIASES = {
    "event": "events",
    "events": "events",
    "event count": "events",
    "number of events": "events",
    "dead": "dead",
    "death": "dead",
    "deaths": "dead",
    "fatality": "dead",
    "fatalities": "dead",
    "killed": "dead",
    "injured": "injured",
    "injury": "injured",
    "injuries": "injured",
    "wounded": "injured",
    "missing": "missing",
    "unaccounted for": "missing",
    "affected person": "affected_persons",
    "affected persons": "affected_persons",
    "affected people": "affected_persons",
    "affected population": "affected_persons",
    "displaced person": "affected_persons",
    "displaced persons": "affected_persons",
    "affected family": "affected_families",
    "affected families": "affected_families",
    "displaced family": "affected_families",
    "displaced families": "affected_families",
    "damage": "damage",
    "damages": "damage",
    "damage amount": "damage",
    "loss": "damage",
    "losses": "damage",
    "cost": "damage",
}
_QUESTION_METRICS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "dead",
        (r"\bdead\b", r"\bdeaths?\b", r"\bfatalit(?:y|ies)\b", r"\bkilled\b"),
    ),
    ("injured", (r"\binjur(?:ed|y|ies)\b", r"\bwounded\b")),
    ("missing", (r"\bmissing\b", r"\bunaccounted\s+for\b")),
    (
        "affected_persons",
        (
            r"\baffected\s+(?:people|persons?|population)\b",
            r"\bdisplaced[-\s]+persons?\b",
        ),
    ),
    (
        "affected_families",
        (
            r"\baffected[-\s]+famil(?:y|ies)\b",
            r"\bdisplaced[-\s]+famil(?:y|ies)\b",
        ),
    ),
    (
        "damage",
        (
            r"\bdamag(?:e|es|ed)\b",
            r"\bproduction[-\s]+loss\b",
            r"\bloss(?:es)?\s+(?:amount|cost|volume)\b",
        ),
    ),
)


def _month_number(value: str) -> int:
    return _MONTHS[value.casefold()]


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _coerce_date(value: Any, *, boundary: str) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, int) and 1900 <= value <= 2100:
        return date(value, 1, 1) if boundary == "start" else date(value, 12, 31)
    if not isinstance(value, str):
        raise ValueError(f"{boundary}_date must be a date, year, or null.")

    text = value.strip()
    if text.casefold() in {"none", "null", "n/a"}:
        return None
    if re.fullmatch(r"(?:19\d{2}|20\d{2}|2100)", text):
        year = int(text)
        return date(year, 1, 1) if boundary == "start" else date(year, 12, 31)
    iso_match = re.match(r"^(\d{4})-(\d{2})-(\d{2})", text)
    if iso_match:
        return date(*(int(part) for part in iso_match.groups()))
    month_match = _MONTH_YEAR_RE.fullmatch(text)
    if month_match:
        month = _month_number(month_match.group(1))
        year = int(month_match.group(2))
        return date(year, month, 1) if boundary == "start" else _month_end(year, month)
    raise ValueError(f"{boundary}_date is not a supported calendar value.")


def normalize_date_range(
    question: str,
    raw_start: Any,
    raw_end: Any,
    *,
    today: date,
) -> tuple[date | None, date | None]:
    """Normalize calendar expressions without asking the model to do date math."""
    normalized_question = question.strip()
    lowered = normalized_question.casefold()

    iso_dates = [
        date(*(int(part) for part in match.groups()))
        for match in _ISO_DATE_RE.finditer(normalized_question)
    ]
    if len(iso_dates) >= 2:
        return iso_dates[0], iso_dates[1]
    if len(iso_dates) == 1:
        if re.search(r"\b(?:until|through|before|ending)\b", lowered):
            return None, iso_dates[0]
        return iso_dates[0], None

    month_range = _MONTH_RANGE_RE.search(normalized_question)
    if month_range:
        start_month = _month_number(month_range.group(1))
        end_month = _month_number(month_range.group(2))
        year = int(month_range.group(3))
        if end_month < start_month:
            raise ValueError("A month range cannot end before it starts within one year.")
        return date(year, start_month, 1), _month_end(year, end_month)

    year_range = _YEAR_RANGE_RE.search(normalized_question)
    if year_range:
        start_year, end_year = (int(value) for value in year_range.groups())
        if end_year < start_year:
            raise ValueError("A year range cannot end before it starts.")
        return date(start_year, 1, 1), date(end_year, 12, 31)

    if "last year" in lowered:
        year = today.year - 1
        return date(year, 1, 1), date(year, 12, 31)
    if "this year" in lowered or "current year" in lowered:
        return date(today.year, 1, 1), today
    if "today" in lowered:
        return today, today

    onward = _YEAR_ONWARD_RE.search(normalized_question)
    if onward:
        return date(int(onward.group(1)), 1, 1), None

    temporal_year = _TEMPORAL_YEAR_RE.search(normalized_question)
    if temporal_year:
        year = int(next(value for value in temporal_year.groups() if value))
        return date(year, 1, 1), date(year, 12, 31)

    month_year = _MONTH_YEAR_RE.search(normalized_question)
    if month_year and re.search(
        r"\b(?:in|during|from|through|within|month)\b", lowered
    ):
        month = _month_number(month_year.group(1))
        year = int(month_year.group(2))
        return date(year, month, 1), _month_end(year, month)

    return (
        _coerce_date(raw_start, boundary="start"),
        _coerce_date(raw_end, boundary="end"),
    )


def _normalize_metric_value(raw_metric: Any) -> str | None:
    if raw_metric is None or raw_metric == "":
        return None
    if not isinstance(raw_metric, str):
        raise ValueError("metric must be a supported string or null.")
    key = re.sub(r"[_-]+", " ", raw_metric.strip().casefold())
    key = re.sub(r"\s+", " ", key)
    if key not in _METRIC_ALIASES:
        raise ValueError(f"Unsupported metric: {raw_metric!r}.")
    return _METRIC_ALIASES[key]


def normalize_metric(question: str, raw_metric: Any, *, intent: Any) -> str | None:
    """Ground metric aliases in question text, resolving them to the plan vocabulary."""
    lowered = question.casefold()
    intent_value = str(intent or "")

    if intent_value == "event_count":
        return "events"
    if intent_value not in {
        "impact_summary",
        "victim_trend",
        "region_ranking",
        "disaster_ranking",
    }:
        return _normalize_metric_value(raw_metric)

    matches: list[tuple[int, str]] = []
    for metric, patterns in _QUESTION_METRICS:
        for pattern in patterns:
            match = re.search(pattern, lowered)
            if match:
                matches.append((match.start(), metric))
                break

    # When both family and person totals are requested, the more inclusive
    # person metric is the primary scalar supported by the current contract.
    matched_metrics = {metric for _, metric in matches}
    if {"affected_persons", "affected_families"} <= matched_metrics:
        return "affected_persons"
    if matches:
        return min(matches, key=lambda item: item[0])[1]
    return _normalize_metric_value(raw_metric)


def normalize_plan_payload(
    question: str,
    payload: dict[str, Any],
    *,
    today: date,
) -> dict[str, Any]:
    normalized = dict(payload)
    start_date, end_date = normalize_date_range(
        question,
        normalized.get("start_date"),
        normalized.get("end_date"),
        today=today,
    )
    normalized["start_date"] = start_date
    normalized["end_date"] = end_date
    normalized["metric"] = normalize_metric(
        question,
        normalized.get("metric"),
        intent=normalized.get("intent"),
    )
    return normalized

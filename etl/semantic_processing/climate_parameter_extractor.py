from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from dateutil.parser import ParserError, parse
from rdflib import Graph, Namespace, RDF, URIRef

SKG = Namespace("https://sakuna.ph/")
BAW = Namespace("https://raw.githubusercontent.com/beAWARE-project/ontology/master/beAWARE_ontology#")

ONTOLOGY_PATH = str(Path(__file__).resolve().parents[2] / "ontology" / "sakunagraph.ttl")
DEFAULT_MODEL = "fastino/gliner2-base-v1"

CLIMATE_PARAMETER_QUERY = """
SELECT DISTINCT ?param WHERE {
  ?param a baw:ClimateParameterType .
}
"""

PARAMETER_ALIASES: dict[str, tuple[str, ...]] = {
    "Temperature": ("temperature", "temp", "celsius", "degree celsius", "degrees celsius"),
    "Magnitude": ("magnitude", "earthquake magnitude"),
    "MagnitudeScale": ("magnitude scale", "richter", "mw", "msf"),
    "WindSpeed": (
        "wind speed",
        "winds",
        "maximum sustained winds",
        "gustiness",
        "gust",
        "kph",
        "km/h",
        "m/s",
    ),
    "Precipitation": ("rainfall", "rain", "precipitation", "accumulated rainfall", "mm"),
    "Humidity": ("humidity", "relative humidity"),
    "AtmosphericPressure": ("pressure", "atmospheric pressure", "central pressure", "hpa", "mb"),
    "Depth": ("depth", "km depth", "focal depth"),
    "Intensity": ("intensity", "signal", "tcws", "tropical cyclone wind signal"),
    "FloodDepth": ("flood depth", "floodwater", "flood water", "water level"),
    "WaveHeight": ("wave height", "tsunami wave height"),
}

WARNING_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "TCWS",
        re.compile(
            r"\b(?:(?:Tropical\s+Cyclone\s+Wind\s+Signal)|TCWS)"
            r"\s*(?:no\.?|number|#)?\s*(?P<level>\d+[A-Za-z]?)\b",
            flags=re.IGNORECASE,
        ),
    ),
    (
        "Public Storm Signal",
        re.compile(
            r"\b(?:Public\s+Storm\s+Warning\s+Signal|Public\s+Storm\s+Signal|Storm\s+Signal|Typhoon\s+Signal)"
            r"\s*(?:no\.?|number|#)?\s*(?P<level>\d+[A-Za-z]?)\b",
            flags=re.IGNORECASE,
        ),
    ),
    (
        "Alert Level",
        re.compile(
            r"\b(?:Alert\s+Level|Alert\s+Status)"
            r"\s*(?:no\.?|number|#)?\s*(?P<level>\d+[A-Za-z]?)\b",
            flags=re.IGNORECASE,
        ),
    ),
    (
        "Rainfall Warning",
        re.compile(
            r"\b(?:(?P<color>yellow|orange|red)\s+)?"
            r"(?:rainfall\s+warning|rainfall\s+advisory|heavy\s+rainfall\s+warning)\b",
            flags=re.IGNORECASE,
        ),
    ),
    (
        "Flood Warning",
        re.compile(
            r"\b(?:flood\s+warning|flood\s+advisory|flood\s+alert)\b",
            flags=re.IGNORECASE,
        ),
    ),
    (
        "Gale Warning",
        re.compile(
            r"\b(?:gale\s+warning|gale\s+warning\s+advisory)\b",
            flags=re.IGNORECASE,
        ),
    ),
    (
        "Storm Surge Warning",
        re.compile(
            r"\b(?:storm\s+surge\s+warning|storm\s+surge\s+advisory)\b",
            flags=re.IGNORECASE,
        ),
    ),
    (
        "Lahar Advisory",
        re.compile(
            r"\b(?:lahar\s+advisory|lahar\s+warning|lahar\s+alert)\b",
            flags=re.IGNORECASE,
        ),
    ),
)

EARTHQUAKE_MAGNITUDE_SCHEMA = {
    "earthquake_magnitudes": [
        "value::str::Single earthquake magnitude number. Include mainshock and aftershock magnitudes, including coordinated values such as 6.2 in 'magnitude 6.4 and 6.2'. Exclude magnitude ranges such as '1.2 - 6.6', counts, dates, times, and older values in phrases such as 'as updated from 6.1'.",
        "scale::str::Magnitude scale only if explicitly stated, such as Mw, ML, Ms, mb.",
    ]
}

EARTHQUAKE_DEPTH_SCHEMA = {
    "earthquake_depths": [
        "value::str::Single earthquake focal depth number. Include depth in phrases such as 'estimated depth of 26 kilometers'. Exclude epicenter distance such as '29 kilometers northeast'.",
        "unit::str::Depth unit only if explicitly stated, such as kilometers or km.",
    ]
}

TIMESTAMP_PATTERN = re.compile(
    r"\b(?:as\s+of\s+)?"
    r"(?:(?:on\s+)?(?P<date1>\d{1,2}\s+"
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|"
    r"sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s+\d{4})"
    r"(?:,?\s*(?:at\s+|around\s+)?)?"
    r"(?P<time1>\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm|H|h)?)?"
    r"|(?P<time2>\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm|H|h))"
    r",?\s*(?P<date2>\d{1,2}\s+"
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|"
    r"sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s+\d{4})"
    r"|(?P<date3>"
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|"
    r"sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s+\d{1,2},?\s+\d{4})"
    r"\s+(?P<time3>\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm|H|h))"
    r")\b",
    flags=re.IGNORECASE,
)

MONTH_PATTERN = (
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|"
    r"sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
)

CONTEXT_DATE_PATTERN = re.compile(
    rf"\b(?:on\s+)?(?P<date>"
    rf"(?:\d{{1,2}}\s+{MONTH_PATTERN}\s+\d{{4}}|{MONTH_PATTERN}\s+\d{{1,2}},?\s+\d{{4}})"
    r")"
    r"(?:,?\s*(?:at\s+|around\s+)?)?"
    r"(?P<time>\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm|H|h)?)?",
    flags=re.IGNORECASE,
)

CONTEXT_TIME_PATTERN = re.compile(
    r"\b(?:at|around|by|as\s+of)\s+"
    r"(?P<time>\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm|H|h))\b",
    flags=re.IGNORECASE,
)


@dataclass
class ExtractedClimateParameter:
    parameter: str | None
    parameterText: str | None
    value: float | None
    unit: str | None
    location: str | None


@dataclass
class ExtractedWarning:
    warningReleased: str
    warningTimeStamp: datetime | None


def _local_name(uri: URIRef) -> str:
    text = str(uri)
    if "#" in text:
        return text.rsplit("#", 1)[1]
    return text.rstrip("/").rsplit("/", 1)[1]


def _normalize(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip().lower())


def _parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).replace(",", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    return float(match.group(0)) if match else None


def _normalize_unit(value: str | None) -> str | None:
    if not value:
        return None

    unit = value.strip()
    unit_lower = unit.lower()
    if unit_lower in {"kilometer", "kilometers", "kilometre", "kilometres"}:
        return "km"
    if unit_lower in {"meter", "meters", "metre", "metres"}:
        return "m"
    if unit_lower in {"mw", "ml", "mb", "ms"}:
        return unit.title()
    return unit


def _json_items(result: Any, key: str) -> list[dict[str, Any]]:
    if not isinstance(result, dict):
        return []

    raw_items = result.get(key)
    if isinstance(raw_items, dict):
        raw_items = [raw_items]
    if not isinstance(raw_items, list):
        return []

    return [item for item in raw_items if isinstance(item, dict)]


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    has_year = re.search(r"\b\d{4}\b", text)
    has_month = re.search(
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\b",
        text,
        flags=re.IGNORECASE,
    )
    if not (has_year or has_month):
        return None

    try:
        return parse(text, fuzzy=True)
    except (ParserError, OverflowError, ValueError):
        return None


def _looks_like_range_or_old_value(value: Any) -> bool:
    text = str(value or "")
    if re.search(r"\d+(?:\.\d+)?\s*[-–]\s*\d+(?:\.\d+)?", text):
        return True
    return "updated from" in text.lower()


def _extract_earthquake_parameters(extractor: Any, text: str) -> list[ExtractedClimateParameter]:
    extracted: list[ExtractedClimateParameter] = []
    seen: dict[tuple[str, float], ExtractedClimateParameter] = {}

    def add_item(parameter: str, parameter_text: str, value: float, unit: str | None) -> None:
        item = ExtractedClimateParameter(
            parameter=parameter,
            parameterText=parameter_text,
            value=value,
            unit=unit,
            location=None,
        )
        key = (parameter, value)
        existing = seen.get(key)
        if existing:
            if existing.unit is None and item.unit:
                existing.unit = item.unit
            return

        seen[key] = item
        extracted.append(item)

    magnitude_result = extractor.extract_json(text, EARTHQUAKE_MAGNITUDE_SCHEMA)
    for item in _json_items(magnitude_result, "earthquake_magnitudes"):
        raw_value = item.get("value")
        if _looks_like_range_or_old_value(raw_value):
            continue

        value = _parse_number(raw_value)
        if value is None:
            continue

        add_item("Magnitude", "magnitude", value, _normalize_unit(item.get("scale")))

    depth_result = extractor.extract_json(text, EARTHQUAKE_DEPTH_SCHEMA)
    for item in _json_items(depth_result, "earthquake_depths"):
        raw_value = item.get("value")
        if _looks_like_range_or_old_value(raw_value):
            continue

        value = _parse_number(raw_value)
        if value is None:
            continue

        add_item("Depth", "depth", value, _normalize_unit(item.get("unit")))

    return extracted


def _parse_context_date(value: str | None) -> date | None:
    if not value:
        return None

    try:
        return parse(value, fuzzy=True).date()
    except (ParserError, OverflowError, ValueError):
        return None


def _parse_context_time(value: str | None, base_date: date | None) -> datetime | None:
    if not value or base_date is None:
        return None

    text = value.strip()
    hour_match = re.fullmatch(r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*[Hh]", text)
    if hour_match:
        hour = int(hour_match.group("hour"))
        minute = int(hour_match.group("minute") or 0)
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return datetime(base_date.year, base_date.month, base_date.day, hour, minute)
        return None

    try:
        parsed = parse(
            f"{base_date.isoformat()} {text}",
            fuzzy=True,
            default=datetime(base_date.year, base_date.month, base_date.day),
        )
        return parsed.replace(year=base_date.year, month=base_date.month, day=base_date.day)
    except (ParserError, OverflowError, ValueError):
        return None


def _context_timestamp_before(text: str, offset: int) -> datetime | None:
    current_date: date | None = None
    current_timestamp: datetime | None = None
    markers: list[tuple[int, str, re.Match[str]]] = []

    prefix = text[:offset]
    markers.extend((match.start(), "date", match) for match in CONTEXT_DATE_PATTERN.finditer(prefix))
    markers.extend((match.start(), "time", match) for match in CONTEXT_TIME_PATTERN.finditer(prefix))

    for _, marker_type, match in sorted(markers, key=lambda item: item[0]):
        if marker_type == "date":
            parsed_date = _parse_context_date(match.group("date"))
            if not parsed_date:
                continue

            current_date = parsed_date
            parsed_time = _parse_context_time(match.groupdict().get("time"), current_date)
            current_timestamp = parsed_time or datetime(
                current_date.year,
                current_date.month,
                current_date.day,
            )
            continue

        parsed_time = _parse_context_time(match.group("time"), current_date)
        if parsed_time:
            current_timestamp = parsed_time

    return current_timestamp


def _nearest_timestamp(text: str, start: int, end: int) -> datetime | None:
    left_boundary = max(text.rfind(".", 0, start), text.rfind("\n", 0, start), text.rfind(";", 0, start))
    right_candidates = [
        pos for pos in (
            text.find(".", end),
            text.find("\n", end),
            text.find(";", end),
        )
        if pos != -1
    ]
    window_start = left_boundary + 1 if left_boundary != -1 else max(0, start - 180)
    window_end = min(right_candidates) if right_candidates else min(len(text), end + 120)
    window = text[window_start:window_end]
    local_start = start - window_start

    candidates: list[tuple[int, datetime]] = []
    for match in TIMESTAMP_PATTERN.finditer(window):
        timestamp = _parse_datetime(match.group(0))
        if not timestamp:
            continue

        if match.end() <= local_start:
            distance = local_start - match.end()
        elif match.start() >= local_start:
            distance = match.start() - local_start
        else:
            distance = 0
        candidates.append((distance, timestamp))

    if not candidates:
        return None

    return min(candidates, key=lambda item: item[0])[1]


def _contains_term(text: str, term: str) -> bool:
    if re.fullmatch(r"[a-z0-9]+", term) and len(term) <= 3:
        return re.search(rf"\b{re.escape(term)}\b", text) is not None
    return term in text


def _expand_warning_level(warning: str, context: str) -> str:
    warning = re.sub(r"\s+", " ", warning).strip()
    if not warning:
        return warning

    already_has_level = re.search(
        r"\b(?:no\.?|number|level|signal)\s*[-:]?\s*\d+[A-Za-z]?\b|\b\d+[A-Za-z]?\b",
        warning,
        flags=re.IGNORECASE,
    )
    if already_has_level:
        return warning

    pattern = re.compile(
        rf"{re.escape(warning)}"
        r"(?:\s*(?:no\.?|number|level|signal))?"
        r"\s*[-:]?\s*(?P<level>\d+[A-Za-z]?)",
        flags=re.IGNORECASE,
    )
    match = pattern.search(context)
    if match:
        if re.search(r"\bsignal\b", warning, flags=re.IGNORECASE):
            prefix = "No. "
        elif re.search(r"\blevel\b", warning, flags=re.IGNORECASE):
            prefix = ""
        else:
            prefix = "Level "
        return f"{warning} {prefix}{match.group('level')}"

    reverse_pattern = re.compile(
        r"(?P<prefix>no\.?|number|level|signal)?\s*[-:]?\s*(?P<level>\d+[A-Za-z]?)"
        rf"\s+{re.escape(warning)}",
        flags=re.IGNORECASE,
    )
    reverse_match = reverse_pattern.search(context)
    if reverse_match:
        if re.search(r"\bsignal\b", warning, flags=re.IGNORECASE):
            prefix = "No. "
        elif re.search(r"\blevel\b", warning, flags=re.IGNORECASE):
            prefix = ""
        else:
            prefix = "Level "
        return f"{warning} {prefix}{reverse_match.group('level')}"

    return warning


def _rule_warnings(text: str) -> list[ExtractedWarning]:
    warnings: list[ExtractedWarning] = []

    for label, pattern in WARNING_PATTERNS:
        for match in pattern.finditer(text):
            level = match.groupdict().get("level")
            color = match.groupdict().get("color")

            if label == "TCWS":
                warning = f"TCWS No. {level}"
            elif label == "Public Storm Signal":
                warning = f"Public Storm Signal No. {level}"
            elif label == "Alert Level":
                warning = f"Alert Level {level}"
            elif color:
                warning = f"{color.title()} {label}"
            else:
                warning = label

            warnings.append(
                ExtractedWarning(
                    warningReleased=warning,
                    warningTimeStamp=(
                        _nearest_timestamp(text, match.start(), match.end())
                        or _context_timestamp_before(text, match.start())
                    ),
                )
            )

    return warnings


class ClimateParameterExtractor:
    def __init__(
        self,
        model_name: str = DEFAULT_MODEL,
        ontology_path: str = ONTOLOGY_PATH,
    ) -> None:
        self.model_name = os.getenv("GLINER2_MODEL", model_name)
        self.ontology_path = ontology_path
        self._extractor: Any | None = None
        self.parameter_types = self._load_parameter_types()

    def _load_parameter_types(self) -> set[str]:
        graph = Graph()
        graph.parse(Path(self.ontology_path))
        return {
            _local_name(row.param)
            for row in graph.query(
                CLIMATE_PARAMETER_QUERY,
                initNs={"baw": BAW, "rdf": RDF},
            )
        }

    def _load_extractor(self) -> Any:
        if self._extractor is not None:
            return self._extractor

        try:
            from gliner2 import GLiNER2
        except ImportError as exc:
            raise RuntimeError(
                "GLiNER2 is required for NDRRMC climate parameter extraction. "
                "Install it with `pip install gliner2[local]`."
            ) from exc

        self._extractor = GLiNER2.from_pretrained(self.model_name)
        return self._extractor

    def classify_parameter(
        self,
        parameter_text: str | None,
        unit: str | None = None,
        context: str | None = None,
    ) -> str | None:
        haystack = _normalize(" ".join(x for x in [parameter_text, unit, context] if x))
        if not haystack:
            return None

        best_label: str | None = None
        best_score = 0
        for label in self.parameter_types:
            aliases = PARAMETER_ALIASES.get(label, ())
            candidates = (label,) + aliases
            for candidate in candidates:
                candidate_norm = _normalize(candidate)
                if not candidate_norm:
                    continue
                if _contains_term(haystack, candidate_norm):
                    score = len(candidate_norm)
                    if score > best_score:
                        best_label = label
                        best_score = score

        return best_label

    def extract(self, text: str) -> list[ExtractedClimateParameter]:
        if not text.strip():
            return []

        extractor = self._load_extractor()
        extracted = _extract_earthquake_parameters(extractor, text)
        seen: set[tuple[str | None, float | None, str | None, str | None]] = {
            (item.parameter, item.value, item.unit, item.location)
            for item in extracted
        }
        targeted_scalar_seen: set[tuple[str | None, float | None]] = {
            (item.parameter, item.value)
            for item in extracted
            if item.parameter in {"Magnitude", "Depth"}
        }

        result = extractor.extract_json(
            text,
            {
                "climate_parameters": [
                    "parameter::str::Climate parameter name or measurement type, such as temperature, magnitude, wind speed, precipitation, humidity, intensity",
                    "value::str::Numeric measured value",
                    "unit::str::Measurement unit",
                    "location::str::Location where the measurement applies",
                ]
            },
        )

        raw_items = _json_items(result, "climate_parameters")
        if not raw_items:
            return extracted

        for item in raw_items:
            parameter_text = item.get("parameter")
            unit = item.get("unit")
            location = item.get("location")
            parameter = self.classify_parameter(parameter_text, unit, text)

            if not any((parameter, parameter_text, item.get("value"), unit, location)):
                continue

            value = _parse_number(item.get("value"))
            normalized_unit = _normalize_unit(str(unit).strip() if unit else None)
            normalized_location = str(location).strip() if location else None
            key = (parameter, value, normalized_unit, normalized_location)
            if key in seen or (parameter, value) in targeted_scalar_seen:
                continue
            seen.add(key)

            extracted.append(
                ExtractedClimateParameter(
                    parameter=parameter,
                    parameterText=str(parameter_text).strip() if parameter_text else None,
                    value=value,
                    unit=normalized_unit,
                    location=normalized_location,
                )
            )

        return extracted

    def extract_warnings(self, text: str) -> list[ExtractedWarning]:
        if not text.strip():
            return []

        warnings: list[ExtractedWarning] = []
        seen: set[tuple[str, datetime | None]] = set()

        for warning in _rule_warnings(text):
            key = (warning.warningReleased.lower(), warning.warningTimeStamp)
            if key in seen:
                continue
            seen.add(key)
            warnings.append(warning)

        extractor = self._load_extractor()
        result = extractor.extract_json(
            text,
            {
                "warnings": [
                    "warning_released::str::Released disaster warning, alert level, Tropical Cyclone Wind Signal, rainfall warning, flood warning, storm surge warning, lahar alert, or similar warning",
                    "warning_timestamp::str::Date or time when the warning was issued, raised, released, or in effect",
                ]
            },
        )

        raw_items = result.get("warnings") if isinstance(result, dict) else None
        if isinstance(raw_items, dict):
            raw_items = [raw_items]
        if not isinstance(raw_items, list):
            return warnings

        for item in raw_items:
            if not isinstance(item, dict):
                continue

            raw_warning = str(item.get("warning_released") or "")
            warning = _expand_warning_level(raw_warning, text)
            if not warning:
                continue

            warning_pos = text.lower().find(raw_warning.strip().lower()) if raw_warning.strip() else -1
            timestamp = _parse_datetime(item.get("warning_timestamp"))
            if timestamp is None and warning_pos != -1:
                timestamp = _context_timestamp_before(text, warning_pos)

            key = (warning.lower(), timestamp)
            if key in seen:
                continue
            seen.add(key)

            warnings.append(
                ExtractedWarning(
                    warningReleased=warning,
                    warningTimeStamp=timestamp,
                )
            )

        return warnings


PARAMS_EXTRACTOR = ClimateParameterExtractor()

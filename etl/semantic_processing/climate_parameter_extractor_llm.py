from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import requests
from dateutil.parser import ParserError, parse

Provider = Literal["ollama", "openai_compatible", "lmstudio"]

DEFAULT_PROVIDER = "lmstudio"

ALLOWED_PARAMETERS = {
    "Temperature",
    "Magnitude",
    "MagnitudeScale",
    "WindSpeed",
    "Precipitation",
    "Humidity",
    "AtmosphericPressure",
    "Depth",
    "Intensity",
    "FloodDepth",
    "WaveHeight",
}

PARAMETER_ALIASES = {
    "temperature": "Temperature",
    "magnitude": "Magnitude",
    "earthquake magnitude": "Magnitude",
    "magnitude scale": "MagnitudeScale",
    "wind speed": "WindSpeed",
    "winds": "WindSpeed",
    "rainfall": "Precipitation",
    "rain": "Precipitation",
    "precipitation": "Precipitation",
    "humidity": "Humidity",
    "pressure": "AtmosphericPressure",
    "atmospheric pressure": "AtmosphericPressure",
    "depth": "Depth",
    "focal depth": "Depth",
    "intensity": "Intensity",
    "flood depth": "FloodDepth",
    "water level": "FloodDepth",
    "wave height": "WaveHeight",
    "tsunami wave height": "WaveHeight",
}

SYSTEM_PROMPT = """You extract disaster measurements and warning releases from Philippine disaster report remarks.

Return only valid JSON with this exact top-level shape:
{
  "climate_parameters": [
    {
      "parameter": "Magnitude|MagnitudeScale|Depth|WindSpeed|Precipitation|Temperature|Humidity|AtmosphericPressure|Intensity|FloodDepth|WaveHeight",
      "parameter_text": "short source concept name, not a full sentence",
      "value": 7.4,
      "unit": "Mw",
      "location": null
    }
  ],
  "warnings": [
    {
      "type": "Tsunami Warning",
      "signal": null,
      "timestamp": "2023-12-02T22:37:00"
    }
  ]
}

Rules:
- Extract scalar measurements only. Do not invent values.
- Exclude counts, dates, times, distances, coordinates, costs, family/person totals, and plotted/felt/recorded counts.
- Exclude ranges unless the text gives a single scalar value. For example, skip "Magnitude Range: 1.2 - 6.6".
- For earthquakes, extract magnitude values and focal depth. Do not treat epicenter distance as depth.
- For tsunami text, extract wave heights as WaveHeight, not Depth.
- For warnings, return only type, signal, and timestamp. Use the nearest explicit or inherited narrative timestamp when the warning sentence omits its own date/time.
- Use null when a field is unknown. Return empty arrays when nothing is present.
"""


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


def _parse_json_object(text: str) -> dict[str, Any]:
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return {}

    try:
        value = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                value = item.get("text") or item.get("content")
                if value is not None:
                    parts.append(str(value))
        return "\n".join(parts)

    return str(content) if content is not None else ""


def _message_content_from_output(output: Any) -> str:
    if isinstance(output, dict):
        return _content_to_text(output.get("content"))

    if not isinstance(output, list):
        return ""

    messages: list[str] = []
    for item in output:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue

        content = _content_to_text(item.get("content"))
        if content:
            messages.append(content)

    return "\n".join(messages)


def _parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).replace(",", "").strip()
    if re.search(r"\d+(?:\.\d+)?\s*[-–]\s*\d+(?:\.\d+)?", text):
        return None

    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    return float(match.group(0)) if match else None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        return parse(text, fuzzy=True)
    except (ParserError, OverflowError, ValueError):
        return None


def _normalize_parameter(value: Any) -> str | None:
    if not value:
        return None

    text = re.sub(r"\s+", " ", str(value).strip())
    if text in ALLOWED_PARAMETERS:
        return text

    alias = PARAMETER_ALIASES.get(text.lower())
    return alias if alias in ALLOWED_PARAMETERS else None


def _normalize_unit(value: Any) -> str | None:
    if value is None:
        return None

    unit = re.sub(r"\s+", " ", str(value).strip())
    if not unit or unit.lower() in {"none", "null", "unknown", "n/a"}:
        return None

    unit_lower = unit.lower()
    if unit_lower in {"kilometer", "kilometers", "kilometre", "kilometres"}:
        return "km"
    if unit_lower in {"meter", "meters", "metre", "metres"}:
        return "m"
    if unit_lower in {"kph", "km/h", "kmh", "kilometers per hour"}:
        return "km/h"
    if unit_lower in {"millimeter", "millimeters", "mm"}:
        return "mm"
    if unit_lower in {"mw", "ml", "mb", "ms"}:
        return unit.title()
    return unit


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None

    text = re.sub(r"\s+", " ", str(value).strip())
    if not text or text.lower() in {"none", "null", "unknown", "n/a"}:
        return None
    return text


def _warning_label(item: dict[str, Any]) -> str | None:
    warning_type = _clean_text(item.get("type"))
    signal = _clean_text(item.get("signal"))
    if not warning_type:
        return signal
    if not signal:
        return warning_type

    if signal.lower() in warning_type.lower():
        return warning_type
    return f"{warning_type} {signal}"


class LLMClimateParameterExtractor:
    def __init__(
        self,
        provider: Provider | None = None,
        model: str | None = None,
        base_url: str | None = None,
        timeout: int | None = None,
        token: str | None = None,
        cache_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        self.provider: Provider = (provider or os.getenv("SAKUNA_LLM_PROVIDER") or DEFAULT_PROVIDER)  # type: ignore[assignment]
        self.model = model or os.getenv("SAKUNA_LLM_MODEL") 
        self.timeout = timeout or int(os.getenv("SAKUNA_LLM_TIMEOUT", "120"))
        self.base_url = (base_url or os.getenv("SAKUNA_LLM_BASE_URL"))
        configured_cache = cache_dir or os.getenv("SAKUNA_LLM_CACHE_DIR")
        self.cache_dir = Path(configured_cache) if configured_cache else None



    def _cache_path(self, text: str) -> Path | None:
        if not self.cache_dir:
            return None

        key = hashlib.sha256(
            f"{self.provider}\n{self.model}\n{text}".encode("utf-8")
        ).hexdigest()
        return self.cache_dir / f"{key}.json"

    def _messages(self, text: str) -> list[dict[str, str]]:
        if self.provider == "lmstudio":
            return SYSTEM_PROMPT + "Here is the text: " + text
        return [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ]

    def _call_openai_compatible(self, text: str) -> str:
        response = requests.post(
            f"{self.base_url}/chat/completions",
            json={
                "model": self.model,
                "messages": self._messages(text),
                "temperature": 0,
                "response_format": {"type": "json_object"},
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        choices = payload.get("choices") if isinstance(payload, dict) else None
        if not choices:
            return ""
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        return str(message.get("content", "")) if isinstance(message, dict) else ""

    def _call_ollama(self, text: str) -> str:
        response = requests.post(
            f"{self.base_url}/api/chat",
            json={
                "model": self.model,
                "messages": self._messages(text),
                "stream": False,
                "format": "json",
                "options": {"temperature": 0},
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        message = payload.get("message") if isinstance(payload, dict) else None
        return str(message.get("content", "")) if isinstance(message, dict) else ""
    
    def _call_lmstudio(self, text: str) -> str:
        response = requests.post(
            url=f"{self.base_url}/api/v1/chat",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            },
            json={
                "model": self.model,
                "input": self._messages(text),
                "stream": False,
                "temperature": 0,
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return ""

        content = _message_content_from_output(payload.get("output"))
        if content:
            return content

        choices = payload.get("choices")
        if choices:
            message = choices[0].get("message") if isinstance(choices[0], dict) else None
            return _content_to_text(message.get("content")) if isinstance(message, dict) else ""

        return ""

    def _raw_extract(self, text: str) -> dict[str, Any]:
        cache_path = self._cache_path(text)
        if cache_path and cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))

        if self.provider == "ollama":
            content = self._call_ollama(text)
        elif self.provider == "openai_compatible":
            content = self._call_openai_compatible(text)
        elif self.provider == "lmstudio":
            self.token = os.getenv("LM_STUDIO_TOKEN")
            content = self._call_lmstudio(text)
        else:
            raise ValueError(f"Unsupported LLM provider: {self.provider}")

        data = _parse_json_object(content)
        if cache_path:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return data

    def _climate_parameters_from_json(self, data: dict[str, Any]) -> list[ExtractedClimateParameter]:
        raw_items = data.get("climate_parameters")
        if not isinstance(raw_items, list):
            return []

        extracted: list[ExtractedClimateParameter] = []
        seen: set[tuple[str | None, float | None, str | None, str | None]] = set()
        for item in raw_items:
            if not isinstance(item, dict):
                continue

            parameter = _normalize_parameter(item.get("parameter"))
            value = _parse_number(item.get("value"))
            if not parameter or value is None:
                continue

            unit = _normalize_unit(item.get("unit"))
            location = _clean_text(item.get("location"))
            key = (parameter, value, unit, location)
            if key in seen:
                continue
            seen.add(key)

            extracted.append(
                ExtractedClimateParameter(
                    parameter=parameter,
                    parameterText=_clean_text(item.get("parameter_text")) or parameter,
                    value=value,
                    unit=unit,
                    location=location,
                )
            )

        return extracted

    def _warnings_from_json(self, data: dict[str, Any]) -> list[ExtractedWarning]:
        raw_items = data.get("warnings")
        if not isinstance(raw_items, list):
            return []

        warnings: list[ExtractedWarning] = []
        seen: set[tuple[str, datetime | None]] = set()
        for item in raw_items:
            if not isinstance(item, dict):
                continue

            label = _warning_label(item)
            if not label:
                continue

            timestamp = _parse_datetime(item.get("timestamp"))
            key = (label.lower(), timestamp)
            if key in seen:
                continue
            seen.add(key)

            warnings.append(
                ExtractedWarning(
                    warningReleased=label,
                    warningTimeStamp=timestamp,
                )
            )

        return warnings

    def extract_all(self, text: str) -> tuple[list[ExtractedClimateParameter], list[ExtractedWarning]]:
        if not text.strip():
            return [], []

        data = self._raw_extract(text)
        return self._climate_parameters_from_json(data), self._warnings_from_json(data)

    def extract(self, text: str) -> list[ExtractedClimateParameter]:
        params, _ = self.extract_all(text)
        return params

    def extract_warnings(self, text: str) -> list[ExtractedWarning]:
        _, warnings = self.extract_all(text)
        return warnings


PARAMS_EXTRACTOR = LLMClimateParameterExtractor()

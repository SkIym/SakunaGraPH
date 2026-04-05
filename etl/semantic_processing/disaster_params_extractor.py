"""Extract disaster-type specific parameters from narrative text using regex patterns.

Targets typhoon properties (wind speed, gust speed, signal number, central pressure,
storm category, international name) and earthquake properties (magnitude, depth,
epicenter coordinates) from NDRRMC situational report narratives.
"""

import re
from dataclasses import dataclass, field


@dataclass
class DisasterParams:
    """Structured disaster-specific parameters extracted from text."""
    windSpeed: float | None = None           # km/h
    gustSpeed: float | None = None           # km/h
    centralPressure: float | None = None     # hPa
    signalNumber: int | None = None          # PAGASA 1-5
    stormCategory: str | None = None         # e.g. "Typhoon", "Tropical Storm"
    internationalName: str | None = None
    magnitude: float | None = None
    earthquakeDepth: float | None = None     # km
    epicenterLatitude: float | None = None
    epicenterLongitude: float | None = None


# Storm category patterns (ordered longest-first to avoid partial matches)
STORM_CATEGORIES = [
    "Super Typhoon",
    "Typhoon",
    "Severe Tropical Storm",
    "Tropical Storm",
    "Tropical Depression",
    "Low Pressure Area",
]

PATTERNS = {
    "windSpeed": re.compile(
        r'(?:maximum\s+)?sustained\s+winds?\s+(?:of\s+)?(?:up\s+to\s+)?(\d+)\s*(?:km/?h|kph|kilometers?\s+per\s+hour)',
        re.IGNORECASE,
    ),
    "gustSpeed": re.compile(
        r'gust(?:iness|s?)?\s+(?:of\s+)?(?:up\s+to\s+)?(\d+)\s*(?:km/?h|kph|kilometers?\s+per\s+hour)',
        re.IGNORECASE,
    ),
    "centralPressure": re.compile(
        r'central\s+pressure\s+(?:of\s+)?(\d+)\s*(?:hPa|mb|mbar|millibar)',
        re.IGNORECASE,
    ),
    "signalNumber": re.compile(
        r'(?:TCWS|signal)\s+(?:no\.?\s*)?#?\s*(\d)',
        re.IGNORECASE,
    ),
    "magnitude": re.compile(
        r'magnitude\s+(?:of\s+)?(\d+\.?\d*)',
        re.IGNORECASE,
    ),
    "earthquakeDepth": re.compile(
        r'depth\s+(?:of\s+)?(\d+\.?\d*)\s*(?:km|kilometers?)',
        re.IGNORECASE,
    ),
    "epicenterLatitude": re.compile(
        r'(\d+\.?\d*)\s*°?\s*(?:N|North)\s*(?:Latitude)?',
        re.IGNORECASE,
    ),
    "epicenterLongitude": re.compile(
        r'(\d+\.?\d*)\s*°?\s*(?:E|East)\s*(?:Longitude)?',
        re.IGNORECASE,
    ),
}

# International name pattern: e.g. "International Name: HAIYAN" or "(I.N. HAIYAN)"
INTL_NAME_PATTERN = re.compile(
    r'(?:International\s+Name[:\s]+|I\.?N\.?\s+)"?([A-Z][A-Za-z]+)"?',
    re.IGNORECASE,
)


class DisasterParamsExtractor:
    """Extract disaster-specific parameters from narrative text using regex."""

    def extract(self, text: str) -> DisasterParams:
        if not text:
            return DisasterParams()

        params = DisasterParams()

        # Extract numeric parameters
        for param_name, pattern in PATTERNS.items():
            match = pattern.search(text)
            if match:
                value_str = match.group(1)
                if param_name == "signalNumber":
                    setattr(params, param_name, int(value_str))
                else:
                    setattr(params, param_name, float(value_str))

        # Extract storm category
        text_lower = text.lower()
        for category in STORM_CATEGORIES:
            if category.lower() in text_lower:
                params.stormCategory = category
                break

        # Extract international name
        intl_match = INTL_NAME_PATTERN.search(text)
        if intl_match:
            params.internationalName = intl_match.group(1).strip()

        return params

    def has_any(self, params: DisasterParams) -> bool:
        """Check if any parameter was extracted."""
        return any(
            getattr(params, f.name) is not None
            for f in params.__dataclass_fields__.values()
        )


# Module-level singleton
PARAMS_EXTRACTOR = DisasterParamsExtractor()

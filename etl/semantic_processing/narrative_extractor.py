"""Extract structured knowledge from free-text narrative fields.

Composes existing components (DisasterParamsExtractor, OrgResolver, LocationMatcher)
to extract entities, quantities, dates, locations, and disaster parameters from
remarks, incidentDescription, incidentActionsTaken, and other text fields.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime

from rdflib import URIRef


@dataclass
class ExtractionResult:
    """Structured data extracted from narrative text."""
    locations: list[URIRef] = field(default_factory=list)
    organizations: list[str] = field(default_factory=list)  # canonical org slugs
    quantities: dict[str, float] = field(default_factory=dict)  # e.g. {"families": 500}
    dates: list[datetime] = field(default_factory=list)
    monetary: list[tuple[float, str]] = field(default_factory=list)  # (amount, unit)
    disaster_params: dict = field(default_factory=dict)  # from DisasterParamsExtractor


# Quantity patterns: captures count + entity type
_QUANTITY_PATTERNS = [
    re.compile(
        r'(\d[\d,]*)\s+(families|family|persons?|individuals?|houses?|barangays?|evacuation\s+centers?|schools?)',
        re.IGNORECASE,
    ),
    re.compile(
        r'(affected|displaced|evacuated|rescued|stranded)\s+(\d[\d,]*)\s+(families|persons?|individuals?)',
        re.IGNORECASE,
    ),
]

# Monetary patterns: PhP/PHP/P amount + optional multiplier
_MONETARY_PATTERN = re.compile(
    r'(?:PhP|PHP|Php|P)\s*(\d[\d,.]*)\s*(million|billion|M|B)?',
    re.IGNORECASE,
)

# Date patterns (reuse common formats from NDRRMC parser)
_DATE_PATTERNS = [
    re.compile(
        r'\b(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})\b',
        re.IGNORECASE,
    ),
    re.compile(
        r'\b((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})\b',
        re.IGNORECASE,
    ),
]

_DATE_FORMATS = [
    "%d %B %Y", "%B %d %Y", "%B %d, %Y",
]

_MULTIPLIERS = {
    "million": 1_000_000,
    "m": 1_000_000,
    "billion": 1_000_000_000,
    "b": 1_000_000_000,
}


class NarrativeExtractor:
    """Extract structured knowledge from narrative text fields."""

    def __init__(self):
        # Lazy imports to avoid circular dependencies and heavy loading at import time
        self._location_matcher = None
        self._org_resolver = None
        self._params_extractor = None

    @property
    def location_matcher(self):
        if self._location_matcher is None:
            from semantic_processing.location_matcher_v2 import LOCATION_MATCHER
            self._location_matcher = LOCATION_MATCHER
        return self._location_matcher

    @property
    def org_resolver(self):
        if self._org_resolver is None:
            from semantic_processing.org_resolver import ORG_RESOLVER
            self._org_resolver = ORG_RESOLVER
        return self._org_resolver

    @property
    def params_extractor(self):
        if self._params_extractor is None:
            from semantic_processing.disaster_params_extractor import PARAMS_EXTRACTOR
            self._params_extractor = PARAMS_EXTRACTOR
        return self._params_extractor

    def extract(self, text: str) -> ExtractionResult:
        """Extract all structured data from a narrative text field."""
        if not text or not text.strip():
            return ExtractionResult()

        result = ExtractionResult()

        # 1. Extract disaster parameters (wind speed, magnitude, etc.)
        params = self.params_extractor.extract(text)
        if self.params_extractor.has_any(params):
            result.disaster_params = {
                k: v for k, v in params.__dict__.items() if v is not None
            }

        # 2. Extract organizations
        result.organizations = self.org_resolver.split_and_resolve(text)

        # 3. Extract quantities
        result.quantities = self._extract_quantities(text)

        # 4. Extract monetary amounts
        result.monetary = self._extract_monetary(text)

        # 5. Extract dates
        result.dates = self._extract_dates(text)

        return result

    def _extract_quantities(self, text: str) -> dict[str, float]:
        """Extract quantity + entity-type pairs from text."""
        quantities = {}

        # Pattern 1: "500 families", "2,300 persons"
        for match in _QUANTITY_PATTERNS[0].finditer(text):
            count_str = match.group(1).replace(",", "")
            entity = match.group(2).lower().rstrip("s")  # normalize plural
            if entity == "familie":
                entity = "family"
            if entity == "individual":
                entity = "person"
            try:
                quantities[entity] = float(count_str)
            except ValueError:
                pass

        # Pattern 2: "affected 500 families"
        for match in _QUANTITY_PATTERNS[1].finditer(text):
            action = match.group(1).lower()
            count_str = match.group(2).replace(",", "")
            entity = match.group(3).lower().rstrip("s")
            if entity == "familie":
                entity = "family"
            if entity == "individual":
                entity = "person"
            key = f"{action}_{entity}"
            try:
                quantities[key] = float(count_str)
            except ValueError:
                pass

        return quantities

    def _extract_monetary(self, text: str) -> list[tuple[float, str]]:
        """Extract monetary amounts with currency from text."""
        results = []

        for match in _MONETARY_PATTERN.finditer(text):
            amount_str = match.group(1).replace(",", "")
            multiplier_str = match.group(2)

            try:
                amount = float(amount_str)
            except ValueError:
                continue

            if multiplier_str:
                multiplier = _MULTIPLIERS.get(multiplier_str.lower(), 1)
                amount *= multiplier

            results.append((amount, "PHP"))

        return results

    def _extract_dates(self, text: str) -> list[datetime]:
        """Extract date values from text."""
        dates = []
        seen = set()

        for pattern in _DATE_PATTERNS:
            for match in pattern.finditer(text):
                date_str = match.group(1).strip().replace(",", "")
                if date_str in seen:
                    continue
                seen.add(date_str)

                for fmt in _DATE_FORMATS:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        dates.append(dt)
                        break
                    except ValueError:
                        continue

        return dates

    def has_any(self, result: ExtractionResult) -> bool:
        """Check if any data was extracted."""
        return bool(
            result.locations or result.organizations or result.quantities or
            result.dates or result.monetary or result.disaster_params
        )


# Module-level singleton
NARRATIVE_EXTRACTOR = NarrativeExtractor()

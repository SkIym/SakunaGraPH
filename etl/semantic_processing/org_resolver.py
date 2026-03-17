"""Extract and resolve agency/organization names to canonical IRIs.

Splits concatenated organization strings (from NDRRMC, GDA, EM-DAT sources)
into individual names and resolves them to canonical slugs for IRI generation.
"""

import re
from thefuzz import fuzz


# Canonical organization dictionary: alias → canonical slug
# The slug is used to generate IRIs like SKG["org/DSWD"]
ALIASES: dict[str, str] = {
    # Government agencies
    "DSWD": "DSWD",
    "DWSD": "DSWD",
    "Department of Social Welfare and Development": "DSWD",
    "DPWH": "DPWH",
    "Department of Public Works and Highways": "DPWH",
    "DOH": "DOH",
    "DOLE": "DOLE",
    "Department of Labor and Employment": "DOLE",
    "Department of Health": "DOH",
    "DILG": "DILG",
    "Department of the Interior and Local Government": "DILG",
    "DA": "DA",
    "Department of Agriculture": "DA",
    "DENR": "DENR",
    "Department of Environment and Natural Resources": "DENR",
    "DepEd": "DepEd",
    "Department of Education": "DepEd",
    "DOE": "DOE",
    "Department of Energy": "DOE",
    "DOT": "DOT",
    "Department of Tourism": "DOT",
    "DOTC": "DOTC",
    "Department of Transportation and Communications": "DOTC",
    "DOST": "DOST",
    "Department of Science and Technology": "DOST",
    "DTI": "DTI",
    "Department of Trade and Industry": "DTI",
    "NHA": "NHA",
    "National Housing Authority": "NHA",
    "Philippine General Hospital": "PGH",
    "PGH": "PGH",
    
    # Disaster/defense agencies
    "OCD": "OCD",
    "Office of Civil Defense": "OCD",
    "NDRRMC": "NDRRMC",
    "National Disaster Risk Reduction and Management Council": "NDRRMC",
    "PAGASA": "PAGASA",
    "Philippine Atmospheric, Geophysical and Astronomical Services Administration": "PAGASA",
    "PHIVOLCS": "PHIVOLCS",
    "Philippine Institute of Volcanology and Seismology": "PHIVOLCS",
    "NDCC": "NDCC",
    "RDCC": "RDCC",
    "National Disaster Coordinating Council": "NDCC",
    "Regional Disaster Coordinating Council": "RDCC",
    "CDAG": "CDAG",
    "Civil Defense Action Group": "CDAG",


    # Military and uniformed services
    "AFP": "AFP",
    "Armed Forces of the Philippines": "AFP",
    "PNP": "PNP",
    "Philippine National Police": "PNP",
    "BFP": "BFP",
    "Bureau of Fire Protection": "BFP",
    "PCG": "PCG",
    "Philippine Coast Guard": "PCG",

    # Red Cross / NGOs
    "PRC": "PNRC",
    "Philippine Red Cross": "PNRC",
    "PNRC": "PNRC",
    "Philippine National Red Cross": "PNRC",
    "ICRC": "ICRC",
    "International Committee of the Red Cross": "ICRC",

    # International organizations
    "UN": "UN",
    "United Nations": "UN",
    "UNICEF": "UNICEF",
    "WHO": "WHO",
    "World Health Organization": "WHO",
    "WFP": "WFP",
    "World Food Programme": "WFP",
    "UNDP": "UNDP",
    "USAID": "USAID",
    "OFDA": "OFDA",
    "BHA": "BHA",
    "OFDA/BHA": "OFDA_BHA",
    "JICA": "JICA",
    "ADB": "ADB",
    "Asian Development Bank": "ADB",

    # Local government
    "LGU": "LGU",
    "Local Government Unit": "LGU",
    "LGUs": "LGU",
    "BLGU": "BLGU",
    "MLGU": "MLGU",
    "PLGU": "PLGU",

    # Utilities / infrastructure
    "NPC": "NPC",
    "National Power Corporation": "NPC",
    "MWSS": "MWSS",
    "LWUA": "LWUA",
    "NTC": "NTC",
    "National Telecommunications Commission": "NTC",
}

# Precompute lowercase keys for fuzzy matching
_ALIAS_KEYS_LOWER = {k.lower(): v for k, v in ALIASES.items()}

# Splitter pattern: commas, semicolons, " and ", slashes, newlines
_SPLIT_PATTERN = re.compile(r'[,;\n/]+|\band\b', re.IGNORECASE)

FUZZY_THRESHOLD = 79


class OrgResolver:
    """Resolve organization names to canonical slugs."""

    def __init__(self, aliases: dict[str, str] | None = None, threshold: int = FUZZY_THRESHOLD):
        self.aliases = aliases or ALIASES
        self._keys_lower = {k.lower(): v for k, v in self.aliases.items()}
        self.threshold = threshold

    def split_orgs(self, text: str) -> list[str]:
        """Split a concatenated org string into individual names."""
        if not text:
            return []
        parts = _SPLIT_PATTERN.split(str(text))
        return [p.strip() for p in parts if p and p.strip()]

    def resolve(self, name: str) -> str | None:
        """Resolve a single org name to its canonical slug.

        Returns None if no match is found above threshold.
        """
        if not name or not name.strip():
            return None

        name_stripped = name.strip()

        # Exact match (case-insensitive)
        exact = self._keys_lower.get(name_stripped.lower())
        if exact:
            return exact

        # Fuzzy match
        best_score = 0
        best_slug = None
        for key_lower, slug in self._keys_lower.items():
            score = fuzz.token_sort_ratio(name_stripped.lower(), key_lower)
            if score > best_score:
                best_score = score
                best_slug = slug

        if best_score >= self.threshold:
            return best_slug

        return None

    def split_and_resolve(self, text: str) -> list[str]:
        """Split text into org names and resolve each. Returns list of canonical slugs."""
        results = []
        seen = set()
        for name in self.split_orgs(text):
            slug = self.resolve(name)
            if slug and slug not in seen:
                results.append(slug)
                seen.add(slug)
        return results


# Module-level singleton
ORG_RESOLVER = OrgResolver()

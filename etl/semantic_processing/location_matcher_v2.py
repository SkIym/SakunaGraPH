from rdflib import Graph, RDF, Namespace, URIRef, RDFS
from typing import List, Optional
from thefuzz import fuzz, process
import re

FUZZ_THRESHOLD_REGION = 85
FUZZ_THRESHOLD_PROVINCE = 85
FUZZ_THRESHOLD_MUNI = 60

_ABBREV_MAP = {
    r'\bsta\.?': 'santa',
    r'\bsto\.?': 'santo',
    r'\bgen\.?': 'general',
    r'\bbrgy\.?': 'barangay',
    r'\bpob\.?': 'poblacion',
    r'\bpurok\.?': 'purok',
    r'\bmt\.?': 'mount',
}

class LocationMatcher:
    def __init__(self, graph_path: str):

        # -------------------------
        # Region normalization map
        # -------------------------
        self.region_map = {
            "i": "0100000000",
            "1": "0100000000",
            "region i": "0100000000",
            "region 1": "0100000000",
            "ilocos": "0100000000",
            "ilocos region": "0100000000",

            "ii": "0200000000",
            "2": "0200000000",
            "region ii": "0200000000",
            "region 2": "0200000000",
            "cagayan valley": "0200000000",

            "iii": "0300000000",
            "3": "0300000000",
            "region iii": "0300000000",
            "region 3": "0300000000",
            "central luzon": "0300000000",

            "iv-a": "0400000000",
            "4a": "0400000000",
            "iva": "0400000000",
            "region iv-a": "0400000000",
            "calabarzon": "0400000000",

            "iv-b": "1700000000",
            "4b": "1700000000",
            "ivb": "1700000000",
            "region iv-b": "1700000000",
            "mimaropa": "1700000000",

            "v": "0500000000",
            "5": "0500000000",
            "region v": "0500000000",
            "region 5": "0500000000",
            "bicol": "0500000000",
            "bicol region": "0500000000",

            "vi": "0600000000",
            "6": "0600000000",
            "region vi": "0600000000",
            "region 6": "0600000000",
            "western visayas": "0600000000",

            "vii": "0700000000",
            "7": "0700000000",
            "region vii": "0700000000",
            "region 7": "0700000000",
            "central visayas": "0700000000",

            "viii": "0800000000",
            "8": "0800000000",
            "region viii": "0800000000",
            "region 8": "0800000000",
            "eastern visayas": "0800000000",

            "ix": "0900000000",
            "9": "0900000000",
            "region ix": "0900000000",
            "region 9": "0900000000",
            "zamboanga peninsula": "0900000000",

            "x": "1000000000",
            "10": "1000000000",
            "region x": "1000000000",
            "region 10": "1000000000",
            "northern mindanao": "1000000000",

            "xi": "1100000000",
            "11": "1100000000",
            "region xi": "1100000000",
            "davao": "1100000000",
            "region 11": "1100000000",

            "xii": "1200000000",
            "12": "1200000000",
            "region xii": "1200000000",
            "region 12": "1200000000",
            "soccsksargen": "1200000000",

            "xiii": "1600000000",
            "13": "1600000000",
            "region xiii": "1600000000",
            "region 13": "1600000000",
            "caraga": "1600000000",
            "rtr": "1600000000",

            "ncr": "1300000000",
            "metro manila": "1300000000",
            "national capital region": "1300000000",

            "car": "1400000000",
            "cordillera": "1400000000",

            "barmm": "1900000000",
            "armm": "1900000000",
            "bangsamoro": "1900000000",

            "nir": "1800000000",
            "negros island region": "1800000000"
        }

        self.SKG = Namespace("https://sakuna.ph/")
        self.base = "https://sakuna.ph/"

        # Load graph
        self.g = Graph()
        self.g.parse(graph_path)

        # IRI → label
        self.municipalities: dict[str, str] = {}

        # IRI -> IRI of the parent
        self.municipalities_parent: dict[str, str] = {}

        # label → IRI
        self.provinces: dict[str, str] = {}

        self._load_locations()

        # reverse lookup

        # label -> IRI
        self.municipalities_rev = {
            label.lower(): iri for iri, label in self.municipalities.items()
        }

    # --------------------------------------------------
    # Load RDF locations
    # --------------------------------------------------
    def _load_locations(self) -> None:
        for s, _, _ in self.g.triples((None, RDF.type, URIRef(self.SKG["Municipality"]))):
            label = str(self.g.value(s, RDFS.label))
            self.municipalities[str(s)] = label.lower()
            self.municipalities_parent[str(s)] = str(
                self.g.value(s, URIRef(self.SKG["isPartOf"]))
            )
        for s, _, _ in self.g.triples((None, RDF.type, URIRef(self.SKG["City"]))):
            label = str(self.g.value(s, RDFS.label))
            self.municipalities[str(s)] = label.lower()
            self.municipalities_parent[str(s)] = str(
                self.g.value(s, URIRef(self.SKG["isPartOf"]))
            )   

        for s, _, _ in self.g.triples((None, RDF.type, URIRef(self.SKG["SubMunicipality"]))):
            label = str(self.g.value(s, RDFS.label))
            self.municipalities[str(s)] = label.lower()
            self.municipalities_parent[str(s)] = str(
                self.g.value(s, URIRef(self.SKG["isPartOf"]))
            )     

        for s, _, _ in self.g.triples((None, RDF.type, URIRef(self.SKG["Province"]))):
            label = str(self.g.value(s, RDFS.label))
            self.provinces[label.lower()] = str(s)

    # --------------------------------------------------
    # Fuzzy helper
    # --------------------------------------------------
    def _fuzzy_match(self, query: str | list[str], choices: list[str], threshold: int):
        if not query or not choices:
            return None

        if type(query) == str:
            match, score = process.extractOne(
                query,
                choices,
                scorer=fuzz.token_sort_ratio
            )
            return match if score >= threshold else None
        
        else:
            highest_score: int = 0
            closest_match = query[0]

            for q in query:
                match, score = process.extractOne(
                    q,
                    choices,
                    scorer=fuzz.token_sort_ratio
                )
                if score > highest_score: 
                    highest_score = score
                    closest_match = match

            return closest_match if highest_score >= threshold else None

    def _normalize(self, label: str) -> str:
        label = label.lower().strip()
        for pattern, replacement in _ABBREV_MAP.items():
            label = re.sub(pattern, replacement, label, flags=re.IGNORECASE)
        return label
    # --------------------------------------------------
    # Region-scoped municipality lookup
    # --------------------------------------------------
    def _municipalities_in_region(self, region_iri: str) -> dict[str, str]:
        """
        Return {muni_iri: label} for all municipalities whose province
        belongs to the given region, plus HUCs directly under the region.
        """
        result = {}
        for muni_iri, prov_iri in self.municipalities_parent.items():
            # HUCs / municipalities directly parented to the region
            if prov_iri == region_iri:
                result[muni_iri] = self.municipalities[muni_iri]
                continue
            # Normal cities/munis whose province is in the region
            grandparent = str(self.g.value(URIRef(prov_iri), URIRef(self.SKG["isPartOf"])))
            if grandparent == region_iri:
                result[muni_iri] = self.municipalities[muni_iri]
        return result

    def _fuzzy_match_municipality_in_region(self, label: str, region_iri: str) -> Optional[str]:
        """
        Fuzzy-match a municipality/city label against all municipalities
        that belong (directly or via province) to the given region.
        Returns the matched municipality IRI or None.
        """
        candidate = label.lower().split(" (")[0].strip()
        city      = candidate + " city"
        city_of   = "city of " + candidate


        candidates = self._municipalities_in_region(region_iri)
        # print(candidates)
        if not candidates:
            return None

        # Exact label check first
        for iri, lbl in candidates.items():
            if lbl in (candidate, city, city_of):
                return iri

        # Fuzzy fallback
        fuzzy = self._fuzzy_match(
            [candidate, city, city_of],
            list(candidates.values()),
            FUZZ_THRESHOLD_MUNI
        )
        if fuzzy:
            return next((iri for iri, lbl in candidates.items() if lbl == fuzzy), None)

        return None

    def match_region(self, label: str) -> Optional[str]:
        """
        Matches region. Returns loc if found, else none.
        
        """
        label = self._normalize(label)

        if label in self.region_map:
            return self.base + self.region_map[label]

        fuzzy = self._fuzzy_match(
            label,
            list(self.region_map.keys()),
            FUZZ_THRESHOLD_REGION
        )
        if fuzzy:
            return self.base + self.region_map[fuzzy]

        return None
    
    def match_province(self, label: str) -> Optional[str]:
        """
        Matches province. Returns loc if found, else none.
        
        """
        label = self._normalize(label)

        if label in self.provinces:
            return self.provinces[label]

        fuzzy = self._fuzzy_match(
            label,
            list(self.provinces.keys()),
            FUZZ_THRESHOLD_PROVINCE
        )
        return self.provinces[fuzzy] if fuzzy else None
    
    def match_municipality(self, label: str, parent_iri: str | None) -> Optional[str]:
        """
        Matches municity. Requires parent province iri to resolve duplicate names. 
        Returns loc if found, else none.
        
        """
        label = self._normalize(label)
        
        candidate = label.split(" (")[0].strip()
        city = candidate + " city"
        city_of = "city of " + city.replace(" city", "")
        
        mun_cities_prov = []
        if parent_iri:
            # exact label + correct parent
            for iri, lbl in self.municipalities.items():

                if (lbl == candidate or lbl == city or lbl == city_of) and self.municipalities_parent[iri] == parent_iri:
                    return iri

            # another pass for HUCs
            for iri, lbl in self.municipalities.items():
                if (lbl == city or lbl == city_of):
                    return iri
                
            mun_cities_pro_iri = [k for k, v in self.municipalities_parent.items() if v == parent_iri]
            mun_cities_prov = [self.municipalities[i] for i in mun_cities_pro_iri]
                
        # fuzzy fallback

        fuzzy = self._fuzzy_match(
            [candidate, city, city_of],
            mun_cities_prov if mun_cities_prov else list(self.municipalities_rev.keys()),
            FUZZ_THRESHOLD_MUNI
        )

        return self.municipalities_rev[fuzzy] if fuzzy else None
    # --------------------------------------------------
    # Main matcher
    # --------------------------------------------------
    def match(self, locations: List[str]) -> List[str]:
        matched: List[str] = []

        for loc in locations:
            levels = [lvl.strip() for lvl in loc.split(",")]
            highest = levels.pop()

            # Nation-wide / island-group passthrough
            if highest in {"Philippines", "Luzon", "Visayas", "Mindanao"}:
                matched.append(self.base + highest)
                continue

            # Ambiguous Region IV → both IV-A and IV-B
            if highest in {"4", "Region 4", "IV"}:
                matched.extend([self.base + "0400000000-A", self.base + "1700000000"])
                continue

            # Single-tier: try region → province → municipality in order
            if not levels:
                region_iri = self.match_region(highest)
                if region_iri:
                    matched.append(region_iri)
                    continue

                prov_iri = self.match_province(highest)
                if prov_iri:
                    matched.append(prov_iri)
                    continue

                muni_iri = self.match_municipality(highest, None)
                matched.append(muni_iri if muni_iri else "")
                continue

            # Multi-tier: try highest as region
            region_iri = self.match_region(highest)

            # highest is not a region — treat as province or municipality
            if not region_iri:
                prov_iri = self.match_province(highest)

                if prov_iri and levels:
                    # Province + municipality
                    muni_label = levels.pop()
                    muni_iri = self.match_municipality(muni_label, prov_iri)

                    # Fallback: try highest as the municipality (city in province slot)
                    if not muni_iri or self.municipalities_parent.get(muni_iri) != prov_iri:
                        muni_iri = self.match_municipality(highest, prov_iri)

                    matched.append(muni_iri if muni_iri else prov_iri)

                elif prov_iri:
                    # Province only — try highest as municipality label
                    muni_iri = self.match_municipality(highest.lower(), prov_iri)
                    matched.append(muni_iri if muni_iri else prov_iri)

                elif levels:
                    # No province match — try next level as municipality
                    muni_label = levels.pop()
                    muni_iri = self.match_municipality(muni_label, None)
                    matched.append(muni_iri if muni_iri else "")

                else:
                    # Last resort: try highest as municipality
                    muni_iri = self.match_municipality(highest, None)
                    matched.append(muni_iri if muni_iri else "")

                continue

            # highest is a region — pop next level as province
            prov_label = levels.pop()
            prov_iri = self.match_province(prov_label)

            if prov_iri and levels:
                # Region + province + municipality
                muni_label = levels.pop()
                muni_iri = self.match_municipality(muni_label, prov_iri)
                matched.append(muni_iri if muni_iri else prov_iri)

            elif prov_iri:
                # Region + province only
                matched.append(prov_iri)

            else:
                # prov_label didn't match — could be a city in the province slot,
                # an outdated province name, or a repeated region alias (e.g. NCR)

                # Special case: Maguindanao split-province legacy
                if levels and prov_label.lower() == "maguindanao":
                    muni_label = levels.pop()
                    muni_iri = self.match_municipality(muni_label, None)
                    if muni_iri:
                        matched.append(muni_iri)
                        continue

                # Try prov_label as a city/municipality scoped to region
                muni_iri = self._fuzzy_match_municipality_in_region(prov_label, region_iri)
                if muni_iri:
                    matched.append(muni_iri)
                    continue

                # Try next level scoped to region, fall back to region itself
                if levels:
                    muni_label = levels.pop()
                    muni_iri = self._fuzzy_match_municipality_in_region(muni_label, region_iri)
                    matched.append(muni_iri if muni_iri else region_iri)
                else:
                    matched.append(region_iri)

        return matched
    
    def match_cell(self, cell: str) -> list[str]:
        """
        Parse a pipe-delimited multi-location cell and return all matched IRIs.

        Example:
            "Davao City, Davao del Sur, XI | Cebu City, VII"
            → [<iri for Davao City>, <iri for Cebu City>]
        """
        locations = [loc.strip() for loc in cell.split("|") if loc.strip()]
        return self.match(locations)

LOCATION_MATCHER = LocationMatcher(
    # graph_path="../data/rdf/psgc_rdf.ttl"
    graph_path="../data/rdf/psgc/psgc.ttl"

)


# ─────────────────────────────────────────────────────────────────────────────
# Smoke tests
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    graph_path = sys.argv[1] if len(sys.argv) > 1 else "../data/rdf/psgc/psgc.ttl"
    matcher    = LocationMatcher(graph_path)
    BASE       = matcher.base

    tests: list[tuple[str, str | None, str]] = [
        # ── Region only ───────────────────────────────────────────────────────
        ("NCR",                               BASE + "1300000000",                         "NCR acronym"),
        ("Metro Manila",                      BASE + "1300000000",                         "Metro Manila alias for NCR"),
        ("National Capital Region",           BASE + "1300000000",                         "NCR full name"),
        ("CAR",                               BASE + "1400000000",                "CAR acronym"),
        ("Cordillera",                        BASE + "1400000000",                "Cordillera alias"),
        ("BARMM",                             BASE + "1900000000", "BARMM acronym"),
        ("ARMM",                              BASE + "1900000000", "ARMM legacy acronym"),
        ("XI",                                BASE + "1100000000",                                       "Roman numeral region"),
        ("Davao",                             BASE + "1100000000",                                       "Region XI by common name"),
        ("CALABARZON",                        BASE + "0400000000-A",                                     "Region IV-A by name"),
        ("MIMAROPA",                          BASE + "1700000000",                                     "Region IV-B by name"),
        ("CARAGA",                            BASE + "1600000000",                                     "Region XIII by name"),
        ("RTR",                               BASE + "1600000000",                                     "Region XIII RTR alias"),

        # ── Ambiguous Region IV → two IRIs appended ───────────────────────────
        ("4",                                 "DUAL", "Bare '4' → both IV-A and IV-B"),
        ("IV",                                "DUAL", "Roman IV → both IV-A and IV-B"),
        ("Region 4",                          "DUAL", "'Region 4' → both IV-A and IV-B"),

        # ── Island group passthrough ───────────────────────────────────────────
        ("Philippines",                       "Philippines", "Philippines passthrough"),
        ("Luzon",                             "Luzon",       "Luzon passthrough"),
        ("Visayas",                           "Visayas",     "Visayas passthrough"),
        ("Mindanao",                          "Mindanao",    "Mindanao passthrough"),

        # ── Province → Region ─────────────────────────────────────────────────
        ("Abra, CAR",                         None, "Abra province under CAR"),
        ("Benguet, CAR",                      None, "Benguet province under CAR"),
        ("Davao del Sur, XI",                 None, "Davao del Sur under Region XI"),
        ("Bulacan, III",                      None, "Bulacan under Central Luzon"),
        ("Cebu, VII",                         None, "Cebu province under Central Visayas"),

        # ── Municipality → Province → Region ──────────────────────────────────
        ("Davao City, Davao del Sur, XI",     None, "Standard 3-level match"),
        ("Laoag, Ilocos Norte, I",            None, "Laoag City under Ilocos Norte"),
        ("Vigan, Ilocos Sur, I",              None, "Vigan City under Ilocos Sur"),

        # ── NCR: no province layer, city sits directly under region ───────────
        ("San Juan, NCR",                     None, "NCR city, province label is region alias"),
        ("Pateros, NCR",                      None, "Pateros municipality directly under NCR"),
        ("Quezon City, NCR",                  None, "Quezon City directly under NCR"),

        # ── HUC not under a province ──────────────────────────────────────────
        ("City of Baguio, CAR",               None, "Baguio HUC — independent of province"),

        # ── Maguindanao split-province legacy ─────────────────────────────────
        ("Cotabato City, Maguindanao, XII",   None, "Maguindanao split: muni matched without province scope"),

        # ── Erratic / inverted input (city where province expected) ───────────
        ("Cebu City, VII",                    None, "City name in province slot"),

        # ── Fuzzy / typo tolerance ────────────────────────────────────────────
        ("Davao del Sur, XII",                None, "'Davao del Sur' fuzzy-matches under Region XII"),
        ("Bulacan, Central Luzon",            None, "Province + region common name"),
        ("Zamboanga, Zamboanga Peninsula",    None, "Region by full name"),

        ("Brgy Sabang, Puerto Galera, Oriental Mindoro", None, "Puerto Galera"),
    ]

    # ── Runner ────────────────────────────────────────────────────────────────
    passed = failed = 0

    print(f"\n  {'ST':<2}  {'INPUT':<46}  {'RESULT':<55}  NOTE")
    print("  " + "─" * 122)

    for inp, expected, note in tests:
        results = matcher.match([inp])

        if expected == "DUAL":
            ok      = len(results) == 2 and results[0] == BASE + "0400000000-A" and results[1] == BASE + "1700000000"
            display = f"...IV-A  +  ...IV-B" if ok else str(results)
        elif expected is None:
            ok      = bool(results) and results[0] not in ("", "Fix location columns please")
            display = results[0] if results else "—"
        else:
            ok      = bool(results) and results[0] == expected
            display = results[0] if results else "—"

        status = "✓" if ok else "✗"
        passed += 1 if ok else 0
        failed += 0 if ok else 1

        print(f"  {status}   {inp:<46}  {display:<55}  {note}")

    print(f"\n  {passed} passed, {failed} failed out of {len(tests)} tests.")
from rdflib import Graph, RDF, Namespace, URIRef, RDFS
from typing import List, Optional
from thefuzz import fuzz, process
import re

FUZZ_THRESHOLD_REGION = 85
FUZZ_THRESHOLD_PROVINCE = 85
FUZZ_THRESHOLD_MUNI = 60


class LocationMatcher:
    def __init__(self, graph_path: str):

        # -------------------------
        # Region normalization map
        # -------------------------
        self.region_map = {
            "I": "Region_I",
            "1": "Region_I",
            "Region I": "Region_I",
            "Region 1": "Region_I",
            "Ilocos": "Region_I",
            "Ilocos Region": "Region_I",

            "II": "Region_II",
            "2": "Region_II",
            "Region II": "Region_II",
            "Region 2": "Region_II",
            "Cagayan Valley": "Region_II",

            "III": "Region_III",
            "3": "Region_III",
            "Region III": "Region_III",
            "Region 3": "Region_III",
            "Central Luzon": "Region_III",

            "IV-A": "Region_IV-A",
            "4A": "Region_IV-A",
            "IVA": "Region_IV-A",
            "Region IV-A": "Region_IV-A",
            "CALABARZON": "Region_IV-A",

            "IV-B": "Region_IV-B",
            "4B": "Region_IV-B",
            "IVB": "Region_IV-B",
            "Region IV-B": "Region_IV-B",
            "MIMAROPA": "Region_IV-B",

            "V": "Region_V",
            "5": "Region_V",
            "Region V": "Region_V",
            "Bicol": "Region_V",

            "VI": "Region_VI",
            "6": "Region_VI",
            "Region VI": "Region_VI",
            "Western Visayas": "Region_VI",

            "VII": "Region_VII",
            "7": "Region_VII",
            "Region VII": "Region_VII",
            "Central Visayas": "Region_VII",

            "VIII": "Region_VIII",
            "8": "Region_VIII",
            "Region VIII": "Region_VIII",
            "Eastern Visayas": "Region_VIII",

            "IX": "Region_IX",
            "9": "Region_IX",
            "Region IX": "Region_IX",
            "Zamboanga Peninsula": "Region_IX",

            "X": "Region_X",
            "10": "Region_X",
            "Region X": "Region_X",
            "Northern Mindanao": "Region_X",

            "XI": "Region_XI",
            "11": "Region_XI",
            "Region XI": "Region_XI",
            "Davao": "Region_XI",

            "XII": "Region_XII",
            "12": "Region_XII",
            "Region XII": "Region_XII",
            "SOCCSKSARGEN": "Region_XII",

            "XIII": "Region_XIII",
            "13": "Region_XIII",
            "Region XIII": "Region_XIII",
            "CARAGA": "Region_XIII",
            "RTR": "Region_XIII",

            "NCR": "National_Capital_Region",
            "Metro Manila": "National_Capital_Region",
            "National Capital Region": "National_Capital_Region",

            "CAR": "Cordillera_Administrative_Region",
            "Cordillera": "Cordillera_Administrative_Region",

            "BARMM": "Bangsamoro_Autonomous_Region_In_Muslim_Mindanao",
            "ARMM": "Bangsamoro_Autonomous_Region_In_Muslim_Mindanao",
            "Bangsamoro": "Bangsamoro_Autonomous_Region_In_Muslim_Mindanao",
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

        for s, _, _ in self.g.triples((None, RDF.type, URIRef(self.SKG["Province"]))):
            label = str(self.g.value(s, RDFS.label))
            self.provinces[label.lower()] = str(s)

    # --------------------------------------------------
    # Fuzzy helper
    # --------------------------------------------------
    def _fuzzy_match(self, query: str, choices: list[str], threshold: int):
        if not query or not choices:
            return None

        match, score = process.extractOne(
            query,
            choices,
            scorer=fuzz.token_sort_ratio
        )
        return match if score >= threshold else None

    def match_region(self, label: str) -> Optional[str]:
        """
        Matches region. Returns loc if found, else none.
        
        """
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
        label = label.lower()
        candidate = label.split(" (")[0].strip()
        
        if parent_iri:
            # exact label + correct parent
            
            city = candidate + " city"
            city_of = "city of " + city.replace(" city", "")
            
            for iri, lbl in self.municipalities.items():

                if (lbl == candidate or lbl == city or lbl == city_of) and self.municipalities_parent[iri] == parent_iri:
                    return iri

            # another pass for HUCs

            for iri, lbl in self.municipalities.items():
                if (lbl == city or lbl == city_of):
                    return iri


        # fuzzy fallback
        fuzzy = self._fuzzy_match(
            candidate,
            list(self.municipalities_rev.keys()),
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

            if highest in {"Philippines", "Luzon", "Visayas", "Mindanao"}:
                matched.append(highest)
                continue

            if highest in {"4", "Region 4", "IV"}:
                matched.extend([
                    self.base + "Region_IV-A",
                    self.base + "Region_IV-B"
                ])
                continue

            region_iri = self.match_region(highest)

            if region_iri and not levels:
                matched.append(region_iri)
                continue
            
            prov_label = levels.pop()
            prov_iri = self.match_province(prov_label)

            # Get prov level
            if region_iri:
            
                # Get municity level
                if prov_iri and levels:
                    muni_label = levels.pop()
                    muni_iri = self.match_municipality(muni_label, prov_iri)


                    matched.append(muni_iri if muni_iri else prov_iri)

                # If no municity match, use province
                elif prov_iri:
                    matched.append(prov_iri)

                # If no province match, use region
                else:
                    
                    # handle city placed in provinces
                    muni_iri = self.match_municipality(prov_label, region_iri)

                    if muni_iri:
                        matched.append(muni_iri)

                    # handle outdated province or repated region (NCR) names
                    # e.g. maguindanao 
                    else:
                        muni_label = levels.pop()
                        if prov_label == "maguindanao":
                            muni_iri = self.match_municipality(muni_label, None)
                        else:
                            muni_iri = self.match_municipality(muni_label, region_iri)

                        matched.append(muni_iri if muni_iri else region_iri)
            
            # handle erratic parsed locations
            # e.g. city_muni in region column
            else:

                if prov_iri and levels:
                    muni_label = levels.pop()
                    muni_iri = self.match_municipality(muni_label, prov_iri)


                    if not muni_iri or self.municipalities_parent[muni_iri] != prov_iri:
                        muni_iri = self.match_municipality(highest, prov_iri)

                    matched.append(muni_iri if muni_iri else prov_iri)

                    # if muni_iri:
                    #     if self.municipalities_parent[muni_iri] != prov_iri: 
                    #         muni_iri = self.match_municipality(highest, prov_iri)
                    #         matched.append(muni_iri if muni_iri else prov_iri)
                    #     else:
                    #         matched.append(muni_iri)
                    
                    # else:
                    #     muni_iri = self.match_municipality(highest, prov_iri)
                    #     matched.append(muni_iri if muni_iri else prov_iri)
                
                else:
                    matched.append("Fix location columns please")


        return matched

LOCATION_MATCHER = LocationMatcher(
    graph_path="triples/psgc_rdf.ttl"
)
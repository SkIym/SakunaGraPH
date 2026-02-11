from rdflib import Graph, RDF, Namespace, URIRef, RDFS
from typing import List
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
            label: iri for iri, label in self.municipalities.items()
        }

    # --------------------------------------------------
    # Load RDF locations
    # --------------------------------------------------
    def _load_locations(self) -> None:
        for s, _, _ in self.g.triples((None, RDF.type, URIRef(self.SKG["Municipality"]))):
            label = str(self.g.value(s, RDFS.label))
            self.municipalities[str(s)] = label
            self.municipalities_parent[str(s)] = str(
                self.g.value(s, URIRef(self.SKG["isPartOf"]))
            )

        for s, _, _ in self.g.triples((None, RDF.type, URIRef(self.SKG["Province"]))):
            label = str(self.g.value(s, RDFS.label))
            self.provinces[label] = str(s)

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

    # --------------------------------------------------
    # Main matcher
    # --------------------------------------------------
    def match(self, locations: List[str]) -> List[str]:
        matched: List[str] = []

        for loc in locations:
            levels = [lvl.strip() for lvl in loc.split(",")]
            highest = levels.pop()

            # Country / island groups
            if highest in {"Philippines", "Luzon", "Visayas", "Mindanao"}:
                matched.append(highest)
                continue

            # Region IV split case
            if highest in {"4", "Region 4", "IV"}:
                matched.extend(["Region_IV-A", "Region_IV-B"])
                continue

            # -------------------------
            # Region matching
            # -------------------------
            region = self.region_map.get(highest)
            prov_label = ""
            if not region:
                fuzzy = self._fuzzy_match(
                    highest,
                    list(self.region_map.keys()),
                    FUZZ_THRESHOLD_REGION
                )
                if fuzzy:
                    region = self.region_map[fuzzy]

            if region:
                if not levels:
                    matched.append(self.base + region)
                    continue
                parent = region
                highest = levels.pop()
            else:
                prov_label = (
                highest
                if highest in self.provinces
                else self._fuzzy_match(
                    highest,
                    list(self.provinces.keys()),
                    FUZZ_THRESHOLD_PROVINCE
                )
            )
                
            if not region:
                matched.append("Fix location columns please")
                continue

            # -------------------------
            # Province matching
            # -------------------------
            if not prov_label:
                prov_label = (
                    highest
                    if highest in self.provinces
                    else self._fuzzy_match(
                        highest,
                        list(self.provinces.keys()),
                        FUZZ_THRESHOLD_PROVINCE
                    )
                )

            if prov_label :

                prov_IRI = self.provinces[prov_label]

                if levels:
                    candidate = levels.pop()

                    # -------------------------
                    # Municipality matching
                    # -------------------------

                    if "(" in candidate:
                        candidate = candidate.split(" (")[0]

                    # handle municities with the same name
                    munis = [
                        iri
                        for iri, label in self.municipalities.items()
                        if label == candidate
                    ]

                    muni_IRI = ""

                    
                    for iri in munis:
                        if self.municipalities_parent[iri] == prov_IRI:
                            muni_IRI = iri
                            break
                    
                    # if "Pampanga" in prov_IRI:
                    #     print(muni_IRI)

                    if not muni_IRI:
                        temp = candidate.replace("City", "")
                        temp = temp.strip()
                        temp = "City of " + temp

                        munis = [
                            iri
                            for iri, label in self.municipalities.items()
                            if label == temp
                        ]

                        muni_IRI = ""

                        for iri in munis:
                            if self.municipalities_parent[iri] == prov_IRI:
                                muni_IRI = iri
                                break
                        
                        # print(muni_IRI)
                        # muni_IRI = self.municipalities_rev[temp] if temp in self.municipalities_rev else ""

                    # fuzzy match
                    if not muni_IRI:
                        fuzzy = self._fuzzy_match(
                            candidate,
                            list(self.municipalities_rev.keys()),
                            FUZZ_THRESHOLD_MUNI
                        )
                        if fuzzy:
                            muni_IRI = self.municipalities_rev[fuzzy]

                    if muni_IRI:
                        matched.append(muni_IRI)
                        
                    else:
                        matched.append(prov_IRI)

                else:
                    matched.append(prov_IRI)

                continue
            
            else:
                
                if levels:
                    highest = levels.pop()
                    if "(" in highest:
                        highest = highest.split(" (")[0].replace("-", "")
                

                iri = self.municipalities_rev[highest] if highest in self.municipalities_rev else ""
                if iri:
                    matched.append(iri)
                    continue

                loc_label = highest.replace("City", "")
                loc_label = loc_label.strip()
                loc_label = "City of " + loc_label

                if loc_label in self.municipalities_rev:
                    matched.append(self.municipalities_rev[loc_label])
                else:
                    

                    fuzzy = self._fuzzy_match(
                            highest,
                            list(self.municipalities_rev.keys()),
                            FUZZ_THRESHOLD_MUNI
                        )
                    if fuzzy:
                        matched.append(self.municipalities_rev[fuzzy])
                    else:

                        matched.append(self.base + region)
  
        return matched

LOCATION_MATCHER = LocationMatcher(
    graph_path="triples/psgc_rdf.ttl"
)
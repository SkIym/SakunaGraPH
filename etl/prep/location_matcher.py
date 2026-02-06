from rdflib import Graph, RDF, Namespace, URIRef, RDFS
from typing import List


class LocationMatcher:
    def __init__(
        self,
        graph_path: str,
        base: str,
        region_map: dict[str, str],
    ):
        self.base = base
        self.region_map = region_map

        self.SKG = Namespace("https://sakuna.ph/")

        # Load graph
        self.g = Graph()
        self.g.parse(graph_path)

        # IRI → label
        self.municipalities: dict[str, str] = {}
        self.municipalities_parent: dict[str, str] = {}

        # label → IRI
        self.provinces: dict[str, str] = {}

        self._load_locations()

        # label → IRI (reverse lookup)
        self.municipalities_rev = {
            label: iri for iri, label in self.municipalities.items()
        }

    def _load_locations(self) -> None:
        """Populate province and municipality lookup tables."""

        # Municipalities / cities
        for s, _, _ in self.g.triples(
            (None, RDF.type, URIRef(self.SKG["Municipality"]))
        ):
            label = str(self.g.value(s, RDFS.label))
            self.municipalities[str(s)] = label
            self.municipalities_parent[str(s)] = str(
                self.g.value(s, URIRef(self.SKG["isPartOf"]))
            )

        # Provinces
        for s, _, _ in self.g.triples(
            (None, RDF.type, URIRef(self.SKG["Province"]))
        ):
            label = str(self.g.value(s, RDFS.label))
            self.provinces[label] = str(s)


    def match(self, locations: str) -> List[str]:
        """
        Match a location string to SKG IRIs.

        Example input:
            "Laguna, Calamba City | NCR, Manila"
        """
        locs = [l.strip() for l in locations.split("|")]
        loc_IRIs: List[str] = []

        for loc in locs:
            if not loc:
                continue

            levels = [lvl.strip() for lvl in loc.split(",")]
            highest = levels.pop()

            # -------------------------
            # Country / island groups
            # -------------------------
            if highest in {"Philippines", "Luzon", "Visayas", "Mindanao"}:
                loc_IRIs.append(self.base + highest)
                continue

            # -------------------------
            # Special Region IV case
            # -------------------------
            if highest in {"4", "Region 4", "IIII", 4}:
                loc_IRIs.extend([
                    self.base + "Region_IV-A",
                    self.base + "Region_IV-B",
                ])
                continue

            # Regions
            region_IRI = self.region_map.get(highest)
            if region_IRI:
                region_full = self.base + region_IRI
                if not levels:
                    loc_IRIs.append(region_full)
                    continue
                highest = levels.pop()

            # Provinces → municipalities
            if highest in self.provinces:
                prov_IRI = self.provinces[highest]

                if levels:
                    candidate = levels.pop()
                    for muni_iri, label in self.municipalities.items():
                        if (
                            label == candidate
                            and self.municipalities_parent[muni_iri] == prov_IRI
                        ):
                            loc_IRIs.append(muni_iri)
                            break
                    else:
                        loc_IRIs.append(prov_IRI)
                else:
                    loc_IRIs.append(prov_IRI)
                continue

            # Municipalities / cities
            muni_IRI = self.municipalities_rev.get(highest)

            if not muni_IRI and highest.endswith(" City"):
                official = f"City of {highest[:-5]}"
                muni_IRI = self.municipalities_rev.get(official)

            if muni_IRI:
                loc_IRIs.append(muni_IRI)
            else:
                # Fallback (debugging / unknown label)
                loc_IRIs.append(highest)

        return loc_IRIs

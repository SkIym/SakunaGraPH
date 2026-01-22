from rdflib import Graph, RDF, Namespace, URIRef, RDFS, Node
import re

SKG = Namespace("https://sakuna.ph/")
base = "https://sakuna.ph/"

# For matching location IRIs
lg = Graph()
lg.parse("triples/psgc_rdf.ttl")

# IRI : label
municities: dict[str, str] = {}
municities_parent: dict[str, str] = {}
# municities_IRI: list[str | None] = []


# Label : IRI
provinces: dict[str, str] = {}
# provinces_IRI: list[str | None] = []

for s, p, o in lg.triples((None, RDF.type ,URIRef(SKG["Municipality"]))):

    lbl = str(lg.value(subject=s, predicate=RDFS.label))

    municities[str(s)] = lbl
    municities_parent[str(s)] = str(lg.value(subject=s, predicate=URIRef(SKG["isPartOf"])))

for s, p, o in lg.triples((None, RDF.type , URIRef(SKG["Province"]))):

    provinces[str(lg.value(subject=s, predicate=RDFS.label))] = str(s)



@udf(
    fun_id="https://sakuna.ph/toTypeIRI",
    hasType="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter"
)
def to_type_iri(hasType: str):
    fixedIRI = (
        hasType.replace(" ", "")
               .replace("(", "")
               .replace(")", "")
               .replace("Misc", "Miscellaneous")
               .replace("Flashflood", "FlashFlood")
               .replace("Earthquake", "")
    )
    return f"{base}{fixedIRI}"


@udf(
    fun_id="https://sakuna.ph/cleanName",
    eventName="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter"
)
def clean_event_name(eventName: str):
    fixedStr = (
        eventName.replace("\"", "")

    )
    return fixedStr


@udf(
    fun_id="https://sakuna.ph/matchLocationsToIRI",
    locations="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter"
)
def match_locs_to_IRI(locations: str):
    locs =  [l.strip() for l in locations.split("|")]
    loc_IRIs: list[str] = []   


    for loc in locs:
        if loc == "": continue

        # match if region
        region_IRI = region_map.get(loc.strip())

        if region_IRI:
            text = base + region_IRI
            loc_IRIs.append(text)

        # get province or municipality, prioritize lower adm level (mun/cities)
        else:
            levels = loc.split(",")
            loc_IRI = ""


            # get the highest adm level location then remove leading/termination ws 
            highest_level = (levels.pop()).strip()

            if highest_level in provinces:
                loc_IRI = provinces.get(highest_level)

                # Catch if levels is empty or municipality has been found then terminate
                if len(levels) > 0:
                    new_IRI = ""
                    highest_level = (levels.pop()).strip()


                    temp_IRIs  = [k for k, v in municities.items() if v == highest_level]
                    for i in temp_IRIs:
                        if municities_parent[i] == loc_IRI:
                            new_IRI = i
                            break
                    
                    if new_IRI: loc_IRI = new_IRI
                    

            if loc_IRI:
                loc_IRIs.append(loc_IRI)

    return loc_IRIs

region_map = {
    # Region I – Ilocos Region
    "I": "Region_I",
    "1": "Region_I",
    "Region I": "Region_I",
    "Region 1": "Region_I",
    "Ilocos": "Region_I",
    "Ilocos Region": "Region_I",

    # Region II – Cagayan Valley
    "II": "Region_II",
    "2": "Region_II",
    "Region II": "Region_II",
    "Region 2": "Region_II",
    "Cagayan Valley": "Region_II",

    # Region III – Central Luzon
    "III": "Region_III",
    "3": "Region_III",
    "Region III": "Region_III",
    "Region 3": "Region_III",
    "Central Luzon": "Region_III",

    # Region IV-A – CALABARZON
    "IV-A": "Region_IV-A",
    "4A": "Region_IV-A",
    "IVA": "Region_IV-A",
    "4-A": "Region_IV-A",
    "Region IV-A": "Region_IV-A",
    "Region 4A": "Region_IV-A",
    "Region IVA": "Region_IV-A",
    "Region 4-A": "Region_IV-A",
    "CALABARZON": "Region_IV-A",
    "Calabarzon": "Region_IV-A",

    # Region IV-B – MIMAROPA
    "IV-B": "Region_IV-B",
    "4B": "Region_IV-B",
    "IVB": "Region_IV-B",
    "4-B": "Region_IV-B",
    "Region IV-B": "Region_IV-B",
    "Region 4B": "Region_IV-B",
    "Region IVB": "Region_IV-B",
    "Region 4-B": "Region_IV-B",
    "MIMAROPA": "Region_IV-B",
    "Mimaropa": "Region_IV-B",

    # Region V – Bicol Region
    "V": "Region_V",
    "5": "Region_V",
    "Region V": "Region_V",
    "Region 5": "Region_V",
    "Bicol": "Region_V",
    "Bicol Region": "Region_V",

    # Region VI – Western Visayas
    "VI": "Region_VI",
    "6": "Region_VI",
    "Region VI": "Region_VI",
    "Region 6": "Region_VI",
    "Western Visayas": "Region_VI",

    # Region VII – Central Visayas
    "VII": "Region_VII",
    "7": "Region_VII",
    "Region VII": "Region_VII",
    "Region 7": "Region_VII",
    "Central Visayas": "Region_VII",

    # Region VIII – Eastern Visayas
    "VIII": "Region_VIII",
    "8": "Region_VIII",
    "Region VIII": "Region_VIII",
    "Region 8": "Region_VIII",
    "Eastern Visayas": "Region_VIII",

    # Region IX – Zamboanga Peninsula
    "IX": "Region_IX",
    "9": "Region_IX",
    "Region IX": "Region_IX",
    "Region 9": "Region_IX",
    "Zamboanga Peninsula": "Region_IX",

    # Region X – Northern Mindanao
    "X": "Region_X",
    "10": "Region_X",
    "Region X": "Region_X",
    "Region 10": "Region_X",
    "Northern Mindanao": "Region_X",

    # Region XI – Davao Region
    "XI": "Region_XI",
    "11": "Region_XI",
    "Region XI": "Region_XI",
    "Region 11": "Region_XI",
    "Davao": "Region_XI",
    "Davao Region": "Region_XI",

    # Region XII – SOCCSKSARGEN
    "XII": "Region_XII",
    "12": "Region_XII",
    "Region XII": "Region_XII",
    "Region 12": "Region_XII",
    "SOCCSKSARGEN": "Region_XII",
    "Soccsksargen": "Region_XII",

    # Region XIII – Caraga
    "XIII": "Region_XIII",
    "13": "Region_XIII",
    "Region XIII": "Region_XIII",
    "Region 13": "Region_XIII",
    "Caraga": "Region_XIII",
    "CARAGA": "Region_XIII",

    # NCR – National Capital Region
    "NCR": "National_Capital_Region",
    "National Capital Region": "National_Capital_Region",
    "Metro Manila": "National_Capital_Region",

    # CAR – Cordillera Administrative Region
    "CAR": "Cordillera_Administrative_Region",
    "Cordillera": "Cordillera_Administrative_Region",
    "Cordillera Administrative Region": "Cordillera_Administrative_Region",

    # BARMM – Bangsamoro
    "BARMM": "Bangsamoro_Autonomous_Region_In_Muslim_Mindanao",
    "ARMM": "Bangsamoro_Autonomous_Region_In_Muslim_Mindanao",
    "Bangsamoro": "Bangsamoro_Autonomous_Region_In_Muslim_Mindanao",
    "Bangsamoro Autonomous Region in Muslim Mindanao": "Bangsamoro_Autonomous_Region_In_Muslim_Mindanao"
}

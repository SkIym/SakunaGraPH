from rdflib import Graph, RDF, Namespace, URIRef, RDFS
import thefuzz as fuzz

SKG = Namespace("https://sakuna.ph/")
base = "https://sakuna.ph/"

# For matching location IRIs
lg = Graph()
lg.parse("../data/rdf/psgc.ttl")

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

for s, p, o in lg.triples((None, RDF.type ,URIRef(SKG["City"]))):

    lbl = str(lg.value(subject=s, predicate=RDFS.label))

    municities[str(s)] = lbl
    municities_parent[str(s)] = str(lg.value(subject=s, predicate=URIRef(SKG["isPartOf"])))

for s, p, o in lg.triples((None, RDF.type ,URIRef(SKG["SubMunicipality"]))):

    lbl = str(lg.value(subject=s, predicate=RDFS.label))

    municities[str(s)] = lbl
    municities_parent[str(s)] = str(lg.value(subject=s, predicate=URIRef(SKG["isPartOf"])))

for s, p, o in lg.triples((None, RDF.type , URIRef(SKG["Province"]))):

    provinces[str(lg.value(subject=s, predicate=RDFS.label))] = str(s)

# Label : IRI
municities_rev = dict([(value, key) for key, value in municities.items()])

@udf(
    fun_id="https://sakuna.ph/toTypeIRI",
    dtype="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter"
)
def to_type_iri(dtype: str):

    # Ignore related incident subtypes (already handled by Pandas)
    if "[" in dtype: return

    types = dtype.split("|")

    cleanedIRIs: list[str] = []

    for t in types:
        
        fixedIRI = (
            t.strip()
                .replace(" ", "")
                .replace("(", "")
                .replace(")", "")
                .replace("Misc", "Miscellaneous")
                .replace("Flashflood", "FlashFlood")
                .replace("Earthquake", "")
        )
        cleanedIRIs.append(base + fixedIRI)

    return cleanedIRIs

@udf(
    fun_id="https://sakuna.ph/matchLocationsToIRI",
    locations="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter"
)
def match_locs_to_IRI(locations: str):
    locs =  [l.strip() for l in locations.split("|")]
    loc_IRIs: list[str] = []   


    for loc in locs:
        if loc == "": continue

        # get province or municipality, prioritize lower adm level (mun/cities)
        
        levels = loc.split(",")
        loc_IRI = ""

        # get the highest adm level location then remove leading/termination ws 
        highest_level = (levels.pop()).strip()

        if highest_level in ["Philippines", "Luzon", "Visayas", "Mindanao"]:
            loc_IRI = base + highest_level
            loc_IRIs.append(loc_IRI)
            continue

        if highest_level in ["4", "Region 4", "IIII", 4]:
            text = base + "0400000000"
            text2 = base + "1700000000"
            loc_IRIs.extend([text, text2])
            continue
        
        # If region
        region_IRI = region_map.get(highest_level)

        if region_IRI:
            text = base + region_IRI

            if len(levels) == 0:
                loc_IRIs.append(text)
                continue
            else:
                highest_level = (levels.pop()).strip()

        # If next highest level is a province
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

        # If enxt highest level is a municipality / city
        else:
            loc_IRI = municities_rev.get(highest_level)    

            if not loc_IRI:
                # Reformat "____ City" to official "City of ____"
                official_name = highest_level[:-5]
                off_highest_level = f"City of {official_name}"
                loc_IRI = municities_rev.get(off_highest_level)    
                

        if loc_IRI:
            loc_IRIs.append(loc_IRI)

        else:
                # For debugging
            print(loc_IRIs, highest_level, provinces.get(highest_level),loc)
            loc_IRIs.append(highest_level)

    
    return loc_IRIs

region_map = {
    # Region I – Ilocos Region
    "I": "0100000000",
    "1": "0100000000",
    "Region I": "0100000000",
    "Region 1": "0100000000",
    "Ilocos": "0100000000",
    "Ilocos Region": "0100000000",

    # Region II – Cagayan Valley
    "II": "0200000000",
    "2": "0200000000",
    "Region II": "0200000000",
    "Region 2": "0200000000",
    "Cagayan Valley": "0200000000",

    # Region III – Central Luzon
    "III": "0300000000",
    "3": "0300000000",
    "Region III": "0300000000",
    "Region 3": "0300000000",
    "Central Luzon": "0300000000",

    # Region IV-A – CALABARZON
    "IV-A": "0400000000",
    "4A": "0400000000",
    "IVA": "0400000000",
    "4-A": "0400000000",
    "Region IV-A": "0400000000",
    "Region 4A": "0400000000",
    "Region IVA": "0400000000",
    "Region 4-A": "0400000000",
    "CALABARZON": "0400000000",
    "Calabarzon": "0400000000",

    # Region IV-B – MIMAROPA
    "IV-B": "1700000000",
    "4B": "1700000000",
    "IVB": "1700000000",
    "4-B": "1700000000",
    "Region IV-B": "1700000000",
    "Region 4B": "1700000000",
    "Region IVB": "1700000000",
    "Region 4-B": "1700000000",
    "MIMAROPA": "1700000000",
    "Mimaropa": "1700000000",

    # Region V – Bicol Region
    "V": "0500000000",
    "5": "0500000000",
    "Region V": "0500000000",
    "Region 5": "0500000000",
    "Bicol": "0500000000",
    "Bicol Region": "0500000000",

    # Region VI – Western Visayas
    "VI": "0600000000",
    "6": "0600000000",
    "Region VI": "0600000000",
    "Region 6": "0600000000",
    "Western Visayas": "0600000000",

    # Region VII – Central Visayas
    "VII": "0700000000",
    "7": "0700000000",
    "Region VII": "0700000000",
    "Region 7": "0700000000",
    "Central Visayas": "0700000000",

    # Region VIII – Eastern Visayas
    "VIII": "0800000000",
    "8": "0800000000",
    "Region VIII": "0800000000",
    "Region 8": "0800000000",
    "Eastern Visayas": "0800000000",

    # Region IX – Zamboanga Peninsula
    "IX": "0900000000",
    "9": "0900000000",
    "Region IX": "0900000000",
    "Region 9": "0900000000",
    "Zamboanga Peninsula": "0900000000",

    # Region X – Northern Mindanao
    "X": "1000000000",
    "10": "1000000000",
    "Region X": "1000000000",
    "Region 10": "1000000000",
    "Northern Mindanao": "1000000000",

    # Region XI – Davao Region
    "XI": "1100000000",
    "11": "1100000000",
    "Region XI": "1100000000",
    "Region 11": "1100000000",
    "Davao": "1100000000",
    "Davao Region": "1100000000",

    # Region XII – SOCCSKSARGEN
    "XII": "1200000000",
    "12": "1200000000",
    "Region XII": "1200000000",
    "Region 12": "1200000000",
    "SOCCSKSARGEN": "1200000000",
    "Soccsksargen": "1200000000",

    # Region XIII – Caraga
    "XIII": "1600000000",
    "13": "1600000000",
    "Region XIII": "1600000000",
    "Region 13": "1600000000",
    "Caraga": "1600000000",
    "CARAGA": "1600000000",

    # NCR – National Capital Region
    "NCR": "1300000000",
    "Region NCR": "1300000000",
    "National Capital Region": "1300000000",
    "National Capital Region (NCR)": "1300000000",
    "Metro Manila": "1300000000",

    # CAR – Cordillera Administrative Region
    "CAR": "1400000000",
    "Region CAR": "1400000000",
    "Cordillera": "1400000000",
    "Cordillera Administrative Region": "1400000000",

    # BARMM – Bangsamoro
    "BARMM": "1900000000",
    "ARMM": "1900000000",
    "Bangsamoro": "1900000000",
    "Bangsamoro Autonomous Region in Muslim Mindanao": "1900000000"
}

@udf(
    fun_id="https://sakuna.ph/toMajorIncident",
    clss="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter"
)
def to_major_incident_class(clss: str):
    return f"{base}MajorEvent" if clss == "M" else f"{base}Incident"

@udf(
    fun_id="https://sakuna.ph/augmentWaterSource",
    txt="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter"
)
def augment_water_source(txt: list[str]):
    return f"{txt[1]}: {txt[0]}"


@udf(
    fun_id="https://sakuna.ph/toMillions",
    val="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter"
)
def to_millions(val: str):

    fl_val = float(val)
    if fl_val == 0: return fl_val

    conv = val
    if fl_val > 1000: 
        conv = fl_val / 1000000

    return conv


from rdflib import Graph, RDF, Namespace, URIRef

SKG = Namespace("https://sakuna.ph/")
base = "https://sakuna.ph/"


# For matching location IRIs
lg = Graph()
lg.parse("triples/psgc_rdf.ttl")

for s, p, o in lg.triples((None, RDF.type, URIRef(SKG["Region"]))):
    print(f"{s} is a region")



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

    # for loc in locs:
        


    return locs


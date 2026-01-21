base = "https://sakuna.ph/"

@udf(
    fun_id="https://sakuna.ph/toTypeIRI",
    hasType="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam"
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
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


@udf(
    fun_id="https://sakuna.ph/cleanName",
    eventName="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam"
)
def clean_event_name(eventName: str):
    fixedStr = (
        eventName.replace("\"", "")

    )
    return fixedStr
raise RuntimeError

@udf(
    fun_id="https://sakuna.ph/toTypeIRI",
    text="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam"
)
def to_type_iri(hasType: str):
    local = (
        hasType.replace(" ", "")
               .replace("(", "")
               .replace(")", "")
               .replace("Misc", "Miscellaneous")
    )
    return f"https://sakuna.ph/{local}"
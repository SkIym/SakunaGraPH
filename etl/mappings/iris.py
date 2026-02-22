from rdflib import Namespace, URIRef
# import hashlib

SKG = Namespace("https://sakuna.ph/")

def event_iri(event_id: str) -> URIRef:
    return URIRef(SKG[event_id])

def prov_iri(report: str) -> URIRef:
    return URIRef(SKG[report
                      .replace(" ", "_")
                      .replace(".pdf", "")
                      .replace("-", "")])

def incident_iri(event_id: URIRef, incident_id: str) -> URIRef:
    return URIRef(event_id + f"/related_incident/{incident_id}")

def aff_pop_iri(event_id: URIRef, aff_pop_id: str) -> URIRef:
    return URIRef(event_id + f"/affected_population/{aff_pop_id}")

def casualties_iri(event_id: URIRef, cas_id: str) -> URIRef:
    return URIRef(event_id + f"/casualties/{cas_id}")

def relief_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/relief/{r_id}")
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

def incident_iri(event_id: str, row_id: str) -> URIRef:
    return URIRef(SKG[f"{event_id}/related_incident/{row_id}"])
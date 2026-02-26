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

def infra_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/infrastructure_damage/{r_id}")

def housing_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/housing_damage/{r_id}")

def agri_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/agriculture_damage/{r_id}")

def pevac_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/preemptive_evacuation/{r_id}")

def rnb_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/road_and_bridges_damage/{r_id}")

def power_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/power_disruption/{r_id}")
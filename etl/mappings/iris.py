from rdflib import Namespace, URIRef
# import hashlib

SKG = Namespace("https://sakuna.ph/")

def event_iri(event_id: str) -> URIRef:
    return URIRef(SKG[event_id])

def prov_iri(report: str) -> URIRef:
    return URIRef(SKG[report
                      .replace(" ", "_")
                      .replace(".pdf", "")
                      .replace(".xlsx", "")
                      .replace("-", "")])

def incident_iri(event_id: URIRef, incident_id: str) -> URIRef:
    return URIRef(event_id + f"/related_incident/{incident_id}")

def aff_pop_iri(event_id: URIRef, aff_pop_id: str) -> URIRef:
    return URIRef(event_id + f"/affected_population/{aff_pop_id}")

def casualties_iri(event_id: URIRef, cas_id: str) -> URIRef:
    return URIRef(event_id + f"/casualties/{cas_id}")

def relief_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/relief/{r_id}")

def assistance_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/assistance/{r_id}")

def recovery_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/recovery/{r_id}")

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

def comms_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/communication_line_disruption/{r_id}")

def doc_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/declaration_of_calamity/{r_id}")

def class_dis_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/class_suspension/{r_id}")

def work_dis_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/work_suspension/{r_id}")

def stranded_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/stranded_event/{r_id}")

def water_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/water_disruption/{r_id}")

def seaport_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/seaport_disruption/{r_id}")

def airport_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/airport_disruption/{r_id}")

def flight_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/flight_disruption/{r_id}")

def damage_gen_iri(event_id: URIRef, r_id: str) -> URIRef:
    return URIRef(event_id + f"/damage_general/{r_id}")

def org_iri(slug: str) -> URIRef:
    return URIRef(SKG[f"org/{slug}"])
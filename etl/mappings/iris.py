from rdflib import Namespace, URIRef
# import hashlib

SKG = Namespace("https://sakuna.ph/")

import uuid

# ontology namespace seed
SKG_EVENT_NS = uuid.UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")

EMDAT_EVENT_NS = uuid.UUID("e9d7c5b3-1f2a-4e6d-8c0b-7a4f3e2d1c5b")

GDA_NS = uuid.UUID("c7a2f3b1-9e4d-4a08-b5c6-1d2e3f4a5b6c")

NDRRMC_EVENT_NS = uuid.UUID("a3f2c1d4-7e8b-4f09-b5a6-2c3d4e5f6a7b")

DROMIC_EVENT_NS = uuid.UUID("f7c14e82-3b9d-4a56-8e01-d2f5a7c93b1e")
 

def event_uri(source: str, source_record_id: str) -> URIRef:
    """
    Deterministic IRI from source name + the record's own ID.
    Re-running on the same CSV always produces the same IRI.
    """
    uid = uuid.uuid5(SKG_EVENT_NS, source_record_id)
    return URIRef(f"https://sakuna.ph/{source}/{uid}")

def prov_iri(report: str) -> URIRef:
    return URIRef(SKG[report
                      .replace(" ", "_")
                      .replace(".pdf", "")
                      .replace(".xlsx", "")
                      .replace(".docx", "")
                      .replace(".", "")
                      .replace("-", "")])

def sub_iri(event_id: URIRef, segment: str, r_id: str | None = None) -> URIRef:
    """
    Build a sub-resource IRI scoped under an event IRI.
    
    sub_iri(event_id, "casualties")          → .../casualties
    sub_iri(event_id, "casualties", "abc123") → .../casualties/abc123
    """
    base = f"{event_id}/{segment}"
    return URIRef(f"{base}/{r_id}" if r_id is not None else base)


# Named aliases — same as before but delegate to sub_iri
def incident_iri    (e: URIRef, r: str | None = None): return sub_iri(e, "related_incident", r)
def aff_pop_iri     (e: URIRef, r: str | None = None): return sub_iri(e, "affected_population", r)
def casualties_iri  (e: URIRef, r: str | None = None): return sub_iri(e, "casualties", r)
def relief_iri      (e: URIRef, r: str | None = None): return sub_iri(e, "relief", r)
def assistance_iri  (e: URIRef, r: str | None = None): return sub_iri(e, "assistance", r)
def recovery_iri    (e: URIRef, r: str | None = None): return sub_iri(e, "recovery", r)
def infra_iri       (e: URIRef, r: str | None = None): return sub_iri(e, "infrastructure_damage", r)
def housing_iri     (e: URIRef, r: str | None = None): return sub_iri(e, "housing_damage", r)
def agri_iri        (e: URIRef, r: str | None = None): return sub_iri(e, "agriculture_damage", r)
def pevac_iri       (e: URIRef, r: str | None = None): return sub_iri(e, "preemptive_evacuation", r)
def rnb_iri         (e: URIRef, r: str | None = None): return sub_iri(e, "road_and_bridges_damage", r)
def power_iri       (e: URIRef, r: str | None = None): return sub_iri(e, "power_disruption", r)
def comms_iri       (e: URIRef, r: str | None = None): return sub_iri(e, "communication_line_disruption", r)
def doc_iri         (e: URIRef, r: str | None = None): return sub_iri(e, "declaration_of_calamity", r)
def class_dis_iri   (e: URIRef, r: str | None = None): return sub_iri(e, "class_suspension", r)
def work_dis_iri    (e: URIRef, r: str | None = None): return sub_iri(e, "work_suspension", r)
def stranded_iri    (e: URIRef, r: str | None = None): return sub_iri(e, "stranded_event", r)
def water_iri       (e: URIRef, r: str | None = None): return sub_iri(e, "water_disruption", r)
def seaport_iri     (e: URIRef, r: str | None = None): return sub_iri(e, "seaport_disruption", r)
def airport_iri     (e: URIRef, r: str | None = None): return sub_iri(e, "airport_disruption", r)
def flight_iri      (e: URIRef, r: str | None = None): return sub_iri(e, "flight_disruption", r)
def damage_gen_iri  (e: URIRef, r: str | None = None): return sub_iri(e, "damage_general", r)

def org_iri(slug: str) -> URIRef:
    return URIRef(SKG[f"org/{slug}"])
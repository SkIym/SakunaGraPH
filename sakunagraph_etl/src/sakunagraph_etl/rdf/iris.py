"""Deterministic IRI construction shared by all source mappings."""

from __future__ import annotations

import uuid

from rdflib import URIRef

from .graph import SKG

SKG_EVENT_NS = uuid.UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")
EMDAT_EVENT_NS = uuid.UUID("e9d7c5b3-1f2a-4e6d-8c0b-7a4f3e2d1c5b")
GDA_NS = uuid.UUID("c7a2f3b1-9e4d-4a08-b5c6-1d2e3f4a5b6c")
NDRRMC_EVENT_NS = uuid.UUID("a3f2c1d4-7e8b-4f09-b5a6-2c3d4e5f6a7b")
DROMIC_EVENT_NS = uuid.UUID("f7c14e82-3b9d-4a56-8e01-d2f5a7c93b1e")
CANONICAL_BASE = "https://sakuna.graph/common/"


def mint_canonical_iri(member_uris: frozenset[str]) -> URIRef:
    key = "|".join(sorted(member_uris))
    return URIRef(CANONICAL_BASE + str(uuid.uuid5(SKG_EVENT_NS, key)))


def event_uri(source: str, source_record_id: str) -> URIRef:
    uid = uuid.uuid5(SKG_EVENT_NS, source_record_id)
    return URIRef(SKG[f"{source}/{uid}"])


def row_iri(source: str, num: int) -> URIRef:
    return URIRef(SKG[f"{source}_row_{num}"])


def prov_iri(report: str) -> URIRef:
    normalized = (
        report.replace(" ", "_")
        .replace(".pdf", "")
        .replace(".xlsx", "")
        .replace(".docx", "")
        .replace(".", "")
        .replace(",", "")
        .replace("#", "")
    )
    return URIRef(SKG[normalized])


def sub_iri(event_id: URIRef, segment: str, r_id: str | None = None) -> URIRef:
    base = f"{event_id}/{segment}"
    return URIRef(f"{base}/{r_id}" if r_id is not None else base)


def incident_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "related_incident", row)


def aff_pop_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "affected_population", row)


def casualties_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "casualties", row)


def relief_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "relief", row)


def assistance_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "assistance", row)


def recovery_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "recovery", row)


def infra_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "infrastructure_damage", row)


def housing_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "housing_damage", row)


def agri_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "agriculture_damage", row)


def pevac_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "preemptive_evacuation", row)


def rnb_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "road_and_bridges_damage", row)


def power_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "power_disruption", row)


def comms_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "communication_line_disruption", row)


def doc_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "declaration_of_calamity", row)


def class_dis_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "class_suspension", row)


def work_dis_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "work_suspension", row)


def stranded_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "stranded_event", row)


def water_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "water_disruption", row)


def seaport_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "seaport_disruption", row)


def airport_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "airport_disruption", row)


def flight_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "flight_disruption", row)


def damage_gen_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "damage_general", row)


def climate_param_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "climate_parameter", row)


def warning_iri(event: URIRef, row: str | None = None) -> URIRef:
    return sub_iri(event, "warning", row)


def org_iri(slug: str) -> URIRef:
    return URIRef(SKG[f"org/{slug}"])


__all__ = [
    "CANONICAL_BASE",
    "DROMIC_EVENT_NS",
    "EMDAT_EVENT_NS",
    "GDA_NS",
    "NDRRMC_EVENT_NS",
    "SKG_EVENT_NS",
    "aff_pop_iri",
    "agri_iri",
    "airport_iri",
    "assistance_iri",
    "casualties_iri",
    "class_dis_iri",
    "climate_param_iri",
    "comms_iri",
    "damage_gen_iri",
    "doc_iri",
    "event_uri",
    "flight_iri",
    "housing_iri",
    "incident_iri",
    "infra_iri",
    "mint_canonical_iri",
    "org_iri",
    "pevac_iri",
    "power_iri",
    "prov_iri",
    "recovery_iri",
    "relief_iri",
    "rnb_iri",
    "row_iri",
    "seaport_iri",
    "stranded_iri",
    "sub_iri",
    "warning_iri",
    "water_iri",
    "work_dis_iri",
]

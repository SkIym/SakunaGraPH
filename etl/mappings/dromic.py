from dataclasses import fields, dataclass
from typing import List, Literal as TypingLiteral
from rdflib import URIRef, Literal, RDFS
from rdflib.namespace import RDF, XSD
from datetime import datetime

from .iris import aff_pop_iri, event_uri, prov_iri
from semantic_processing.org_resolver import ORG_RESOLVER
from .graph import CUR, SKG, Graph, PROV, add_monetary


@dataclass
class Event:
    eventName: str
    hasDisasterType: str
    startDate: datetime | None
    endDate: datetime | None
    id: str
    remarks: str
    hasBarangay: str | None
    hasLocation: URIRef | None   # events can be disaggregated

@dataclass
class Provenance:
    lastUpdateDate: datetime | None
    reportName: str
    reportLink: str | None
    obtainedDate: str | None

@dataclass
class AffectedPopulation:
    id: str
    affectedBarangays: int
    affectedFamilies: int
    affectedPersons: int
    displacedFamilies: int
    displacedPersons: int
    hasLocation: URIRef

AFF_POP_COL_MAP = {
    "NUMBER_OF_EVACUATION_CENTERS_ECs_CUM" : "evacuationCenters", # should be in Preemptive Evac
    "NUMBER_OF_DISPLACED_INSIDE_ECs_Families_CUM": "displacedFamiliesI",
    "NUMBER_OF_DISPLACED_OUTSIDE_ECs_Families_CUM": "displacedFamiliesO",
    "NUMBER_OF_DISPLACED_INSIDE_ECs_Persons_CUM": "displacedPersonsI",
    "NUMBER_OF_DISPLACED_OUTSIDE_ECs_Persons_CUM": "displacedPersonsO",
    "NUMBER_OF_AFFECTED_REGION_PROVINCE_MUNICIPALITY_Barangays": "affectedBarangays",
    "NUMBER_OF_AFFECTED_REGION_PROVINCE_MUNICIPALITY_Families": "affectedFamilies",
    "NUMBER_OF_AFFECTED_REGION_PROVINCE_MUNICIPALITY_Persons": "affectedPersons",
    "NUMBER_OF_DISPLACED_TOTAL_Families_CUM": "displacedFamilies",
    "TOTAL_DISPLACED_SERVED_Families_Total_Families_CUM": "displacedFamilies",
    "TOTAL_DISPLACED_SERVED_Families_REGION_PROVINCE_MUNICIPALITY_Total_Families_CUM": "displacedFamilies",
    "TOTAL_DISPLACED_SERVED_Persons_Total_Persons_CUM": "displacedPersons",
    "TOTAL_DISPLACED_SERVED_Persons_REGION_PROVINCE_MUNICIPALITY_Total_Persons_CUM": "displacedPersons",
    "NUMBER_OF_DISPLACED_TOTAL_Persons_CUM": "displacedPersons",
    # "REGION_PROVINCE_MUNICIPALITY": "affectedBarangays",
    "NUMBER_OF_AFFECTED_Families": "affectedFamilies",
    "NUMBER_OF_AFFECTED_Persons": "affectedPersons",
    "NUMBER_OF_DISPLACED_OUTSIDE_ECs_REGION_PROVINCE_MUNICIPALITY_Families_CUM": "displacedFamiliesO",
    "NUMBER_OF_DISPLACED_OUTSIDE_ECs_REGION_PROVINCE_MUNICIPALITY_Persons_CUM": "displacedPersonsO"
}

INCIDENT_MARKERS = ["incident", "conflict",  "disorganization"]
# rule-based incident v major event resolution
def _is_incident_by_name(name: str) -> bool:

    lowered = name.lower()
    return any(marker in lowered for marker in INCIDENT_MARKERS)

def event_mapping(g: Graph, ev: Event) -> URIRef:

    uri = event_uri("dromic", ev.id)

    eventType = SKG["Incident"] if _is_incident_by_name(ev.eventName) else SKG["MajorEvent"]

    g.add((uri, RDF.type, eventType))
    
    for f in fields(ev):

        if f.name == "id": continue

        value = getattr(ev, f.name)
        if value is None: continue

        if f.name == "hasLocation":

            # add catch for barangasys
            g.add((uri, SKG.hasLocation, URIRef(str(value))))
        elif f.name == "hasDisasterType":
            g.add((uri, SKG.hasDisasterType, URIRef(SKG[value])))
        elif f.type == datetime:
            g.add((uri, getattr(SKG, f.name), Literal(value, datatype=XSD.dateTime)))
        else:
            g.add((uri, getattr(SKG, f.name), Literal(value)))


    return uri

def prov_mapping(g: Graph, prov: Provenance, event_iri: URIRef):
    uri = prov_iri(prov.reportName)

    g.add((uri, RDF.type, SKG.Source))
    g.add((event_iri, PROV.wasDerivedFrom, uri))

    file_format = prov.reportName[prov.reportName.rfind('.') + 1:] 

    g.add((uri, SKG["format"], Literal(file_format)))

    for f in fields(prov):

        value = getattr(prov, f.name)
        if value is None: continue

        if f.type == datetime:
            g.add((uri, getattr(SKG, f.name), Literal(value, datatype=XSD.dateTime)))
        else:
            g.add((uri, getattr(SKG, f.name), Literal(value)))

def  aff_pop_mapping(g: Graph, aps: List[AffectedPopulation], event_iri: URIRef):

    for ap in aps:
        uri = aff_pop_iri(event_iri, ap.id)
        
        g.add((uri, RDF.type, SKG.AffectedPopulation))
        g.add((event_iri, SKG.hasAffectedPopulation, uri))

        for f in fields(ap):

            if f.name == "id": continue

            value = getattr(ap, f.name)
            if value is None: continue

            if f.name == "affectedBarangays" and value > 1:
                g.add((uri, SKG.affectedBarangays, Literal(value, datatype=XSD.int)))
            elif f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))
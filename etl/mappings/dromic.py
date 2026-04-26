from dataclasses import fields, dataclass
from typing import List
from rdflib import URIRef, Literal
from rdflib.namespace import RDF, XSD
from datetime import datetime

from .iris import aff_pop_iri, assistance_iri, event_uri, housing_iri, pevac_iri, prov_iri
from .graph import SKG, Graph, PROV, add_monetary


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


AFF_POP_TOKENS = {

    "evacuationCenters": ["evacuation", "center", "cum"],
    "displacedFamiliesI": ["displaced", "inside", "families", "cum"],
    "displacedFamiliesO": ["displaced", "outside", "families", "cum"],
    "displacedPersonsI": ["displaced", "inside", "persons", "cum"],
    "displacedPersonsO": ["displaced", "outside", "persons", "cum"],
    "affectedBarangays": ["affected", "barangays"],
    "affectedBarangays": ["affected", "brgy"],
    "affectedFamilies": ["affected", "families"],
    "affectedPersons": ["affected", "persons"],
    "displacedFamilies": ["displaced", "total", "families", "cum"],
    "displacedPersons": ["displaced", "total", "persons", "cum"],

    # "NUMBER_OF_EVACUATION_CENTERS_ECs_CUM" : "evacuationCenters", # should be in Preemptive Evac
    # "NUMBER_OF_DISPLACED_INSIDE_ECs_Families_CUM": "displacedFamiliesI",
    # "NUMBER_OF_DISPLACED_OUTSIDE_ECs_Families_CUM": "displacedFamiliesO",
    # "NUMBER_OF_DISPLACED_INSIDE_ECs_Persons_CUM": "displacedPersonsI",
    # "NUMBER_OF_DISPLACED_OUTSIDE_ECs_Persons_CUM": "displacedPersonsO",
    # "NUMBER_OF_AFFECTED_REGION_PROVINCE_MUNICIPALITY_Barangays": "affectedBarangays",
    # "NUMBER_OF_AFFECTED_REGION_PROVINCE_MUNICIPALITY_Families": "affectedFamilies",
    # "NUMBER_OF_AFFECTED_REGION_PROVINCE_MUNICIPALITY_Persons": "affectedPersons",
    # "NUMBER_OF_DISPLACED_TOTAL_Families_CUM": "displacedFamilies",
    # "TOTAL_DISPLACED_SERVED_Families_Total_Families_CUM": "displacedFamilies",
    # "TOTAL_DISPLACED_SERVED_Families_REGION_PROVINCE_MUNICIPALITY_Total_Families_CUM": "displacedFamilies",
    # "TOTAL_DISPLACED_SERVED_Persons_Total_Persons_CUM": "displacedPersons",
    # "TOTAL_DISPLACED_SERVED_Persons_REGION_PROVINCE_MUNICIPALITY_Total_Persons_CUM": "displacedPersons",
    # "NUMBER_OF_DISPLACED_TOTAL_Persons_CUM": "displacedPersons",
    # "NUMBER_OF_AFFECTED_Barangays": "affectedBarangays",
    # "NUMBER_OF_AFFECTED_Families": "affectedFamilies",
    # "NUMBER_OF_AFFECTED_Persons": "affectedPersons",
    # "NUMBER_OF_DISPLACED_OUTSIDE_ECs_REGION_PROVINCE_MUNICIPALITY_Families_CUM": "displacedFamiliesO",
    # "NUMBER_OF_DISPLACED_OUTSIDE_ECs_REGION_PROVINCE_MUNICIPALITY_Persons_CUM": "displacedPersonsO"
}

@dataclass
class Housing:
    id: str
    hasLocation: URIRef
    totallyDamagedHouses: int 
    partiallyDamagedHouses: int

HOUSING_TOKENS = {
    "totallyDamagedHouses": ["totally"],
    "partiallyDamagedHouses": ["partially"],
}

@dataclass
class Assistance:
    id: str
    hasLocation: URIRef
    contributingOrg: URIRef
    contributionAmount: float

ASSISTANCE_TOKENS = {
    "dswd": ["dswd"],
    "lgu": ["lgu"],
    "ngo": ["ngos"],
    "others": ["others"],
}
@dataclass
class PEvac:
    id: str
    hasLocation: URIRef
    evacuationCenters: int

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
                g.add((uri, SKG.affectedBarangays, Literal(value)))
            elif f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))

def housing_mapping(g: Graph, hs: List[Housing], event_iri: URIRef):

    for h in hs:
        uri = housing_iri(event_iri, h.id)
        
        g.add((uri, RDF.type, SKG.HousingDamage))
        g.add((event_iri, SKG.hasHousingDamage, uri))

        for f in fields(h):

            if f.name == "id": continue

            value = getattr(h, f.name)
            if value is None: continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))

def assistance_mapping(g: Graph, assis: List[Assistance], event_iri: URIRef):

    for a in assis:
        uri = assistance_iri(event_iri, a.id)

        g.add((uri, RDF.type, SKG.Assistance))
        g.add((event_iri, SKG.hasAssistance, uri))

        for f in fields(a):
            if f.name == "id": continue

            value = getattr(a, f.name)
            if value is None: continue

            if f.type == URIRef:
                g.add((uri, getattr(SKG, f.name), URIRef(value)))

            # just the contributionAmount left
            else:
                add_monetary(g, uri, SKG.contributionAmount, value, SKG.PHP_millions)

def pevac_mapping(g: Graph, pevac: List[PEvac], event_iri: URIRef):

    for p in pevac:
        uri = pevac_iri(event_iri, p.id)

        if not p.evacuationCenters or p.evacuationCenters == 0:
            continue

        g.add((uri, RDF.type, SKG.PreemptiveEvacuation))
        g.add((event_iri, SKG.hasPreemptiveEvacuation, uri))

        for f in fields(p):
            if f.name == "id": continue

            value = getattr(p, f.name)
            if value is None: continue

            if f.type == URIRef:
                g.add((uri, getattr(SKG, f.name), URIRef(value)))

            # just the evacCenters left
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))
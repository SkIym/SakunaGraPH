# NDRRMC MAPPINGS HERE (rdflib)
from typing import List
from rdflib import URIRef, Literal
from rdflib.namespace import RDF, XSD
from datetime import datetime
from dataclasses import dataclass
from .graph import SKG, Graph, PROV
from .iris import aff_pop_iri, event_iri, incident_iri, prov_iri

@dataclass
class Event:
    eventName: str
    hasType: str
    startDate: datetime | None
    endDate: datetime | None
    id: str
    remarks: str | None = None
    
def event_mapping(g: Graph, ev: Event) -> URIRef:
    uri = event_iri(ev.id)
    g.add((
        uri, 
        RDF.type, 
        SKG["MajorEvent"]
    ))
    
    g.add((
        uri, 
        URIRef(SKG["eventName"]), 
        Literal(ev.eventName)
    ))
    g.add((
        uri,
        URIRef(SKG["hasType"]),
        URIRef(SKG[ev.hasType])
    ))

    if ev.startDate:
        g.add((
            uri, 
            URIRef(SKG["startDate"]), 
            Literal(
                ev.startDate, 
                datatype=XSD.dateTime)
        ))

    if ev.endDate:
        g.add((
            uri, 
            URIRef(SKG["endDate"]), 
            Literal(ev.endDate, 
                    datatype=XSD.dateTime)
        ))

    if ev.remarks:
        g.add((uri, 
               URIRef(SKG["remarks"]), 
               Literal(ev.remarks)
        ))
    
    return uri

@dataclass
class Provenance:
    lastUpdateDate: datetime
    reportName: str
    obtainedDate: str | None = None
    reportLink: str | None = None

def prov_mapping(g: Graph, prov: Provenance, event_iri: URIRef):
    uri = prov_iri(prov.reportName)

    g.add((
        uri,
        RDF.type,
        URIRef(SKG["Source"])
    ))

    g.add((
        event_iri,
        URIRef(SKG["fromSource"]),
        uri
    ))

    g.add((
        uri,
        URIRef(SKG["format"]),
        Literal("pdf")
    ))

    g.add((
        uri,
        URIRef(SKG["lastUpdateDate"]),
        Literal(prov.lastUpdateDate,
                datatype=XSD.dateTime)
    ))

    g.add((
        uri,
        URIRef(SKG["reportName"]),
        Literal(prov.reportName)
    ))

    if prov.obtainedDate:

        g.add((
            uri,
            URIRef(SKG["obtainedDate"]),
            Literal(prov.obtainedDate,
                    datatype=XSD.dateTime)
        ))
    
    if prov.reportLink:

        g.add((
            uri,
            URIRef(SKG["reportLink"]),
            Literal(prov.reportLink)
        ))
    
    g.add((
        uri,
        URIRef(PROV["wasAttributedTo"]),
        URIRef(SKG["NDRRMC"])
    ))

    g.add((
        uri,
        URIRef(PROV["wasGeneratedBy"]),
        URIRef(SKG["ndrrmc_website_access"])
    ))

@dataclass
class Incident:
    id: str
    incidentActionsTaken: str | None
    incidentDescription: str | None
    startDate: datetime
    endDate: datetime
    hasLocation: URIRef
    hasBarangay: str | None
    hasType: str
    remarks: str | None

INCIDENT_COLUMN_MAPPINGS = {
    "REGION_|_PROVINCE_|_CITY_MUNICIPALITY_|\nBARANGAY": "QTY",
    "REGION_|_PROVINCE_|_CITY\n_MUNICIPALITY_|_BARANGAY": "QTY",
    "Column_2": "Type of Incident",
    "Column_3": "Date",
    "Column_4": "Time",
    "Column_5": "Description",
    "Column_6": "Actions Taken",
    "Column_7": "Remarks",
    "Column_8": "Status",

}

def incident_mapping(g: Graph, inci: List[Incident], event_iri: URIRef):

    for i in inci:
        uri = incident_iri(event_iri, i.id)

        # rdf:type
        g.add((
            uri,
            RDF.type,
            SKG.Incident
        ))

        # Link incident to its parent event
        g.add((
            event_iri,
            SKG.hasRelatedIncident,
            uri
        ))

        # --- Textual / descriptive properties ---
        if i.incidentDescription:
            g.add((
                uri,
                SKG.incidentDescription,
                Literal(i.incidentDescription)
            ))

        if i.incidentActionsTaken:
            g.add((
                uri,
                SKG.incidentActionsTaken,
                Literal(i.incidentActionsTaken)
            ))

        if i.remarks:
            g.add((
                uri,
                SKG.remarks,
                Literal(i.remarks)
            ))

        # --- Temporal properties ---
        g.add((
            uri,
            SKG.startDate,
            Literal(i.startDate, datatype=XSD.dateTime)
        ))

        if i.endDate:
            g.add((
                uri,
                SKG.endDate,
                Literal(i.endDate, datatype=XSD.dateTime)
            ))

        # --- Classification / location ---
        g.add((
            uri,
            SKG.hasType,
            URIRef(SKG[i.hasType])
        ))

        g.add((
            uri,
            SKG.hasLocation,
            URIRef(i.hasLocation)
        ))

        if i.hasBarangay:
            g.add((
                uri,
                SKG.hasBarangay,
                Literal(i.hasBarangay)
            ))

AFF_POP_COL_MAP = {
    "REGION_|_PROVINCE_|_CITY_\nMUNICIPALITY_|_BARANGAY": "QTY",
    "NO_OF_AFFECTED_Brgys": "affectedBarangays",
    "NO_OF_AFFECTED_Families": "affectedFamilies",
    "NO_OF_AFFECTED_Persons": "affectedPersons",
    "TOTAL_SERVED_CURRENT_Inside_+_Outside_Persons_CUM": "displacedPersons",
    "TOTAL_SERVED_CURRENT_Inside_+_Outside_Families_CUM": "displacedFamilies",
    "TOTAL_SERVED_Inside_+_Outside_Persons_CUM": "displacedPersons",
    "TOTAL_SERVED_Inside_+_Outside_Families_CUM": "displacedFamilies",
    "No_of_ECs_Persons_CUM": "evacuationCenters",
}

@dataclass
class AffectedPopulation:
    id: str
    affectedBarangays: int
    affectedFamilies: int
    affectedPersons: int
    displacedFamilies: int
    displacedPersons: int
    hasLocation: URIRef
    hasBarangay: str | None

def aff_pop_mapping(g: Graph, aps: List[AffectedPopulation], event_iri: URIRef):

    for ap in aps:
        uri = aff_pop_iri(event_iri, ap.id)

        # rdf:type
        g.add((
            uri,
            RDF.type,
            SKG.AffectedPopulation
        ))

        # Link impact to parent event
        g.add((
            event_iri,
            SKG.hasImpact,
            uri
        ))

        if int(ap.affectedBarangays) > 1:

            g.add((
                uri,
                SKG.affectedBarangays,
                Literal(ap.affectedBarangays, datatype=XSD.int)
            ))
        
        else:

            g.add((
                uri,
                SKG.hasBarangay,
                Literal(ap.hasBarangay)
            ))

        g.add((
                uri,
                SKG.affectedFamilies,
                Literal(ap.affectedFamilies, datatype=XSD.int)
        ))

        g.add((
                uri,
                SKG.affectedPersons,
                Literal(ap.affectedPersons, datatype=XSD.int)
        ))

        g.add((
                uri,
                SKG.displacedFamilies,
                Literal(ap.displacedFamilies, datatype=XSD.int)
        ))

        g.add((
                uri,
                SKG.displacedPersons,
                Literal(ap.displacedPersons, datatype=XSD.int)
        ))

        g.add((
                uri,
                SKG.hasLocation,
                URIRef(ap.hasLocation)
        ))
        

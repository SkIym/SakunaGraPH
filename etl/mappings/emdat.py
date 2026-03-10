from dataclasses import dataclass, fields
from datetime import datetime, date
from polars import DataFrame
from rdflib import RDF, XSD, Literal, URIRef, Graph
from .graph import PROV, SKG
from .iris import aff_pop_iri, assistance_iri, casualties_iri, damage_gen_iri, event_iri, prov_iri, recovery_iri
from typing import Type, TypeVar, Literal as TypingLiteral


T = TypeVar("T")

@dataclass
class Event:
    eventName: str | None
    hasDisasterType: str
    hasDisasterSubtype: str | None
    hasLocation: URIRef 
    startDate: date
    endDate: date
    id: str
    lastUpdateDate: date
    entryDate: date

@dataclass
class Assistance:
    id: str
    contributionAID: float | None
    internationalOrgsPresent: str

@dataclass
class Recovery:
    id: str
    postStructureCost: float | None

@dataclass
class DamageGeneral:
    id: str
    insuredDamage: float | None
    generalDamageAmount: float | None
    cpi: float | None

@dataclass
class Casualties:
    id: str
    dead: int | None
    injured: int | None

@dataclass
class AffectedPopulation:
    id: str
    affectedPersons: int | None
    displacedPersons: int | None

@dataclass
class Source:
    format: str
    obtainedDate: date
    reportName: str


def source_mapping(g: Graph, s: Source) -> URIRef:
    """
    Called once per file
    """
    
    uri = prov_iri(s.reportName)

    g.add((uri, RDF.type, SKG.Source))
    g.add((uri, URIRef(SKG["format"]), Literal(s.format)))
    g.add((uri, SKG.reportLink, Literal("https://public.emdat.be/data")))
    g.add((uri, SKG.reportName, Literal(s.reportName)))
    g.add((uri, SKG.obtainedDate, Literal(s.obtainedDate, datatype=XSD.dateTime)))
    g.add((uri, PROV.wasGeneratedBy, URIRef(SKG["em-dat_website_access"])))
    g.add((uri, PROV.wasDerivedFrom, URIRef(SKG["EM-DAT"])))

    return uri

def event_mapping(rs: list[Event], g: Graph, src_uri: URIRef):

    for r in rs:

        uri = event_iri(r.id)

        g.add((uri, RDF.type, SKG.MajorEvent)) # rdf type
        g.add((uri, PROV.wasDerivedFrom, src_uri)) # link source

        for f in fields(r):

            if f.name == "id": continue

            value = getattr(r, f.name)
            if value is None or value == "":
                continue  
            
            if f.name == "hasLocation":
                value = str(value)
                locs = value.split("|")
                for loc in locs:
                    g.add((uri, SKG.hasLocation, URIRef(loc)))
            elif f.name == "startDate":
                g.add((uri, SKG.startDate, Literal(value, datatype=XSD.date)))
            elif f.name == "endDate":
                g.add((uri, SKG.endDate, Literal(value, datatype=XSD.date)))
            elif f.name == "lastUpdateDate":
                g.add((uri, SKG.lastUpdateDate, Literal(value, datatype=XSD.date)))
            elif f.name == "entryDate":
                g.add((uri, SKG.entryDate, Literal(value, datatype=XSD.date)))
            elif f.name == "hasDisasterType":
                g.add((uri, SKG.hasDisasterType, URIRef(SKG[value])))
            elif f.name == "hasDisasterSubtype":
                value = str(value)
                subtypes = value.split("|")
                for s in subtypes:
                    s = s.strip()
                    g.add((uri, SKG.hasDisasterSubtype, URIRef(SKG[s])))

            else:
                g.add((uri, getattr(SKG, f.name), Literal(value))) 
        
def assistance_mapping(rs: list[Assistance], g: Graph):

    for r in rs:

        event_uri = event_iri(r.id)
        uri = assistance_iri(event_uri, "1")

        if r.contributionAID is None and r.internationalOrgsPresent.lower() == "no": continue

        g.add((uri, RDF.type, SKG.Assistance)) # rdf type
        g.add((event_uri, SKG.hasAssistance, uri)) # link event

        for f in fields(r):

            if f.name == "id": continue

            value = getattr(r, f.name)
            if value is None or value == "":
                continue  
            
            if f.name == "contributionAID":
                g.add((uri, SKG.contributionAID, Literal(value, datatype=XSD.decimal)))

            elif f.name == "internationalOrgsPresent":
                value = str(value)
                if value.lower() == "yes":
                    g.add((uri, SKG.internationalOrgsPresent, Literal("OFDA/BHA")))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value))) 

def recovery_mapping(rs: list[Recovery], g: Graph):

    for r in rs:

        event_uri = event_iri(r.id)
        uri = recovery_iri(event_uri, "1")

        g.add((uri, RDF.type, SKG.Recovery)) # rdf type
        g.add((event_uri, SKG.hasRecovery, uri)) # link event

        if r.postStructureCost:
            g.add((uri, SKG.postStructureCost, Literal(r.postStructureCost, datatype=XSD.decimal)))

def damage_gen_mapping(rs: list[DamageGeneral], g: Graph):

    for r in rs:

        event_uri = event_iri(r.id)
        uri = damage_gen_iri(event_uri, "1")

        g.add((uri, RDF.type, SKG.DamageGeneral)) # rdf type
        g.add((event_uri, SKG.hasDamageGeneral, uri)) # link event

        if r.insuredDamage:
            g.add((uri, SKG.insuredDamage, Literal(r.insuredDamage, datatype=XSD.decimal)))

        if r.generalDamageAmount:
            g.add((uri, SKG.generalDamageAmount, Literal(r.generalDamageAmount, datatype=XSD.decimal)))
        
        if r.cpi:
            g.add((uri, SKG.cpi, Literal(r.cpi, datatype=XSD.decimal)))

CasualtDatatype = TypingLiteral["DEAD", "INJURED", "MISSING"]

def casualties_mapping(rs: list[Casualties], g: Graph):

    for r in rs:

        event_uri = event_iri(r.id)

        id = 0

        if r.dead:
            uri = casualties_iri(event_uri, str(id+1))

            g.add((uri, RDF.type, SKG.Casualties)) # rdf type
            g.add((event_uri, SKG.hasCasualties, uri)) # link event
            g.add((uri, SKG.casualtyCount, Literal(r.dead)))
            g.add((uri, SKG.casualtyType, Literal("DEAD", datatype=SKG.casualtyDatatype)))

            id += 1
        
        if r.injured:
            uri = casualties_iri(event_uri, str(id+1))

            g.add((uri, RDF.type, SKG.Casualties)) # rdf type
            g.add((event_uri, SKG.hasCasualties, uri)) # link event
            g.add((uri, SKG.casualtyCount, Literal(r.injured)))
            g.add((uri, SKG.casualtyType, Literal("INJURED", datatype=SKG.casualtyDatatype)))

def aff_pop_mapping(rs: list[AffectedPopulation], g: Graph):

    for r in rs:

        event_uri = event_iri(r.id)
        uri = aff_pop_iri(event_uri, "1")

        g.add((uri, RDF.type, SKG.AffectedPopulation)) # rdf type
        g.add((event_uri, SKG.hasAffectedPopulation, uri)) # link event

        if r.affectedPersons:
            g.add((uri, SKG.affectedPersons, Literal(r.affectedPersons)))

        if r.displacedPersons:
            g.add((uri, SKG.displacedPersons, Literal(r.displacedPersons)))
        
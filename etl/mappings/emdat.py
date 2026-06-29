from dataclasses import dataclass, fields
from datetime import datetime, date
from polars import DataFrame
from rdflib import RDF, RDFS, XSD, Literal, URIRef, Graph
from .graph import BAW, ORG, PROV, SKG, SKOS, add_monetary
from .iris import aff_pop_iri, assistance_iri, casualties_iri, climate_param_iri, damage_gen_iri, event_uri, org_iri, prov_iri, recovery_iri, row_iri
from typing import Type, TypeVar, Literal as TypingLiteral
from semantic_processing.org_resolver import ORG_RESOLVER

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
    rowNumber: int
    hasMagnitude: float | None = None
    hasMagnitudeScale: str | None = None
    latitude: float | None = None
    longitude: float | None = None

@dataclass
class Assistance:
    id: str
    contributionAID: float | None
    internationalOrgsPresent: str
    hasLocation: URIRef | None

@dataclass
class Recovery:
    id: str
    postStructureCost: float | None
    hasLocation: URIRef | None


@dataclass
class DamageGeneral:
    id: str
    insuredDamage: float | None
    generalDamageAmount: float | None
    cpi: float | None
    hasLocation: URIRef | None


@dataclass
class Casualties:
    id: str
    dead: int | None
    injured: int | None
    hasLocation: URIRef | None


@dataclass
class AffectedPopulation:
    id: str
    affectedPersons: int | None
    displacedPersons: int | None
    hasLocation: URIRef | None


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
    g.add((uri, PROV.wasAttributedTo, ORG.CRED))

    return uri

def _is_incident(loc: URIRef, dtype: str) -> bool:

    if not loc: return False
    print(loc)
    if ("|" not in loc
        and all(
            l not in loc
            for l in 
            ["Philippines", "Mindanao", "Visayas", "Luzon"]
        )
        and all(
            t not in dtype.lower()
            for t in
            ["cyclone", "storm", "ground"]
        )
    ): return True

    return False


def _add_magnitude_parameter(g: Graph, event_iri: URIRef, value: float | None, scale: str | None) -> None:
    if value is None:
        return

    uri = climate_param_iri(event_iri, "magnitude")
    g.add((uri, RDF.type, BAW.ClimateParameter))
    g.add((event_iri, BAW.hasClimateParameterMeasurement, uri))
    g.add((uri, BAW.isOfClimateParameterType, SKG.Magnitude))
    g.add((uri, BAW.hasValue, Literal(float(value), datatype=XSD.float)))
    if scale:
        g.add((uri, BAW.hasUnit, Literal(str(scale))))


def event_mapping(rs: list[Event], g: Graph, src_uri: URIRef):

    for r in rs:

        uri = event_uri("emdat", r.id)

        # print(r)
        if _is_incident(r.hasLocation, r.hasDisasterType): # add type
            g.add((uri, RDF.type, SKG.Incident))
        else:
            g.add((uri, RDF.type, SKG.MajorEvent)) # rdf type

        
        # row number entity
        row_uri = row_iri("emdat", r.rowNumber)
        g.add((row_uri, RDF.type, PROV.Entity))
        g.add((row_uri, PROV.value, Literal(r.rowNumber)))
        g.add((row_uri, PROV.wasDerivedFrom, src_uri)) # link source

        g.add((uri, PROV.wasDerivedFrom, row_uri)) # link source row

        _add_magnitude_parameter(g, uri, r.hasMagnitude, r.hasMagnitudeScale)

        for f in fields(r):

            if f.name == "id" or f.name == "rowNumber": continue

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
            elif f.name in {"hasMagnitude", "hasMagnitudeScale"}:
                continue
            elif f.name in {"latitude", "longitude"}:
                g.add((uri, getattr(SKG, f.name), Literal(float(value), datatype=XSD.decimal)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value))) 
        
def assistance_mapping(rs: list[Assistance], g: Graph):
    for r in rs:

        e_uri = event_uri("emdat", r.id)
        uri = assistance_iri(e_uri)

        if r.contributionAID is None and r.internationalOrgsPresent.lower() == "no": continue

        g.add((uri, RDF.type, SKG.Assistance)) # rdf type
        g.add((e_uri, SKG.hasAssistance, uri)) # link event

        for f in fields(r):

            if f.name == "id": continue

            value = getattr(r, f.name)
            if value is None or value == "":
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(value)))

            elif f.name == "contributionAID":
                add_monetary(g, uri, SKG.contributionAmount, value, SKG.USD_thousands)
                g.add((uri, SKG.contributingOrg, ORG.Unspecified))
                g.add((uri, SKOS.note, Literal("The total amount (in thousands of US$ at the time of the report) of contributions for immediate relief activities to the country in response to the disaster, sourced from the Financial Tracking System of OCHA (1992–2015). Not maintained after 2015.")))

            elif f.name == "internationalOrgsPresent":
                value = str(value)
                if value.lower() == "yes":
                    g.add((uri, SKG.internationalOrgsPresent, Literal("OFDA/BHA")))
                    # Resolve OFDA/BHA to org IRI
                    for org in ORG_RESOLVER.split_and_resolve(str(value)):
                        g.add((uri, SKG.contributingOrg, org))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value))) 

def recovery_mapping(rs: list[Recovery], g: Graph):

    for r in rs:

        e_uri = event_uri("emdat", r.id)
        uri = recovery_iri(e_uri)

        g.add((uri, RDF.type, SKG.Recovery)) # rdf type
        g.add((e_uri, SKG.hasRecovery, uri)) # link event

        if r.hasLocation and r.postStructureCost:
            g.add((uri, SKG.hasLocation, URIRef(r.hasLocation)))

        if r.postStructureCost:
            add_monetary(g, uri, SKG.postStructureCost, r.postStructureCost, SKG.USD_thousands)
        # g.add((uri, SKG.postStructureCost, Literal(r.postStructureCost, datatype=XSD.decimal)))

def damage_gen_mapping(rs: list[DamageGeneral], g: Graph):

    for r in rs:

        e_uri = event_uri("emdat", r.id)
        uri = damage_gen_iri(e_uri)

        g.add((uri, RDF.type, SKG.DamageGeneral)) # rdf type
        g.add((e_uri, SKG.hasDamageGeneral, uri)) # link event

        if r.hasLocation and (r.insuredDamage or r.generalDamageAmount or r.cpi):
            g.add((uri, SKG.hasLocation, URIRef(r.hasLocation)))

        if r.insuredDamage:
            # g.add((uri, SKG.insuredDamageAmount, Literal(r.insuredDamage, datatype=XSD.decimal)))
            add_monetary(g, uri, SKG.insuredDamageAmount, r.insuredDamage, SKG.USD_thousands)

        if r.generalDamageAmount:
            add_monetary(g, uri, SKG.generalDamageAmount, r.generalDamageAmount, SKG.USD_thousands)
            # g.add((uri, SKG.generalDamageAmount, Literal(r.generalDamageAmount, datatype=XSD.decimal)))
        
        if r.cpi:
            g.add((uri, SKG.cpi, Literal(r.cpi, datatype=XSD.decimal)))

CasualtDatatype = TypingLiteral["DEAD", "INJURED", "MISSING"]

def casualties_mapping(rs: list[Casualties], g: Graph):

    for r in rs:

        e_uri = event_uri("emdat", r.id)

        id = 0

        loc = ""
        if r.hasLocation:
            loc = URIRef(r.hasLocation)

        if r.dead:
            uri = casualties_iri(e_uri, str(id+1))

            if loc: 
                g.add((uri, SKG.hasLocation, loc)) 

            g.add((uri, RDF.type, SKG.Casualties)) # rdf type
            g.add((e_uri, SKG.hasCasualties, uri)) # link event
            g.add((uri, SKG.casualtyCount, Literal(r.dead)))
            g.add((uri, SKG.isOfCasualtyType, SKG.Dead))

            id += 1
        
        if r.injured:
            uri = casualties_iri(e_uri, str(id+1))

            if loc: 
                g.add((uri, SKG.hasLocation, loc)) 

            g.add((uri, RDF.type, SKG.Casualties)) # rdf type
            g.add((e_uri, SKG.hasCasualties, uri)) # link event
            g.add((uri, SKG.casualtyCount, Literal(r.injured)))
            g.add((uri, SKG.isOfCasualtyType, SKG.Injured))

def aff_pop_mapping(rs: list[AffectedPopulation], g: Graph):

    for r in rs:

        e_uri = event_uri("emdat", r.id)
        uri = aff_pop_iri(e_uri)

        g.add((uri, RDF.type, SKG.AffectedPopulation)) # rdf type
        g.add((e_uri, SKG.hasAffectedPopulation, uri)) # link event

        if r.affectedPersons:
            g.add((uri, SKG.affectedPersons, Literal(r.affectedPersons)))

        if r.displacedPersons:
            g.add((uri, SKG.displacedPersons, Literal(r.displacedPersons)))

        if r.hasLocation and (r.affectedPersons or r.displacedPersons):
            g.add((uri, SKG.hasLocation, URIRef(r.hasLocation)))
        

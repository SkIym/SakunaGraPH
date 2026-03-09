from dataclasses import dataclass
from datetime import datetime, date

from rdflib import RDF, XSD, Literal, URIRef, Graph

from .graph import PROV, SKG
from .iris import prov_iri

@dataclass
class Event:
    eventName: str
    hasDisasterType: str
    hasDisasterSubType: str
    hasLocation: URIRef 
    startDate: date
    endDate: date
    id: str
    lastUpdateDate: date
    entryDate: date
    remarks: str | None = None
    
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



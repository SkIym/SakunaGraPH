from dataclasses import fields, dataclass
from typing import List, Literal as TypingLiteral
from rdflib import URIRef, Literal, RDFS
from rdflib.namespace import RDF, XSD
from datetime import datetime

from .iris import event_uri
from semantic_processing.org_resolver import ORG_RESOLVER
from .graph import CUR, SKG, Graph, PROV, add_monetary


@dataclass
class Event:
    eventName: str
    hasDisasterType: str
    startDate: datetime
    endDate: datetime
    id: str
    remarks: str
    hasLocation: URIRef | None   # events can be incidents or 


INCIDENT_MARKERS = ["incident", "conflict", "disorganization"]
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
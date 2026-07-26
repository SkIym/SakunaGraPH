from dataclasses import fields, dataclass
from typing import List
from rdflib import URIRef, Literal
from rdflib.namespace import DCTERMS, RDF, XSD
from datetime import datetime

from sakunagraph_etl.rdf.iris import aff_pop_iri, assistance_iri, event_uri, housing_iri, pevac_iri, prov_iri
from sakunagraph_etl.rdf.graph import ORG, SKG, Graph, PROV, add_monetary
from .identity import report_version_id
from .versioning import source_recency_key


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
    legacy_id: str | None = None

@dataclass
class ReportVersion:
    reportName: str
    reportLink: str | None = None
    downloadUrl: str | None = None
    obtainedDate: str | None = None
    postDate: str | None = None
    sha256: str | None = None
    lastUpdateDate: datetime | None = None


@dataclass
class Provenance:
    lastUpdateDate: datetime | None
    reportName: str
    reportLink: str | None
    obtainedDate: str | None
    downloadUrl: str | None = None
    postDate: str | None = None
    sha256: str | None = None
    versions: tuple[ReportVersion, ...] = ()

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

    "evacuationCenters": [
        ["evacuation", "center", "cum"],
        ["evacuation", "center"],

    ],
    "displacedFamiliesI": [
        ["displaced", "inside", "families", "cum"],
        ["inside", "families", "evacuation"]
    ],
    "displacedFamiliesO": [
        ["displaced", "outside", "families", "cum"],
        ["outside", "families", "evacuation"]
    ],
    "displacedPersonsI": [
        ["displaced", "inside", "persons", "cum"],
        ["inside", "person", "evacuation"]
    ],
    "displacedPersonsO": [
        ["displaced", "outside", "persons", "cum"],
        ["outside", "person", "evacuation"]
    ],
    "affectedBarangays": [["affected", "barangays"]],
    "affectedBarangays": [["affected", "brgy"]],
    "affectedFamilies": [["affected", "families"]],
    "affectedPersons": [["affected", "person"]],
    "displacedFamilies": [
        ["displaced", "total", "families", "cum"],
        ["served", "total", "families", "cum"],
        ["served", "total", "families"],
        ["evacuee", "families"]
    ],
    "displacedPersons": [
        ["displaced", "total", "person", "cum"],
        ["served", "total", "persons", "cum"],
        ["served", "total", "persons"],
        ["evacuee", "person"]
    ]
}

@dataclass
class Housing:
    id: str
    hasLocation: URIRef
    totallyDamagedHouses: int 
    partiallyDamagedHouses: int

HOUSING_TOKENS = {
    "totallyDamagedHouses": [["totally"]],
    "partiallyDamagedHouses": [["partially"]],
}

@dataclass
class Assistance:
    id: str
    hasLocation: URIRef
    contributingOrg: URIRef
    contributionAmount: float

ASSISTANCE_TOKENS = {
    "dswd": [["dswd"]],
    "lgu": [["lgu"]],
    "ngo": [["ngo"]],
    "others": [["others"]],
    "nga": [["nga"]],
}

ORG_MAPPING = {

    "dswd": "https://sakuna.ph/org/DSWD",
    "lgu": "https://sakuna.ph/org/LGU",
    "ngo": "https://sakuna.ph/org/NGO",
    "others": "https://sakuna.ph/org/Unspecified",
    "nga": "https://sakuna.ph/org/NGA"
}

@dataclass
class PEvac:
    id: str
    hasLocation: URIRef
    evacuationCenters: int

@dataclass
class Stranded:
    id: str
    hasLocation: URIRef
    strandedVessels: int | None
    strandedMotorBancas: int | None
    strandedPassengers: int | None
    strandedRollingCargoes: int | None
    portOrTerminalName: str | None

STRANDED_TOKENS = {
    "strandedPassengers": [
        ["strandees"],
        ["passenger"],
        ["stranded", "passengers"]
    ],
    "strandedVessel": [
        ["vehicle"],
        ["vessel"]
    ],
    "strandedMotorBancas": [
        ["motor", "banca"]
    ],
    "strandedRollingCargoes": [
        ["others"]
    ],
    "port": [
        ["port"],
        ["terminal"]
    ],
}


INCIDENT_MARKERS = ["incident", "conflict",  "disorganization"]
# rule-based incident v major event resolution
def _is_incident_by_name(name: str) -> bool:

    lowered = name.lower()
    return any(marker in lowered for marker in INCIDENT_MARKERS)

def event_mapping(g: Graph, ev: Event) -> URIRef:

    uri = event_uri("dromic", ev.id)
    if ev.legacy_id and ev.legacy_id != ev.id:
        g.add((uri, DCTERMS.replaces, event_uri("dromic", ev.legacy_id)))

    eventType = SKG["Incident"] if _is_incident_by_name(ev.eventName) else SKG["MajorEvent"]

    g.add((uri, RDF.type, eventType))
    
    for f in fields(ev):

        if f.name in {"id", "legacy_id"}: continue

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

def _report_version_iri(version: ReportVersion) -> URIRef:
    identifier = report_version_id(
        version.reportLink,
        version.reportName,
        sha256=version.sha256,
        post_date=version.postDate,
        downloaded_at=version.obtainedDate,
        download_url=version.downloadUrl,
    )
    return URIRef(SKG[f"dromic/report-version/{identifier}"])


def _map_report_version(g: Graph, version: ReportVersion) -> URIRef:
    uri = _report_version_iri(version)
    g.add((uri, RDF.type, SKG.Source))
    g.add((uri, RDF.type, PROV.Entity))
    g.add((uri, PROV.wasAttributedTo, ORG.DROMIC))

    file_format = version.reportName.rsplit(".", 1)[-1] if "." in version.reportName else ""
    if file_format:
        g.add((uri, SKG["format"], Literal(file_format)))
    g.add((uri, SKG.reportName, Literal(version.reportName)))
    if version.reportLink:
        g.add((uri, SKG.reportLink, Literal(version.reportLink)))
    if version.downloadUrl:
        g.add((uri, SKG.downloadUrl, Literal(version.downloadUrl)))
    if version.obtainedDate:
        g.add((uri, SKG.obtainedDate, Literal(version.obtainedDate)))
    if version.postDate:
        g.add((uri, SKG.postDate, Literal(version.postDate, datatype=XSD.date)))
    if version.sha256:
        g.add((uri, SKG.sha256, Literal(version.sha256)))
    if version.lastUpdateDate:
        g.add((
            uri,
            SKG.lastUpdateDate,
            Literal(version.lastUpdateDate, datatype=XSD.dateTime),
        ))
    return uri


def _legacy_prov_mapping(g: Graph, prov: Provenance, event_iri: URIRef) -> URIRef:
    """Preserve historical RDF for parsed data without acquisition versions."""

    uri = prov_iri(prov.reportName)

    g.add((uri, RDF.type, SKG.Source))
    g.add((event_iri, PROV.wasDerivedFrom, uri))
    g.add((uri, PROV.wasAttributedTo, ORG.DROMIC))

    file_format = prov.reportName[prov.reportName.rfind('.') + 1:] 

    g.add((uri, SKG["format"], Literal(file_format)))

    for f in fields(prov):

        value = getattr(prov, f.name)
        if value is None: continue

        if f.type == datetime:
            g.add((uri, getattr(SKG, f.name), Literal(value, datatype=XSD.dateTime)))
        elif f.name not in {"versions", "downloadUrl", "postDate", "sha256"}:
            g.add((uri, getattr(SKG, f.name), Literal(value)))
    return uri


def prov_mapping(g: Graph, prov: Provenance, event_iri: URIRef) -> URIRef:
    if not prov.versions:
        return _legacy_prov_mapping(g, prov, event_iri)

    current = ReportVersion(
        reportName=prov.reportName,
        reportLink=prov.reportLink,
        downloadUrl=prov.downloadUrl,
        obtainedDate=prov.obtainedDate,
        postDate=prov.postDate,
        sha256=prov.sha256,
        lastUpdateDate=prov.lastUpdateDate,
    )
    by_iri = {
        _report_version_iri(version): version
        for version in (*prov.versions, current)
    }
    ordered = sorted(
        by_iri.items(),
        key=lambda item: source_recency_key({
            "postDate": item[1].postDate,
            "obtainedDate": item[1].obtainedDate,
            "lastUpdateDate": (
                item[1].lastUpdateDate.isoformat()
                if item[1].lastUpdateDate
                else None
            ),
            "reportName": item[1].reportName,
        }),
    )
    for uri, version in ordered:
        _map_report_version(g, version)
    for (older_uri, _), (newer_uri, _) in zip(ordered, ordered[1:]):
        g.add((newer_uri, PROV.wasRevisionOf, older_uri))

    current_uri = _report_version_iri(current)
    g.add((event_iri, PROV.wasDerivedFrom, current_uri))
    return current_uri

def aff_pop_mapping(
    g: Graph,
    aps: List[AffectedPopulation],
    event_iri: URIRef,
    source_iri: URIRef | None = None,
):

    for ap in aps:
        uri = aff_pop_iri(event_iri, ap.id)
        
        g.add((uri, RDF.type, SKG.AffectedPopulation))
        g.add((event_iri, SKG.hasAffectedPopulation, uri))
        if source_iri is not None:
            g.add((uri, PROV.wasDerivedFrom, source_iri))

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

def housing_mapping(
    g: Graph,
    hs: List[Housing],
    event_iri: URIRef,
    source_iri: URIRef | None = None,
):

    for h in hs:
        uri = housing_iri(event_iri, h.id)
        
        g.add((uri, RDF.type, SKG.HousingDamage))
        g.add((event_iri, SKG.hasHousingDamage, uri))
        if source_iri is not None:
            g.add((uri, PROV.wasDerivedFrom, source_iri))

        for f in fields(h):

            if f.name == "id": continue

            value = getattr(h, f.name)
            if value is None: continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))

def assistance_mapping(
    g: Graph,
    assis: List[Assistance],
    event_iri: URIRef,
    source_iri: URIRef | None = None,
):

    for a in assis:
        uri = assistance_iri(event_iri, a.id)

        g.add((uri, RDF.type, SKG.Assistance))
        g.add((event_iri, SKG.hasAssistance, uri))
        if source_iri is not None:
            g.add((uri, PROV.wasDerivedFrom, source_iri))

        for f in fields(a):
            if f.name == "id": continue

            value = getattr(a, f.name)
            if value is None: continue

            if f.type == URIRef:
                g.add((uri, getattr(SKG, f.name), URIRef(value)))

            # just the contributionAmount left
            else:
                add_monetary(g, uri, SKG.contributionAmount, value, SKG.PHP_millions)

def pevac_mapping(
    g: Graph,
    pevac: List[PEvac],
    event_iri: URIRef,
    source_iri: URIRef | None = None,
):

    for p in pevac:
        uri = pevac_iri(event_iri, p.id)

        if not p.evacuationCenters or p.evacuationCenters == 0:
            continue

        g.add((uri, RDF.type, SKG.PreemptiveEvacuation))
        g.add((event_iri, SKG.hasPreemptiveEvacuation, uri))
        if source_iri is not None:
            g.add((uri, PROV.wasDerivedFrom, source_iri))

        for f in fields(p):
            if f.name == "id": continue

            value = getattr(p, f.name)
            if value is None: continue

            if f.type == URIRef:
                g.add((uri, getattr(SKG, f.name), URIRef(value)))

            # just the evacCenters left
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))

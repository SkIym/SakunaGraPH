from dataclasses import dataclass, fields
from datetime import date
from rdflib import RDF, RDFS, XSD, BNode, Literal, URIRef, Graph
from regex import M
from .graph import PROV, QUDT, SKG, add_monetary
from .iris import (
    event_iri, prov_iri, assistance_iri, aff_pop_iri, casualties_iri,
    damage_gen_iri, org_iri, recovery_iri
)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class Event:
    id: str
    eventClass: str | None
    eventName: str | None
    hasType: str | None
    hasSubtype: str | None
    hasLocation: URIRef | None
    startDate: date | None
    endDate: date | None
    reference: str | None
    remarks: str | None
    otherDescription: str | None


@dataclass
class Incident:
    id: str
    sub_id: str
    hasLocation: URIRef | None
    hasType: str | None


@dataclass
class Preparedness:
    id: str
    agencyLGUsPresentPreparedness: str | None
    announcementsReleased: str | None


@dataclass
class Evacuation:
    id: str
    evacuationPlan: str | None
    evacuationCenters: int | None


@dataclass
class Rescue:
    id: str
    rescueEquipment: str | None
    rescueUnit: str | None


@dataclass
class DeclarationOfCalamity:
    id: str
    declarationOfCalamity: str | None


@dataclass
class AffectedPopulation:
    id: str
    affectedBarangays: int | None
    affectedFamilies: int | None
    affectedPersons: int | None
    displacedFamilies: int | None
    displacedPersons: int | None


@dataclass
class Casualties:
    id: str
    dead: int | None
    injured: int | None
    missing: int | None


@dataclass
class HousingDamage:
    id: str
    totallyDamagedHouses: int | None
    partiallyDamagedHouses: int | None


@dataclass
class InfrastructureDamage:
    id: str
    infraDamageAmount: float | None
    commercialDamageAmount: float | None
    socialDamageAmount: float | None
    crossSectoralDamageAmount: float | None


@dataclass
class DamageGeneral:
    id: str
    generalDamageAmount: float | None


@dataclass
class PowerDisruption:
    id: str
    powerAffected: str | None


@dataclass
class CommunicationLineDisruption:
    id: str
    communicationAffected: str | None


@dataclass
class RoadAndBridgesDamage:
    id: str
    roadBridgeAffected: str | None


@dataclass
class SeaportDisruption:
    id: str
    seaportsAffected: str | None


@dataclass
class WaterDisruption:
    id: str
    areDamsAffected: str | None
    isTapAffected: str | None


@dataclass
class Assistance:
    id: str
    allocatedFunds: float | None         # stored in millions
    agencyLGUsPresentAssistance: str | None
    internationalOrgsPresent: str | None
    amountNGOs: float | None             # stored in millions
    
@dataclass
class Relief:  
    id: str
    itemType: str                        # e.g. "Goods", "Water", "Clothing" …
    itemCost: float | None  = None             # stored in millions
    itemQty: str | None = None


@dataclass
class Recovery:
    id: str
    srrDone: str | None
    policyChanges: str | None
    postTraining: str | None
    postStructureCost: float | None      # stored in millions


def _event_uri(event_id: str) -> URIRef:
    return URIRef(f"https://sakuna.ph/{event_id}")


def _sub_uri(event_id: str, suffix: str) -> URIRef:
    return URIRef(f"https://sakuna.ph/{event_id}/{suffix}")



def _add_location(g: Graph, subject: URIRef, location: URIRef | str | None) -> None:
    if not location:
        return
    for loc in str(location).split("|"):
        loc = loc.strip()
        if loc:
            g.add((subject, SKG.hasLocation, URIRef(loc)))


def _add_type_iri(g: Graph, subject: URIRef, predicate: URIRef, value: str | None) -> None:
    if value:
        for v in str(value).split("|"):
            v = v.strip()
            if v:
                g.add((subject, predicate, URIRef(SKG[v])))


def event_mapping(rs: list[Event], g: Graph, src_uri: URIRef) -> None:
    for r in rs:
        uri = _event_uri(r.id)

        # rdf:type — map eventClass to the appropriate SKG class
        if r.eventClass == "I":
            g.add((uri, RDF.type, SKG.Incident))
        else:
            g.add((uri, RDF.type, SKG.MajorEvent))

        g.add((uri, PROV.wasDerivedFrom, src_uri))

        if r.eventName:
            g.add((uri, SKG.eventName, Literal(r.eventName)))

        _add_type_iri(g, uri, SKG.hasDisasterType, r.hasType)
        _add_type_iri(g, uri, SKG.hasDisasterSubtype, r.hasSubtype)
        _add_location(g, uri, r.hasLocation)

        if r.startDate:
            g.add((uri, SKG.startDate, Literal(r.startDate, datatype=XSD.dateTime)))
        if r.endDate:
            g.add((uri, SKG.endDate, Literal(r.endDate, datatype=XSD.dateTime)))

        if r.reference:
            g.add((uri, SKG.reference, Literal(r.reference)))
        if r.remarks:
            g.add((uri, SKG.remarks, Literal(r.remarks)))
        if r.otherDescription:
            g.add((uri, SKG.remarks, Literal(r.otherDescription)))


# ---------------------------------------------------------------------------
# Incident mapping
# ---------------------------------------------------------------------------

def incident_mapping(rs: list[Incident], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, f"related_incident/{r.sub_id}")

        g.add((uri, RDF.type, SKG.Incident))
        g.add((event_uri, SKG.hasRelatedIncident, uri))

        _add_location(g, uri, r.hasLocation)
        _add_type_iri(g, uri, SKG.hasDisasterType, r.hasType)


# ---------------------------------------------------------------------------
# Preparedness mapping
# ---------------------------------------------------------------------------

def preparedness_mapping(rs: list[Preparedness], g: Graph) -> None:
    from semantic_processing.org_resolver import ORG_RESOLVER

    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "preparedness")

        g.add((uri, RDF.type, SKG.Preparedness))
        g.add((event_uri, SKG.hasPreparedness, uri))

        if r.agencyLGUsPresentPreparedness:
            g.add((uri, SKG.agencyLGUsPresent, Literal(r.agencyLGUsPresentPreparedness)))
            for slug in ORG_RESOLVER.split_and_resolve(r.agencyLGUsPresentPreparedness):
                o_uri = org_iri(slug)
                g.add((o_uri, RDF.type, PROV.Organization))
                g.add((o_uri, RDFS.label, Literal(slug)))
                g.add((uri, SKG.contributingOrg, o_uri))
        if r.announcementsReleased:
            g.add((uri, SKG.announcementsReleased, Literal(r.announcementsReleased)))


# ---------------------------------------------------------------------------
# Evacuation mapping
# ---------------------------------------------------------------------------

def evacuation_mapping(rs: list[Evacuation], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "preemptive_evacuation")

        g.add((uri, RDF.type, SKG.PreemptiveEvacuation))
        g.add((event_uri, SKG.hasPreemptiveEvacuation, uri))

        if r.evacuationPlan:
            g.add((uri, SKG.evacuationPlan, Literal(r.evacuationPlan)))
        if r.evacuationCenters is not None:
            g.add((uri, SKG.evacuationCenters, Literal(int(r.evacuationCenters), datatype=XSD.int)))


# ---------------------------------------------------------------------------
# Rescue mapping
# ---------------------------------------------------------------------------

def rescue_mapping(rs: list[Rescue], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "rescue")

        g.add((uri, RDF.type, SKG.Rescue))
        g.add((event_uri, SKG.hasRescue, uri))

        if r.rescueEquipment:
            g.add((uri, SKG.rescueEquipment, Literal(r.rescueEquipment)))
        if r.rescueUnit:
            g.add((uri, SKG.rescueUnit, Literal(r.rescueUnit)))


# ---------------------------------------------------------------------------
# Declaration of Calamity mapping
# ---------------------------------------------------------------------------

def calamity_mapping(rs: list[DeclarationOfCalamity], g: Graph) -> None:
    for r in rs:
        
        value = r.declarationOfCalamity

        if value:

            if "calamity" in value.lower():
                event_uri = _event_uri(r.id)
                uri = _sub_uri(r.id, "declaration_of_calamity")
                g.add((uri, RDF.type, SKG.DeclarationOfCalamity))
                g.add((event_uri, SKG.hasDeclarationOfCalamity, uri))
                g.add((uri, SKG.declarationType, Literal(r.declarationOfCalamity)))
            else:
                event_uri = _event_uri(r.id)
                uri = _sub_uri(r.id, "preparedness")
                g.add((uri, RDF.type, SKG.Preparedness))
                g.add((event_uri, SKG.hasPreparedness, uri))
                g.add((uri, SKG.announcementsReleased, Literal(r.declarationOfCalamity)))


# ---------------------------------------------------------------------------
# Affected Population mapping
# ---------------------------------------------------------------------------

def aff_pop_mapping(rs: list[AffectedPopulation], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "affected_population")

        g.add((uri, RDF.type, SKG.AffectedPopulation))
        g.add((event_uri, SKG.hasAffectedPopulation, uri))

        if r.affectedBarangays is not None:
            g.add((uri, SKG.affectedBarangays, Literal(r.affectedBarangays, datatype=XSD.int)))
        if r.affectedFamilies is not None:
            g.add((uri, SKG.affectedFamilies, Literal(r.affectedFamilies, datatype=XSD.int)))
        if r.affectedPersons is not None:
            g.add((uri, SKG.affectedPersons, Literal(r.affectedPersons, datatype=XSD.int)))
        if r.displacedFamilies is not None:
            g.add((uri, SKG.displacedFamilies, Literal(int(r.displacedFamilies), datatype=XSD.int)))
        if r.displacedPersons is not None:
            g.add((uri, SKG.displacedPersons, Literal(int(r.displacedPersons), datatype=XSD.int)))


# ---------------------------------------------------------------------------
# Casualties mapping
# ---------------------------------------------------------------------------

def casualties_mapping(rs: list[Casualties], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        
        index = 0
        if r.dead is not None:
            uri = casualties_iri(event_uri, str(index+1))
            g.add((uri, RDF.type, SKG.Casualties))
            g.add((event_uri, SKG.hasCasualties, uri))
            g.add((uri, SKG.casualtyCount, Literal(r.dead, datatype=XSD.int)))
            g.add((uri, SKG.casualtyType, Literal("DEAD", datatype=SKG.casualtyDatatype)))
            index+=1

        if r.injured is not None:
            uri = casualties_iri(event_uri, str(index+1))
            g.add((uri, RDF.type, SKG.Casualties))
            g.add((event_uri, SKG.hasCasualties, uri))
            g.add((uri, SKG.casualtyCount, Literal(r.injured, datatype=XSD.int)))
            g.add((uri, SKG.casualtyType, Literal("INJURED", datatype=SKG.casualtyDatatype)))
            index+=1

        if r.missing is not None:
            uri = casualties_iri(event_uri, str(index+1))
            g.add((uri, RDF.type, SKG.Casualties))
            g.add((event_uri, SKG.hasCasualties, uri))
            g.add((uri, SKG.casualtyCount, Literal(r.missing, datatype=XSD.int)))
            g.add((uri, SKG.casualtyType, Literal("MISSING", datatype=SKG.casualtyDatatype)))
            index+=1


# ---------------------------------------------------------------------------
# Housing Damage mapping
# ---------------------------------------------------------------------------

def housing_damage_mapping(rs: list[HousingDamage], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "housing_damage")

        g.add((uri, RDF.type, SKG.HousingDamage))
        g.add((event_uri, SKG.hasHousingDamage, uri))

        if r.totallyDamagedHouses is not None:
            g.add((uri, SKG.totallyDamagedHouses, Literal(r.totallyDamagedHouses, datatype=XSD.int)))
        if r.partiallyDamagedHouses is not None:
            g.add((uri, SKG.partiallyDamagedHouses, Literal(int(r.partiallyDamagedHouses), datatype=XSD.int)))


# ---------------------------------------------------------------------------
# Infrastructure Damage mapping
# ---------------------------------------------------------------------------

def infra_damage_mapping(rs: list[InfrastructureDamage], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "infrastructure_damage")

        g.add((uri, RDF.type, SKG.InfrastructureDamage))
        g.add((event_uri, SKG.hasInfrastructureDamage, uri))

        if r.infraDamageAmount is not None:
            # g.add((uri, SKG.infraDamageAmount, Literal(r.infraDamageAmount, datatype=XSD.decimal)))

            add_monetary(g, uri, SKG.infraDamageAmount, _to_millions(r.infraDamageAmount), SKG.PHP_millions)
            
        if r.commercialDamageAmount is not None:
            # g.add((uri, SKG.commercialDamageAmount, Literal(r.commercialDamageAmount, datatype=XSD.decimal)))

            add_monetary(g, uri, SKG.commercialDamageAmount, _to_millions(r.commercialDamageAmount), SKG.PHP_millions)


        if r.socialDamageAmount is not None:
            # g.add((uri, SKG.socialDamageAmount, Literal(r.socialDamageAmount, datatype=XSD.decimal)))

            add_monetary(g, uri, SKG.socialDamageAmount, _to_millions(r.socialDamageAmount), SKG.PHP_millions)

        if r.crossSectoralDamageAmount is not None:
            # g.add((uri, SKG.crossSectoralDamageAmount, Literal(r.crossSectoralDamageAmount, datatype=XSD.decimal)))

            add_monetary(g, uri, SKG.crossSectoralDamageAmount, _to_millions(r.crossSectoralDamageAmount), SKG.PHP_millions)


# ---------------------------------------------------------------------------
# General Damage mapping
# ---------------------------------------------------------------------------

def damage_gen_mapping(rs: list[DamageGeneral], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "damage_general")

        g.add((uri, RDF.type, SKG.DamageGeneral))
        g.add((event_uri, SKG.hasDamageGeneral, uri))

        if r.generalDamageAmount is not None:
            # g.add((uri, SKG.generalDamageAmount, Literal(r.generalDamageAmount, datatype=XSD.decimal)))

            add_monetary(g, uri, SKG.generalDamageAmount, _to_millions(r.generalDamageAmount), SKG.PHP_millions)


# ---------------------------------------------------------------------------
# Power Disruption mapping
# ---------------------------------------------------------------------------

def power_disruption_mapping(rs: list[PowerDisruption], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "power_disruption")

        g.add((uri, RDF.type, SKG.PowerDisruption))
        g.add((event_uri, SKG.hasPowerDisruption, uri))

        if r.powerAffected:
            g.add((uri, SKG.remarks, Literal(r.powerAffected)))


# ---------------------------------------------------------------------------
# Communication Line Disruption mapping
# ---------------------------------------------------------------------------

def comms_disruption_mapping(rs: list[CommunicationLineDisruption], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "communication_line_disruption")

        g.add((uri, RDF.type, SKG.CommunicationLineDisruption))
        g.add((event_uri, SKG.hasCommunicationLineDisruption, uri))

        if r.communicationAffected:
            g.add((uri, SKG.remarks, Literal(r.communicationAffected)))


# ---------------------------------------------------------------------------
# Road and Bridges Damage mapping
# ---------------------------------------------------------------------------

def rnb_damage_mapping(rs: list[RoadAndBridgesDamage], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "road_and_bridges_damage")

        g.add((uri, RDF.type, SKG.RoadAndBridgesDamage))
        g.add((event_uri, SKG.hasRoadAndBridgesDamage, uri))

        if r.roadBridgeAffected:
            g.add((uri, SKG.remarks, Literal(r.roadBridgeAffected)))


# ---------------------------------------------------------------------------
# Seaport Disruption mapping
# ---------------------------------------------------------------------------

def seaport_disruption_mapping(rs: list[SeaportDisruption], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "seaport_disruption")

        g.add((uri, RDF.type, SKG.SeaportDisruption))
        g.add((event_uri, SKG.hasSeaportDisruption, uri))

        if r.seaportsAffected:
            g.add((uri, SKG.remarks, Literal(r.seaportsAffected)))


# ---------------------------------------------------------------------------
# Water Disruption mapping
# ---------------------------------------------------------------------------

def _augment_water_source(source_type: str, affected: str | None) -> str | None:
    
    affected = str(affected)
    if affected and affected.strip().lower() not in ("", "no", "false", "0"):
        return f"{source_type} affected"
    return None


def water_disruption_mapping(rs: list[WaterDisruption], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "water_disruption")

        g.add((uri, RDF.type, SKG.WaterDisruption))
        g.add((event_uri, SKG.hasWaterDisruption, uri))

        dam_desc = _augment_water_source("Dam", r.areDamsAffected)
        if dam_desc:
            g.add((uri, SKG.remarks, Literal(dam_desc)))

        tap_desc = _augment_water_source("Tap", r.isTapAffected)
        if tap_desc:
            g.add((uri, SKG.remarks, Literal(tap_desc)))

def _to_millions(val: float):

    fl_val = float(val)
    if fl_val == 0: return fl_val

    conv = val
    if fl_val > 1000: 
        conv = fl_val / 1000000

    return conv

# ---------------------------------------------------------------------------
# Assistance mapping
# ---------------------------------------------------------------------------

def assistance_mapping(rs: list[Assistance], g: Graph) -> None:
    from semantic_processing.org_resolver import ORG_RESOLVER

    for r in rs:
        event_uri = _event_uri(r.id)

        if r.allocatedFunds is not None:

            uri = _sub_uri(r.id, "assistance/allocated")

            g.add((uri, RDF.type, SKG.Assistance))
            g.add((event_uri, SKG.hasAssistance, uri))

            add_monetary(g, uri, SKG.contributionAmount, _to_millions(r.allocatedFunds), SKG.PHP_millions)

        muri = _sub_uri(r.id, "assistance")

        g.add((muri, RDF.type, SKG.Assistance))
        g.add((event_uri, SKG.hasAssistance, muri))

        if r.agencyLGUsPresentAssistance:
            g.add((muri, SKG.agencyLGUsPresent, Literal(r.agencyLGUsPresentAssistance)))
            for slug in ORG_RESOLVER.split_and_resolve(r.agencyLGUsPresentAssistance):
                o_uri = org_iri(slug)
                g.add((o_uri, RDF.type, PROV.Organization))
                g.add((o_uri, RDFS.label, Literal(slug)))
                g.add((muri, SKG.contributingOrg, o_uri))

        if r.internationalOrgsPresent:
            g.add((muri, SKG.internationalOrgsPresent, Literal(r.internationalOrgsPresent)))
            for slug in ORG_RESOLVER.split_and_resolve(r.internationalOrgsPresent):
                o_uri = org_iri(slug)
                g.add((o_uri, RDF.type, PROV.Organization))
                g.add((o_uri, RDFS.label, Literal(slug)))
                g.add((muri, SKG.contributingOrg, o_uri))

        if r.amountNGOs is not None:
            uri = _sub_uri(r.id, "assistance/ngo+international")

            g.add((uri, RDF.type, SKG.Assistance))
            g.add((event_uri, SKG.hasAssistance, uri))

            add_monetary(g, uri, SKG.contributionAmount, _to_millions(r.amountNGOs), SKG.PHP_millions)


# ---------------------------------------------------------------------------
# Assistance mapping  (goods / water / clothing / medicine / unspecified / general)
# ---------------------------------------------------------------------------

_RELIEF_ITEM_TYPE_LABELS: dict[str, str] = {
    "goods":       "Canned Goods, Rice, etc.",
    "water":       "Water",
    "clothing":    "Clothing",
    "medicine":    "Medicine",
    "unspecified": "Unspecified",
    "general":     "Unspecified",
}


def relief_mapping(rs: list[Relief], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        # slug matches the RML template suffix, e.g. "assistance/goods"
        uri = _sub_uri(r.id, f"assistance/{r.itemType.lower()}")

        g.add((uri, RDF.type, SKG.Assistance))
        g.add((event_uri, SKG.hasAssistance, uri))

        label = _RELIEF_ITEM_TYPE_LABELS.get(r.itemType.lower(), r.itemType)
        g.add((uri, SKG.itemTypeOrNeeds, Literal(label)))

        if r.itemCost is not None:

            add_monetary(g, uri, SKG.itemCost, _to_millions(r.itemCost), SKG.PHP_millions)

            # g.add((uri, SKG.itemCost, Literal(r.itemCost, datatype=XSD.decimal)))
        if r.itemQty:
            g.add((uri, SKG.itemQty, Literal(r.itemQty)))


# ---------------------------------------------------------------------------
# Recovery mapping
# ---------------------------------------------------------------------------

def recovery_mapping(rs: list[Recovery], g: Graph) -> None:
    for r in rs:
        event_uri = _event_uri(r.id)
        uri = _sub_uri(r.id, "recovery")

        g.add((uri, RDF.type, SKG.Recovery))
        g.add((event_uri, SKG.hasRecovery, uri))

        if r.srrDone:
            g.add((uri, SKG.srrDone, Literal(r.srrDone)))
        if r.policyChanges:
            g.add((uri, SKG.policyChanges, Literal(r.policyChanges)))
        if r.postTraining:
            g.add((uri, SKG.postTraining, Literal(r.postTraining)))
        if r.postStructureCost is not None:
            add_monetary(g, uri, SKG.postStructureCost, _to_millions(r.postStructureCost), SKG.PHP_millions)

            # g.add((uri, SKG.postStructureCost, Literal(r.postStructureCost, datatype=XSD.decimal)))

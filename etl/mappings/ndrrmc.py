# NDRRMC MAPPINGS HERE (rdflib)
from dataclasses import fields
from typing import List, Literal as TypingLiteral
from rdflib import URIRef, Literal
from rdflib.namespace import RDF, XSD
from datetime import datetime
from dataclasses import dataclass
from .graph import SKG, Graph, PROV
from .iris import aff_pop_iri, agri_iri, casualties_iri, event_iri, housing_iri, incident_iri, infra_iri, pevac_iri, power_iri, prov_iri, relief_iri, rnb_iri

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
    "NO_OF_AFFECTED_Brgys": "affectedBarangays",
    "NO_OF_AFFECTED_Families": "affectedFamilies",
    "NO_OF_AFFECTED_Persons": "affectedPersons",
    "TOTAL_SERVED_CURRENT_Inside_+_Outside_Persons_CUM": "displacedPersons",
    "TOTAL_SERVED_CURRENT_Inside_+_Outside_Families_CUM": "displacedFamilies",
    "TOTAL_SERVED_Inside_+_Outside_Persons_CUM": "displacedPersons",
    "TOTAL_SERVED_Inside_+_Outside_Families_CUM": "displacedFamilies",
    "No_of_ECs_Persons_CUM": "evacuationCenters",
    "Barangay": "hasBarangay"
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
            SKG.hasAffectedPopulation,
            uri
        ))

        if int(ap.affectedBarangays) > 1:

            g.add((
                uri,
                SKG.affectedBarangays,
                Literal(ap.affectedBarangays, datatype=XSD.int)
            ))
        
        elif ap.hasBarangay:

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

CasualtyType = TypingLiteral["DEAD", "INJURED", "MISSING"]
       
@dataclass
class Casualties:
    id: str
    casualtyType: CasualtyType
    casualtyCount: int
    hasLocation: URIRef
    hasBarangay: str | None
    casualtyDataSource: str | None
    casualtyCause: str | None
    remarks: str | None

def casualties_mapping(g: Graph, cas: List[Casualties], event_iri: URIRef):

    for c in cas:
        uri = casualties_iri(event_iri, c.id)

        # rdf:type
        g.add((
            uri,
            RDF.type,
            SKG.Casualties
        ))

        # Link impact to parent event
        g.add((
            event_iri,
            SKG.hasCasualties,
            uri
        ))

        g.add((
            uri,
            SKG.casualtyType,
            Literal(c.casualtyType, datatype=SKG.casualtyType)
        ))

        g.add((
            uri,
            SKG.casualtyCount,
            Literal(c.casualtyCount, datatype=XSD.int)
        ))

        g.add((
                uri,
                SKG.hasLocation,
                URIRef(c.hasLocation)
        ))

        if c.casualtyCause:
            g.add((
                uri,
                SKG.casualtyCause,
                Literal(c.casualtyCause)
            ))
        
        if c.remarks:
            g.add((
                uri,
                SKG.remarks,
                Literal(c.remarks)
            ))
        
        if c.casualtyDataSource:
            g.add((
                uri,
                SKG.casualtyDataSource,
                Literal(c.casualtyDataSource)
            ))
        
        if c.hasBarangay:
            g.add((
                uri,
                SKG.hasBarangay,
                Literal(c.hasBarangay)
            ))

# assistance provided

ASSISTANCE_PROVIDED_MAPPING = {
    "NEEDS": "itemTypeOrNeeds",
    "NFIs_Services_Provided_TYPE": "itemTypeOrNeeds",
    "TYPE": "itemTypeOrNeeds",
    "QUANTITY": "itemQuantity",
    "F_NFIs_PROVIDED_QTY": "itemQuantity",
    "NFIs_Services_Provided_QTY": "itemQuantity",
    "NFIs_Services_Provided_UNIT": "itemUnit",
    "F_NFIs_PROVIDED_UNIT": "itemUnit",
    "UNIT": "itemUnit",
    "F_NFIs_PROVIDED_AMOUNT": "itemCost",
    "NFIs_Services_Provided_AMOUNT": "itemCost",
    "COSTPHP": "itemCost",
    "F_NFIs_PROVIDED_COST_PER\nUNIT": "itemCostPerUnit",
    "F_NFIs_PROVIDED_COST_PER\rUNIT": "itemCostPerUnit",
    "F_NFIs_PROVIDED_COST_PER\r\nUNIT": "itemCostPerUnit",
    "NFIs_Services_Provided_COST_PER_UNIT": "itemCostPerUnit",
    "COST PER UNIT": "itemCostPerUnit",
    "F_NFIs_PROVIDED_SOURCE": "itemSource",
    "SOURCE_AMOUNT": "itemSource",
    "SOURCE": "itemSource",
    "REMARKS": "remarks",
    "REMARKS_SOURCE": "remarks",
    "REMARKS_AMOUNT": "remarks",
    "Barangay": "hasBarangay"

}

@dataclass
class Relief:
    id: str
    hasLocation: URIRef
    hasBarangay: str | None
    itemSource: str | None
    itemQuantity: float | None
    itemUnit: str | None
    itemTypeOrNeeds: str | None
    itemCost: float
    remarks: str | None
    itemCostPerUnit: float | None

def relief_mapping(g: Graph, reliefs: List[Relief], event_iri: URIRef):

    for r in reliefs:
        uri = relief_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.Relief)) # rdf type
        g.add((event_iri, SKG.hasRelief, uri)) # event link

        for f in fields(r):

            if f.name == "id": continue

            value = getattr(r, f.name)
            if value is None:
                continue  
            
            if (type(value) == int or type(value) == float) and value == 0:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "itemCost":
                g.add((uri, SKG.itemCost, Literal(value, datatype=XSD.decimal)))
            elif f.name == "itemQuantity":
                g.add((uri, SKG.itemQuantity, Literal(value, datatype=XSD.decimal)))
            elif f.name == "itemCostPerUnit" and value > 0:
                g.add((uri, SKG.itemCostPerUnit, Literal(value, datatype=XSD.decimal)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


INFRA_MAPPING = {
    "TYPE": "infraDamageType",
    "CLASSIFICATION": "infraDamageClassification",
    "INFRASTRUCTURE": "infraName",
    "INFRASTRUCTU\nRE": "infraName",
    "INFRASTRUCTU\r\nRE": "infraName",
    "NUMBER_OF\nDAMAGED": "numberInfraDamaged",
    "NUMBER_OF\r\nDAMAGED": "numberInfraDamaged",
    "COSTPHP": "infraDamageAmount",
    "REMARKS": "remarks",
    "Barangay": "hasBarangay"

}

@dataclass
class Infrastructure:
    id: str
    hasBarangay: str | None
    hasLocation: URIRef
    infraDamageType: str | None
    infraDamageClassification: str | None
    infraName: str | None
    numberInfraDamaged: int
    infraDamageAmount: float 
    remarks: str | None

def infra_mapping(g: Graph, infra: List[Infrastructure], event_iri: URIRef):

    for r in infra:
        uri = infra_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.InfrastructureDamage)) # rdf type
        g.add((event_iri, SKG.hasInfrastructureDamage, uri)) # event link

        for f in fields(r):

            if f.name == "id": continue

            value = getattr(r, f.name)
            if value is None:
                continue  
            
            if (type(value) == int or type(value) == float) and value == 0:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "infraDamageAmount":
                g.add((uri, SKG.infraDamageAmount, Literal(value, datatype=XSD.decimal)))
            elif f.name == "numberInfraDamaged" and value < 2:
                continue
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))

HOUSES_MAPPING = {
    "NO_OF_DAMAGED_HOUSES_TOTALLY": "totallyDamagedHouses",
    "NO_OF_DAMAGED_HOUSES_PARTIALLY": "partiallyDamagedHouses",
    "AMOUNT_PHP_GRAND_TOTAL": "housingDamageAmount",
    "REMARKS_GRAND_TOTAL": "remarks",    
    "Barangay": "hasBarangay"

}

@dataclass
class Housing:
    id: str
    hasLocation: URIRef
    hasBarangay: str | None
    totallyDamagedHouses: int
    partiallyDamagedHouses: int
    housingDamageAmount: float | None
    remarks: str | None

def housing_mapping(g: Graph, hs: List[Housing], event_iri: URIRef):

    for r in hs:
        uri = housing_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.HousingDamage)) # rdf type
        g.add((event_iri, SKG.hasHousingDamage, uri)) # event link

        for f in fields(r):

            if f.name == "id": continue

            value = getattr(r, f.name)
            if value is None:
                continue  
            
            # if (type(value) == int or type(value) == float) and value == 0:
            #     continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "housingDamageAmount":
                g.add((uri, SKG.housingDamageAmount, Literal(value, datatype=XSD.decimal)))
            elif f.name == "totallyDamagedHouses":
                g.add((uri, SKG.totallyDamagedHouses, Literal(value, datatype=XSD.int)))
            elif f.name == "partiallyDamagedHouses":                
                g.add((uri, SKG.partiallyDamagedHouses, Literal(value, datatype=XSD.int)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value))) 


AGRI_MAPPING = {
    "CLASSIFICATION": "agriDamageClassification",
    "CLASSIFICATI_ON": "agriDamageClassification",
    "TYPE": "agriDamageType",
    "UNIT": "agriDamageUnit",
    "QUANTITY": "agriDamageQuantity",
    "TOTAL_COSTPHP": "agriDamageAmount",
    "REMARKS": "remarks",
    "NO_OF_FARMERS__FISHERFOLK_AFFECTED": "farmerFisherfolkAffected",
    "AFFECTED_CROP_AREA_HA_WITH_NO_CHANCE_OF_RECOVERY_TOTALLY_DAMAGED": "totallyDamagedCropArea",
    "AFFECTED_CROP_AREA_HA_WITH_CHANCE_OF_RECOVERY_PARTIALLY_DAMAGED": "partiallyDamagedCropArea",
    "NUMBER_OF_DAMAGED_INFRASTRUCTURE,_MACHINERIES,_EQUIPMENT_TOTALLY_DAMAGED": "totallyDamagedInfrastructure",
    "NUMBER_OF_DAMAGED_INFRASTRUCTURE,_MACHINERIES,_EQUIPMENT_PARTIALLY_DAMAGED": "partiallyDamagedInfrastructure",
    "PRODUCTION_LOSS_IN_VOLUME_MT_TOTAL": "productionLossVolume",
    "PRODUCTION_LOSS_COST_OF_DAMAGE_IN_VALUE_PHP_TOTAL": "productionLossCost",
    "Barangay": "hasBarangay"

}

@dataclass
class Agriculture:
    id: str
    hasLocation: URIRef
    hasBarangay: str | None
    agriDamageAmount: float
    agriDamageClassification: str | None
    agriDamageType: str | None
    agriDamageQuantity: float | None
    agriDamageUnit: str | None
    farmerFisherfolkAffected: int | None
    partiallyDamagedCropArea: float | None
    totallyDamagedCropArea: float | None
    partiallyDamagedInfrastructure: int | None
    totallyDamagedInfrastructure: int | None
    productionLossCost: float | None
    productionLossVolume: float | None
    remarks: str | None

def agri_mapping(g: Graph, hs: List[Agriculture], event_iri: URIRef):

    for r in hs:
        uri = agri_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.AgricultureDamage)) # rdf type
        g.add((event_iri, SKG.hasAgricultureDamage, uri)) # event link

        for f in fields(r):

            if f.name == "id": continue

            value = getattr(r, f.name)
            if value is None:
                continue  
            
            # if (type(value) == int or type(value) == float) and value == 0:
            #     continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "agriDamageAmount":
                g.add((uri, SKG.agriDamageAmount, Literal(value, datatype=XSD.decimal)))
            elif f.name == "agriDamageQuantity":
                g.add((uri, SKG.agriDamageQuantity, Literal(value, datatype=XSD.decimal)))
            elif f.name == "totallyDamagedCropArea":
                g.add((uri, SKG.totallyDamagedCropArea, Literal(value, datatype=XSD.decimal)))
            elif f.name == "partiallyDamagedCropArea":                
                g.add((uri, SKG.partiallyDamagedCropArea, Literal(value, datatype=XSD.decimal)))
            elif f.name == "productionLossCost":
                g.add((uri, SKG.productionLossCost, Literal(value, datatype=XSD.decimal)))
            elif f.name == "productionLossVolume":                
                g.add((uri, SKG.productionLossVolume, Literal(value, datatype=XSD.decimal)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value))) 

PEVAC_MAPPING = {
    "FAMILIES": "preemptFamilies",
    "TOTAL": "preemptPersons",
    "REMARKS": "remarks",
    "Barangay": "hasBarangay"
}

@dataclass
class PEvacuation: #Preemptive Evacuation
    id: str
    hasLocation: URIRef
    hasBarangay: str | None
    preemptFamilies: int | None
    preemptPersons: int | None
    remarks: str | None
    evacuationCenters: int | None

def pevac_mapping(g: Graph, hs: List[PEvacuation], event_iri: URIRef):

    for r in hs:
        uri = pevac_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.PreemptiveEvacuation)) # rdf type
        g.add((event_iri, SKG.hasPreemptiveEvacuation, uri)) # event link

        for f in fields(r):

            if f.name == "id": continue

            value = getattr(r, f.name)
            if value is None:
                continue  
            
            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value))) 
    

RNB_MAPPING = {
    "TYPE": "roadBridgeType",
    "CLASSIFICATION": "roadBridgeClassification",
    "ROAD_SECTION_BRIDG_E": "roadBridgeName",
    "DATE_REPORTED_passable": "passableDate",
    "TIME_REPORTED_passable": "passableTime",
    "DATE_REPORTED_not_passable": "notPassableDate",
    "TIME_REPORTED_not_passable": "notPassableTime",
    "STATUS": "roadBridgeStatus",
    "REMARK": "remarks",
    "Barangay": "hasBarangay"
}

@dataclass
class RNB:
    id: str
    hasLocation: URIRef
    hasBarangay: str | None
    roadBridgeType: str | None
    roadBridgeClassification: str | None
    roadBridgeName: str | None
    passableDateTime: datetime | None
    notPassableDateTime: datetime | None
    roadBridgeStatus: str | None
    remarks: str | None


def rnb_mapping(g: Graph, hs: List[RNB], event_iri: URIRef):

    for r in hs:
        uri = rnb_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.RoadAndBridgesDamage)) # rdf type
        g.add((event_iri, SKG.hasRoadAndBridgesDamage, uri)) # event link

        for f in fields(r):

            if f.name == "id": continue

            value = getattr(r, f.name)
            if value is None:
                continue  
            
            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "passableDateTime":
                g.add((uri, SKG.passableDateTime, Literal(value, datatype=XSD.dateTime)))
            elif f.name == "notPassableDateTime":
                g.add((uri, SKG.notPassableDateTime, Literal(value, datatype=XSD.dateTime)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value))) 

POWER_MAPPING = {
    "TYPE": "disruptionType",
    "SERVICE_PROVIDER": "serviceProvider",
    "DATE_OF_INTERRUPTION__OUTAGE": "interruptionDate",
    "TIME_OF_INTERRUPTION__OUTAGE": "interruptionTime",
    "DATE_RESTORED": "restorationDate",
    "TIME_RESTORED": "restorationTime",
    "REMARKS": "remarks",
    "Barangay": "hasBarangay"
}

@dataclass
class Power:
    id: str
    hasLocation: URIRef
    hasBarangay: str | None
    disruptionType: str 
    serviceProvider: str | None
    interruptionDateTime: datetime | None
    restorationDateTime: datetime | None
    remarks: str | None

def power_mapping(g: Graph, hs: List[Power], event_iri: URIRef):

    for r in hs:
        uri = power_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.PowerDisruption)) # rdf type
        g.add((event_iri, SKG.hasPowerDisruption, uri)) # event link

        for f in fields(r):

            if f.name == "id": continue

            value = getattr(r, f.name)
            if value is None:
                continue  
            
            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "interruptionDateTime":
                g.add((uri, SKG.interruptionDateTime, Literal(value, datatype=XSD.dateTime)))
            elif f.name == "restorationDateTime":
                g.add((uri, SKG.restorationDateTime, Literal(value, datatype=XSD.dateTime)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value))) 


COMMS_MAPPING = {
    "TELECOM_PANY": "telecompany",
    "TELECOMPANY": "telecompany",
    "STATUS_OF_COMM_UNICATIO_N": "communicationStatus",
    "STATUS_OF_COMMUNICATION": "communicationStatus",
    "DATE_INT_ERRUPTI_ON": "interruptionDate",
    "DATE_INTERRUPTION": "interruptionDate",
    "TIME_INTE_RRUPTIO_N": "interruptionTime",
    "TIME_INTERRUPTION": "interruptionTime",
    "DATE_RES_TORATIO_N": "restorationDate",
    "DATE_RESTORATION": "restorationDate",
    "TIME_RES_TORATIO_N": "restorationTime",
    "TIME_RESTORATION": "restorationTime",
    "REMARKS": "remarks",
    "Barangay": "hasBarangay"
}

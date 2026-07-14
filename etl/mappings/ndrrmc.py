# NDRRMC MAPPINGS HERE (rdflib)
from dataclasses import dataclass, field, fields
from typing import List, Literal as TypingLiteral
from rdflib import URIRef, Literal, RDFS
from rdflib.namespace import RDF, XSD
from datetime import datetime

from semantic_processing.org_resolver import ORG_RESOLVER
from .graph import BAW, CUR, SKG, Graph, PROV, add_monetary, ORG
from .iris import (aff_pop_iri, agri_iri, airport_iri, assistance_iri, casualties_iri,
                   class_dis_iri, climate_param_iri, comms_iri, doc_iri, event_uri, flight_iri, housing_iri,
                   incident_iri, infra_iri, org_iri, pevac_iri, power_iri, prov_iri,
                   relief_iri, rnb_iri, seaport_iri, stranded_iri, warning_iri, water_iri, work_dis_iri)


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class LocationRecord:
    """Common fields for all location-scoped impact and disruption records."""
    id: str
    hasLocation: URIRef
    hasBarangay: str | None


@dataclass
class ClimateParameterMeasurement:
    id: str
    parameter: str | None
    value: float | None
    unit: str | None
    location: str | None = None
    hasLocation: URIRef | None = None
    parameterText: str | None = None


@dataclass
class Warning:
    id: str
    warningReleased: str
    warningTimeStamp: datetime | None = None


@dataclass
class Event:
    eventName: str
    hasDisasterType: str
    startDate: datetime | None
    endDate: datetime | None
    id: str
    remarks: str | None = None
    climateParameters: list[ClimateParameterMeasurement] = field(default_factory=list)
    warnings: list[Warning] = field(default_factory=list)


@dataclass
class Provenance:
    lastUpdateDate: datetime
    reportName: str
    obtainedDate: str | None = None
    reportLink: str | None = None


@dataclass
class Incident(LocationRecord):
    incidentActionsTaken: str | None
    incidentDescription: str | None
    startDate: datetime
    endDate: datetime
    hasDisasterType: str
    remarks: str | None


@dataclass
class AffectedPopulation(LocationRecord):
    affectedBarangays: int
    affectedFamilies: int
    affectedPersons: int
    displacedFamilies: int
    displacedPersons: int


CasualtyDatatype = TypingLiteral["DEAD", "INJURED", "MISSING"]

def casualty_type_to_iri(ctyp: str) -> URIRef:
    if "dead" in ctyp.lower(): return SKG.Dead
    if "injured" in ctyp.lower(): return SKG.Injured
    if "missing" in ctyp.lower(): return SKG.Missing

@dataclass
class Casualties(LocationRecord):
    casualtyType: str
    casualtyCount: int
    casualtyDataSource: str | None
    casualtyCause: str | None
    remarks: str | None


@dataclass
class Assistance(LocationRecord):
    itemSource: str | None
    itemQuantity: float | None
    itemUnit: str | None
    itemTypeOrNeeds: str | None
    itemCost: float
    remarks: str | None
    itemCostPerUnit: float | None


@dataclass
class Infrastructure(LocationRecord):
    infraDamageType: str | None
    infraDamageClassification: str | None
    infraName: str | None
    numberInfraDamaged: int
    infraDamageAmount: float
    remarks: str | None


@dataclass
class Housing(LocationRecord):
    totallyDamagedHouses: int
    partiallyDamagedHouses: int
    housingDamageAmount: float | None
    remarks: str | None


@dataclass
class Agriculture(LocationRecord):
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


@dataclass
class PEvacuation(LocationRecord):
    preemptFamilies: int | None
    preemptPersons: int | None
    remarks: str | None
    evacuationCenters: int | None


@dataclass
class RNB(LocationRecord):
    roadBridgeType: str | None
    roadBridgeClassification: str | None
    roadBridgeName: str | None
    passableDateTime: datetime | None
    notPassableDateTime: datetime | None
    roadBridgeStatus: str | None
    remarks: str | None


@dataclass
class Power(LocationRecord):
    disruptionType: str
    serviceProvider: str | None
    interruptionDateTime: datetime | None
    restorationDateTime: datetime | None
    remarks: str | None


@dataclass
class CommunicationLines(LocationRecord):
    telecompany: str | None
    communicationStatus: str | None
    interruptionDateTime: datetime | None
    restorationDateTime: datetime | None
    remarks: str | None


@dataclass
class DOC(LocationRecord):
    declarationType: str | None
    resolutionNo: str
    resolutionDate: datetime
    remarks: str | None


@dataclass
class ClassDisruption(LocationRecord):
    fromClassLevel: str
    toClassLevel: str
    suspensionType: str | None
    cancellationDateTime: datetime
    resumptionDateTime: datetime
    remarks: str | None


@dataclass
class WorkDisruption(LocationRecord):
    suspensionType: str | None
    cancellationDateTime: datetime
    resumptionDateTime: datetime
    remarks: str | None


@dataclass
class Stranded(LocationRecord):
    district: str | None
    station: str | None
    substation: str | None
    portOrTerminalName: str | None
    strandedPassengers: int
    strandedRollingCargoes: int
    strandedVessels: int
    strandedMotorBancas: int
    remarks: str | None


@dataclass
class WaterDisruption(LocationRecord):
    disruptionType: str | None
    serviceProvider: str | None
    interruptionDateTime: datetime
    restorationDateTime: datetime | None
    remarks: str | None


@dataclass
class Seaport(LocationRecord):
    portOrTerminalName: str | None
    portStatus: str | None
    cancellationDateTime: datetime
    resumptionDateTime: datetime | None
    remarks: str | None


@dataclass
class Airport(LocationRecord):
    portOrTerminalName: str | None
    portStatus: str | None
    strandedPassengers: int | None
    cancellationDateTime: datetime
    resumptionDateTime: datetime | None
    remarks: str | None


@dataclass
class Flight(LocationRecord):
    portOrTerminalName: str | None
    airline: str | None
    airportType: str | None
    flightNo: str | None
    flightRoute: str | None
    cancellationDateTime: datetime
    resumptionDateTime: datetime | None
    remarks: str | None


# ── Column mappings ───────────────────────────────────────────────────────────

INCIDENT_COLUMN_MAPPINGS = {
    "type_of_incident":             "hasOrigType",
    "status_for_flooded_areas":     "incidentStatus",
    "actions_taken":                "incidentActionsTaken",
    "description":                  "incidentDescription",
    "column_2":                     "hasDisasterType",
    "column_3":                     "startDate",
    "column_4":                     "startTime",
    "column_5":                     "incidentDescription",
    "column_6":                     "incidentActionsTaken",
    "column_7":                     "remarks",
    "column_8":                     "incidentStatus",
    "remarks":                      "remarks",
    "date_of_occurence":            "startDate",
    "time_of_occurence":            "startTime",
}

AFF_POP_COL_MAP = {
    "no_of_affected_brgys":                                     "affectedBarangays",
    "no_of_affected_families":                                  "affectedFamilies",
    "no_of_affected_persons":                                   "affectedPersons",
    "total_served_current_inside_+_outside_persons_cum":        "displacedPersons",
    "total_served_current_inside_+_outside_families_cum":       "displacedFamilies",
    "total_served_inside_+_outside_persons_cum":                "displacedPersons",
    "total_served_inside_+_outside_families_cum":               "displacedFamilies",
    "no_of_ecs_persons_cum":                                    "evacuationCenters",
}

CASUALTY_MAPPING = {
    "cause":            "casualtyCause",
    "remarks":          "remarks",
    "source_of\ndata":  "casualtyDataSource",
}

ASSISTANCE_PROVIDED_MAPPING = {
    "needs":                                    "itemTypeOrNeeds",
    "nfis_services_provided_type":              "itemTypeOrNeeds",
    "type":                                     "itemTypeOrNeeds",
    "quantity":                                 "itemQuantity",
    "f_nfis_provided_qty":                      "itemQuantity",
    "nfis_services_provided_qty":               "itemQuantity",
    "nfis_services_provided_unit":              "itemUnit",
    "f_nfis_provided_unit":                     "itemUnit",
    "unit":                                     "itemUnit",
    "f_nfis_provided_amount":                   "itemCost",
    "nfis_services_provided_amount":            "itemCost",
    "costphp":                                  "itemCost",
    "f_nfis_provided_cost_per\nunit":           "itemCostPerUnit",
    "f_nfis_provided_cost_per\runit":           "itemCostPerUnit",
    "f_nfis_provided_cost_per\r\nunit":         "itemCostPerUnit",
    "nfis_services_provided_cost_per_unit":     "itemCostPerUnit",
    "cost per unit":                            "itemCostPerUnit",
    "f_nfis_provided_source":                   "itemSource",
    "source_amount":                            "itemSource",
    "source":                                   "itemSource",
    "remarks":                                  "remarks",
    "remarks_source":                           "remarks",
    "remarks_amount":                           "remarks",
}

INFRA_MAPPING = {
    "type":                     "infraDamageType",
    "classification":           "infraDamageClassification",
    "infrastructure":           "infraName",
    "infrastructu\nre":         "infraName",
    "infrastructu\r\nre":       "infraName",
    "number_of\ndamaged":       "numberInfraDamaged",
    "number_of\r\ndamaged":     "numberInfraDamaged",
    "costphp":                  "infraDamageAmount",
    "remarks":                  "remarks",
}

HOUSES_MAPPING = {
    "no_of_damaged_houses_totally":     "totallyDamagedHouses",
    "no_of_damaged_houses_partially":   "partiallyDamagedHouses",
    "amount_php_grand_total":           "housingDamageAmount",
    "remarks_grand_total":              "remarks",
}

AGRI_MAPPING = {
    "classification":                                                                   "agriDamageClassification",
    "classificati_on":                                                                  "agriDamageClassification",
    "type":                                                                             "agriDamageType",
    "unit":                                                                             "agriDamageUnit",
    "quantity":                                                                         "agriDamageQuantity",
    "total_costphp":                                                                    "agriDamageAmount",
    "remarks":                                                                          "remarks",
    "no_of_farmers__fisherfolk_affected":                                               "farmerFisherfolkAffected",
    "affected_crop_area_ha_with_no_chance_of_recovery_totally_damaged":                 "totallyDamagedCropArea",
    "affected_crop_area_ha_with_chance_of_recovery_partially_damaged":                  "partiallyDamagedCropArea",
    "number_of_damaged_infrastructure,_machineries,_equipment_totally_damaged":         "totallyDamagedInfrastructure",
    "number_of_damaged_infrastructure,_machineries,_equipment_partially_damaged":       "partiallyDamagedInfrastructure",
    "production_loss_in_volume_mt_total":                                               "productionLossVolume",
    "production_loss_cost_of_damage_in_value_php_total":                                "productionLossCost",
}

PEVAC_MAPPING = {
    "families":     "preemptFamilies",
    "total":        "preemptPersons",
    "remarks":      "remarks",
}

RNB_MAPPING = {
    "type":                         "roadBridgeType",
    "classification":               "roadBridgeClassification",
    "road_section_bridg_e":         "roadBridgeName",
    "date_reported_passable":       "passableDate",
    "time_reported_passable":       "passableTime",
    "date_reported_not_passable":   "notPassableDate",
    "time_reported_not_passable":   "notPassableTime",
    "status":                       "roadBridgeStatus",
    "remark":                       "remarks",
}

POWER_MAPPING = {
    "type":                             "disruptionType",
    "service_provider":                 "serviceProvider",
    "date_of_interruption__outage":     "interruptionDate",
    "time_of_interruption__outage":     "interruptionTime",
    "date_restored":                    "restorationDate",
    "time_restored":                    "restorationTime",
    "remarks":                          "remarks",
}

COMMS_MAPPING = {
    "telecom_pany":             "telecompany",
    "telecompany":              "telecompany",
    "status_of_comm_unicatio_n":"communicationStatus",
    "status_of_communication":  "communicationStatus",
    "date_int_errupti_on":      "interruptionDate",
    "date_interruption":        "interruptionDate",
    "time_inte_rruptio_n":      "interruptionTime",
    "time_interruption":        "interruptionTime",
    "date_res_toratio_n":       "restorationDate",
    "date_restoration":         "restorationDate",
    "time_res_toratio_n":       "restorationTime",
    "time_restoration":         "restorationTime",
    "remarks":                  "remarks",
}

DOC_MAPPING = {
    "type":                 "declarationType",
    "resolution_number":    "resolutionNo",
    "resolution_date":      "resolutionDate",
    "remarks":              "remarks",
}

CLASS_MAPPING = {
    "level_from":           "fromClassLevel",
    "level_to":             "toClassLevel",
    "type":                 "suspensionType",
    "date_of_suspension":   "cancellationDate",
    "time_of_suspension":   "cancellationTime",
    "date_resumed":         "resumptionDate",
    "time_resumed":         "resumptionTime",
    "remarks":              "remarks",
}

WORK_MAPPING = {
    "type":                 "suspensionType",
    "date_of_suspension":   "cancellationDate",
    "time_of_suspension":   "cancellationTime",
    "date_resumed":         "resumptionDate",
    "time_resumed":         "resumptionTime",
    "remarks":              "remarks",
}

STRANDED_MAPPING = {
    "district":         "district",
    "station":          "station",
    "substation":       "substation",
    "port_terminal":    "portOrTerminalName",
    "passenger":        "strandedPassengers",
    "rolling_cargoes":  "strandedRollingCargoes",
    "vessels_bus_liner":"strandedVessels",
    "mbca_motor_banca": "strandedMotorBancas",
    "remarks":          "remarks",
}

WATER_DIS_MAPPING = {
    "type":                             "disruptionType",
    "service_provider":                 "serviceProvider",
    "date_of_interruption__outage":     "interruptionDate",
    "time_of_interruption__outage":     "interruptionTime",
    "date_restored":                    "restorationDate",
    "time_restored":                    "restorationTime",
    "remarks":                          "remarks",
}

SEAPORT_MAPPING = {
    "name_of_port":                                     "portOrTerminalName",
    "status":                                           "portStatus",
    "date_reported_non-operational__cancelled_trips":   "cancellationDate",
    "time_reported_non-operational__cancelled_trips":   "cancellationTime",
    "date_reported_operational_resumed_trips":          "resumptionDate",
    "time_reported_operational_resumed_trips":          "resumptionTime",
    "date_reported_non-operational":                    "cancellationDate",
    "time_reported_non-operational":                    "cancellationTime",
    "date_cancelled":                                   "cancellationDate",
    "time_cancelled":                                   "cancellationTime",
    "date_reported_operational":                        "resumptionDate",
    "time_reported_operational":                        "resumptionTime",
    "remarks":                                          "remarks",
}

AIRPORT_MAPPING = {
    "name_of_airport":                                  "portOrTerminalName",
    "airport":                                          "portOrTerminalName",
    "airline":                                          "airline",
    "type":                                             "airportType",
    "flight_no":                                        "flightNo",
    "route":                                            "flightRoute",
    "status":                                           "portStatus",
    "stranded_passengers":                              "strandedPassengers",
    "date_reported_non-operational__cancelled_trips":   "cancellationDate",
    "time_reported_non-operational__cancelled_trips":   "cancellationTime",
    "date_reported_non-operational":                    "cancellationDate",
    "time_reported_non-operational":                    "cancellationTime",
    "date_cancelled":                                   "cancellationDate",
    "time_cancelled":                                   "cancellationTime",
    "date_reported_operational_resumed_trips":          "resumptionDate",
    "time_reported_operational_resumed_trips":          "resumptionTime",
    "date_reported_operational":                        "resumptionDate",
    "time_reported_operational":                        "resumptionTime",
    "date_resumed":                                     "resumptionDate",
    "time_resumed":                                     "resumptionTime",
    "remarks":                                          "remarks",
}


# ── Mapping functions ─────────────────────────────────────────────────────────

def event_mapping(g: Graph, ev: Event) -> URIRef:
    uri = event_uri("ndrrmc", ev.id)
    g.add((uri, RDF.type, SKG["MajorEvent"]))
    g.add((uri, URIRef(SKG["eventName"]), Literal(ev.eventName)))
    g.add((uri, URIRef(SKG["hasDisasterType"]), URIRef(SKG[ev.hasDisasterType])))

    if ev.startDate:
        g.add((uri, URIRef(SKG["startDate"]), Literal(ev.startDate, datatype=XSD.dateTime)))
    if ev.endDate:
        g.add((uri, URIRef(SKG["endDate"]), Literal(ev.endDate, datatype=XSD.dateTime)))
    if ev.remarks:
        g.add((uri, URIRef(SKG["remarks"]), Literal(ev.remarks)))

    for measurement in ev.climateParameters:
        if measurement.value is None:
            continue

        measure_uri = climate_param_iri(uri, measurement.id)
        g.add((measure_uri, RDF.type, BAW.ClimateParameter))
        g.add((uri, BAW.hasClimateParameterMeasurement, measure_uri))

        if measurement.parameter:
            g.add((measure_uri, BAW.isOfClimateParameterType, URIRef(SKG[measurement.parameter])))
        g.add((measure_uri, BAW.hasValue, Literal(measurement.value, datatype=XSD.float)))
        if measurement.unit:
            g.add((measure_uri, BAW.hasUnit, Literal(measurement.unit)))
        if measurement.hasLocation:
            g.add((measure_uri, BAW.hasMeasurementLocation, URIRef(measurement.hasLocation)))

    for warning in ev.warnings:
        if not warning.warningReleased:
            continue

        warn_uri = warning_iri(uri, warning.id)
        g.add((warn_uri, RDF.type, SKG.Warning))
        g.add((uri, SKG.hasWarning, warn_uri))
        g.add((warn_uri, SKG.warningReleased, Literal(warning.warningReleased)))

        if warning.warningTimeStamp:
            g.add((warn_uri, SKG.warningTimeStamp, Literal(warning.warningTimeStamp, datatype=XSD.dateTime)))

    return uri


def prov_mapping(g: Graph, prov: Provenance, event_iri: URIRef):
    uri = prov_iri(prov.reportName)

    g.add((uri, RDF.type, URIRef(SKG["Source"])))
    g.add((event_iri, URIRef(PROV["wasDerivedFrom"]), uri))
    g.add((uri, URIRef(SKG["format"]), Literal("pdf")))
    g.add((uri, URIRef(SKG["lastUpdateDate"]), Literal(prov.lastUpdateDate, datatype=XSD.dateTime)))
    g.add((uri, URIRef(SKG["reportName"]), Literal(prov.reportName)))

    if prov.obtainedDate:
        g.add((uri, URIRef(SKG["obtainedDate"]), Literal(prov.obtainedDate, datatype=XSD.dateTime)))
    if prov.reportLink:
        g.add((uri, URIRef(SKG["reportLink"]), Literal(prov.reportLink)))

    g.add((uri, URIRef(PROV["wasAttributedTo"]), URIRef(ORG.NDRRMC)))
    g.add((uri, URIRef(PROV["wasGeneratedBy"]), URIRef(SKG["ndrrmc_website_access"])))


def incident_mapping(g: Graph, inci: List[Incident], event_iri: URIRef):
    for i in inci:
        uri = incident_iri(event_iri, i.id)

        g.add((uri, RDF.type, SKG.Incident))
        g.add((event_iri, SKG.hasRelatedIncident, uri))

        if i.incidentDescription:
            g.add((uri, SKG.incidentDescription, Literal(i.incidentDescription)))
        if i.incidentActionsTaken:
            g.add((uri, SKG.incidentActionsTaken, Literal(i.incidentActionsTaken)))
        if i.remarks:
            g.add((uri, SKG.remarks, Literal(i.remarks)))

        g.add((uri, SKG.startDate, Literal(i.startDate, datatype=XSD.dateTime)))
        if i.endDate:
            g.add((uri, SKG.endDate, Literal(i.endDate, datatype=XSD.dateTime)))

        g.add((uri, SKG.hasDisasterType, URIRef(SKG[i.hasDisasterType])))
        g.add((uri, SKG.hasLocation, URIRef(i.hasLocation)))

        if i.hasBarangay:
            g.add((uri, SKG.hasBarangay, Literal(i.hasBarangay)))


def aff_pop_mapping(g: Graph, aps: List[AffectedPopulation], event_iri: URIRef):
    for ap in aps:
        uri = aff_pop_iri(event_iri, ap.id)

        g.add((uri, RDF.type, SKG.AffectedPopulation))
        g.add((event_iri, SKG.hasAffectedPopulation, uri))

        if int(ap.affectedBarangays) > 1:
            g.add((uri, SKG.affectedBarangays, Literal(ap.affectedBarangays, datatype=XSD.int)))
        elif ap.hasBarangay:
            g.add((uri, SKG.hasBarangay, Literal(ap.hasBarangay)))

        g.add((uri, SKG.affectedFamilies, Literal(ap.affectedFamilies, datatype=XSD.int)))
        g.add((uri, SKG.affectedPersons, Literal(ap.affectedPersons, datatype=XSD.int)))
        g.add((uri, SKG.displacedFamilies, Literal(ap.displacedFamilies, datatype=XSD.int)))
        g.add((uri, SKG.displacedPersons, Literal(ap.displacedPersons, datatype=XSD.int)))
        g.add((uri, SKG.hasLocation, URIRef(ap.hasLocation)))


def casualties_mapping(g: Graph, cas: List[Casualties], event_iri: URIRef):
    for c in cas:
        uri = casualties_iri(event_iri, c.id)

        g.add((uri, RDF.type, SKG.Casualties))
        g.add((event_iri, SKG.hasCasualties, uri))
        g.add((uri, SKG.isOfCasualtyType, casualty_type_to_iri(c.casualtyType)))
        g.add((uri, SKG.casualtyCount, Literal(c.casualtyCount if c.casualtyCount else 1, datatype=XSD.int)))
        g.add((uri, SKG.hasLocation, URIRef(c.hasLocation)))

        if c.casualtyCause:
            g.add((uri, SKG.casualtyCause, Literal(c.casualtyCause)))
        if c.remarks:
            g.add((uri, SKG.remarks, Literal(c.remarks)))
        if c.casualtyDataSource:
            g.add((uri, SKG.casualtyDataSource, Literal(c.casualtyDataSource)))
        if c.hasBarangay:
            g.add((uri, SKG.hasBarangay, Literal(c.hasBarangay)))


def relief_mapping(g: Graph, reliefs: List[Assistance], event_iri: URIRef):
    for r in reliefs:
        uri = assistance_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.Assistance))
        g.add((event_iri, SKG.hasAssistance, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue
            if (type(value) == int or type(value) == float) and value == 0:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "itemCost":
                add_monetary(g, uri, SKG.contributionAmount, value, SKG.PHP_millions)
            elif f.name == "itemQuantity":
                g.add((uri, SKG.itemQuantity, Literal(value, datatype=XSD.decimal)))
            elif f.name == "itemCostPerUnit" and value > 0:
                add_monetary(g, uri, SKG.itemCostPerUnit, value, CUR.PHP)
            elif f.name == "itemSource":
                g.add((uri, SKG.itemSource, Literal(value)))
                for org in ORG_RESOLVER.split_and_resolve(str(value)):
                    g.add((uri, SKG.contributingOrg, org))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


def infra_mapping(g: Graph, infra: List[Infrastructure], event_iri: URIRef):
    for r in infra:
        uri = infra_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.InfrastructureDamage))
        g.add((event_iri, SKG.hasInfrastructureDamage, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue
            if (type(value) == int or type(value) == float) and value == 0:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "infraDamageAmount":
                add_monetary(g, uri, SKG.infraDamageAmount, value, SKG.PHP_millions)
            elif f.name == "numberInfraDamaged" and value < 2:
                continue
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


def housing_mapping(g: Graph, hs: List[Housing], event_iri: URIRef):
    for r in hs:
        uri = housing_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.HousingDamage))
        g.add((event_iri, SKG.hasHousingDamage, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "housingDamageAmount":
                add_monetary(g, uri, SKG.housingDamageAmount, value, SKG.PHP_millions)
            elif f.name == "totallyDamagedHouses":
                g.add((uri, SKG.totallyDamagedHouses, Literal(value, datatype=XSD.int)))
            elif f.name == "partiallyDamagedHouses":
                g.add((uri, SKG.partiallyDamagedHouses, Literal(value, datatype=XSD.int)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


def agri_mapping(g: Graph, hs: List[Agriculture], event_iri: URIRef):
    for r in hs:
        uri = agri_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.AgricultureDamage))
        g.add((event_iri, SKG.hasAgricultureDamage, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "agriDamageAmount":
                add_monetary(g, uri, SKG.agriDamageAmount, value, SKG.PHP_millions)
            elif f.name == "agriDamageQuantity":
                g.add((uri, SKG.agriDamageQuantity, Literal(value, datatype=XSD.decimal)))
            elif f.name == "totallyDamagedCropArea":
                g.add((uri, SKG.totallyDamagedCropArea, Literal(value, datatype=XSD.decimal)))
            elif f.name == "partiallyDamagedCropArea":
                g.add((uri, SKG.partiallyDamagedCropArea, Literal(value, datatype=XSD.decimal)))
            elif f.name == "productionLossCost":
                add_monetary(g, uri, SKG.productionLossCost, value, SKG.PHP_millions)
            elif f.name == "productionLossVolume":
                g.add((uri, SKG.productionLossVolume, Literal(value, datatype=XSD.decimal)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


def pevac_mapping(g: Graph, hs: List[PEvacuation], event_iri: URIRef):
    for r in hs:
        uri = pevac_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.PreemptiveEvacuation))
        g.add((event_iri, SKG.hasPreemptiveEvacuation, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


def rnb_mapping(g: Graph, hs: List[RNB], event_iri: URIRef):
    for r in hs:
        uri = rnb_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.RoadAndBridgesDamage))
        g.add((event_iri, SKG.hasRoadAndBridgesDamage, uri))

        for f in fields(r):
            if f.name == "id":
                continue

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


def power_mapping(g: Graph, hs: List[Power], event_iri: URIRef):
    for r in hs:
        uri = power_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.PowerDisruption))
        g.add((event_iri, SKG.hasPowerDisruption, uri))

        for f in fields(r):
            if f.name == "id":
                continue

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


def comms_mapping(g: Graph, hs: List[CommunicationLines], event_iri: URIRef):
    for r in hs:
        uri = comms_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.CommunicationLineDisruption))
        g.add((event_iri, SKG.hasCommunicationLineDisruption, uri))

        for f in fields(r):
            if f.name == "id":
                continue

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


def doc_mapping(g: Graph, hs: List[DOC], event_iri: URIRef):
    for r in hs:
        uri = doc_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.DeclarationOfCalamity))
        g.add((event_iri, SKG.hasDeclarationOfCalamity, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "resolutionDate":
                g.add((uri, SKG.resolutionDate, Literal(value, datatype=XSD.dateTime)))
            elif f.name == "resolutionNo":
                g.add((uri, SKG.resolutionNo, Literal(str(value), datatype=XSD.string)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


def class_mapping(g: Graph, hs: List[ClassDisruption], event_iri: URIRef):
    for r in hs:
        uri = class_dis_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.ClassSuspension))
        g.add((event_iri, SKG.hasClassSuspension, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "cancellationDateTime":
                g.add((uri, SKG.cancellationDateTime, Literal(value, datatype=XSD.dateTime)))
            elif f.name == "resumptionDateTime":
                g.add((uri, SKG.resumptionDateTime, Literal(value, datatype=XSD.dateTime)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


def work_mapping(g: Graph, hs: List[WorkDisruption], event_iri: URIRef):
    for r in hs:
        uri = work_dis_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.WorkSuspension))
        g.add((event_iri, SKG.hasWorkSuspension, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "cancellationDateTime":
                g.add((uri, SKG.cancellationDateTime, Literal(value, datatype=XSD.dateTime)))
            elif f.name == "resumptionDateTime":
                g.add((uri, SKG.resumptionDateTime, Literal(value, datatype=XSD.dateTime)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


def stranded_mapping(g: Graph, hs: List[Stranded], event_iri: URIRef):
    for r in hs:
        uri = stranded_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.StrandedEvent))
        g.add((event_iri, SKG.hasStrandedEvent, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "strandedPassengers":
                g.add((uri, SKG.strandedPassengers, Literal(value, datatype=XSD.int)))
            elif f.name == "strandedRollingCargoes":
                g.add((uri, SKG.strandedRollingCargoes, Literal(value, datatype=XSD.int)))
            elif f.name == "strandedVessels":
                g.add((uri, SKG.strandedVessels, Literal(value, datatype=XSD.int)))
            elif f.name == "strandedMotorBancas":
                g.add((uri, SKG.strandedMotorBancas, Literal(value, datatype=XSD.int)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


def water_mapping(g: Graph, hs: List[WaterDisruption], event_iri: URIRef):
    for r in hs:
        uri = water_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.WaterDisruption))
        g.add((event_iri, SKG.hasWaterDisruption, uri))

        for f in fields(r):
            if f.name == "id":
                continue

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


def seaport_mapping(g: Graph, hs: List[Seaport], event_iri: URIRef):
    for r in hs:
        uri = seaport_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.SeaportDisruption))
        g.add((event_iri, SKG.hasSeaportDisruption, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "cancellationDateTime":
                g.add((uri, SKG.cancellationDateTime, Literal(value, datatype=XSD.dateTime)))
            elif f.name == "resumptionDateTime":
                g.add((uri, SKG.resumptionDateTime, Literal(value, datatype=XSD.dateTime)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


def airport_mapping(g: Graph, hs: List[Airport], event_iri: URIRef):
    for r in hs:
        uri = airport_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.AirportDisruption))
        g.add((event_iri, SKG.hasAirportDisruption, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "cancellationDateTime":
                g.add((uri, SKG.cancellationDateTime, Literal(value, datatype=XSD.dateTime)))
            elif f.name == "resumptionDateTime":
                g.add((uri, SKG.resumptionDateTime, Literal(value, datatype=XSD.dateTime)))
            elif f.name == "strandedPassengers":
                g.add((uri, SKG.strandedPassengers, Literal(value, datatype=XSD.int)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))


def flight_mapping(g: Graph, hs: List[Flight], event_iri: URIRef):
    for r in hs:
        uri = flight_iri(event_iri, r.id)

        g.add((uri, RDF.type, SKG.FlightDisruption))
        g.add((event_iri, SKG.hasFlightDisruption, uri))

        for f in fields(r):
            if f.name == "id":
                continue

            value = getattr(r, f.name)
            if value is None:
                continue

            if f.name == "hasLocation":
                g.add((uri, SKG.hasLocation, URIRef(str(value))))
            elif f.name == "cancellationDateTime":
                g.add((uri, SKG.cancellationDateTime, Literal(value, datatype=XSD.dateTime)))
            elif f.name == "resumptionDateTime":
                g.add((uri, SKG.resumptionDateTime, Literal(value, datatype=XSD.dateTime)))
            else:
                g.add((uri, getattr(SKG, f.name), Literal(value)))

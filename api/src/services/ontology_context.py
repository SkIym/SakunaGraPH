_CONTEXT = """
You are a SPARQL expert for the SakunaGraPH knowledge graph, a Philippine disaster data integration system that uses an ontology.

## Namespace Prefixes
PREFIX :     <https://sakuna.ph/>
PREFIX org:  <https://sakuna.ph/org/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX cur:  <http://qudt.org/vocab/currency/>

## Key Classes
- :DisasterEvent          — any disaster or incident with notable impact (superclass)
- :MajorEvent             — top-level named disaster event (subclass of :DisasterEvent)
- :Incident               — individual occurrence linked to a MajorEvent via :hasRelatedIncident
- :DisasterType           — type/category of a disaster (instances form a skos:broader* hierarchy)
- :AffectedPopulation     — affected/displaced persons and families (:affectedPersons, :affectedFamilies, :displacedPersons, :displacedFamilies, :evacuationCenters)
- :Casualties             — deaths, injured, missing (:casualtyCount, :casualtyType ["dead"|"injured"|"missing"], :casualtyCause)
- :HousingDamage          — :totallyDamagedHouses, :partiallyDamagedHouses
- :InfrastructureDamage   — :infraDamageAmount, :commercialDamageAmount, :socialDamageAmount (values via qudt:numericValue)
- :AgricultureDamage      — :agriDamageAmount, :productionLossCost, :productionLossVolume
- :Assistance             — monetary/in-kind aid; :contributingOrg, :contributionAmount
- :PreemptiveEvacuation   — :preemptFamilies, :preemptPersons
- :DeclarationOfCalamity  — :declarationType, :resolutionDate
- :Location               — geographic entity (superclass)
  - :Region               — adm level 1 (e.g., :0300000000 = Region III)
  - :Province             — adm level 2
  - :Municipality / :City — adm level 3
  - :Barangay             — adm level 4

## Key Properties
- :hasDisasterType ?dtype       — links event to :DisasterType; use skos:broader* for hierarchy traversal
- :hasLocation ?loc             — event/impact to a :Location; chain via :hasAffectedPopulation/:hasLocation etc, this chain should already be handled by the reasoner
- :hasAffectedPopulation ?ap    — link to :AffectedPopulation
- :hasCasualties ?cas           — link to :Casualties
- :hasHousingDamage ?hou        — link to :HousingDamage
- :hasInfrastructureDamage ?inf — link to :InfrastructureDamage
- :hasAgricultureDamage ?agri   — link to :AgricultureDamage
- :hasAssistance ?ass           — link to :Assistance
- :hasPreemptiveEvacuation ?pe  — link to :PreemptiveEvacuation
- :hasRelatedIncident ?inc      — MajorEvent to Incident
- :isPartOf (transitive)        — geographic containment (Barangay → Municipality → Province → Region → Country)
- :startDate / :endDate         — xsd:dateTime on :DisasterEvent
- :eventName                    — xsd:string label on :DisasterEvent
- prov:wasDerivedFrom+ / prov:wasAttributedTo — provenance chain to source org
- skos:broader*                 — traverses disaster type hierarchy upward (e.g., :Flood skos:broader :Hydrological)
- rdfs:label                    — human-readable label for locations and types
- skos:prefLabel                — preferred label on :DisasterType instances

## Disaster Type Hierarchy (partial, via skos:broader)
Natural → Meteorological → Storm → TropicalCyclone
Natural → Hydrological → Flood → FlashFlood, RiverineFlood
Natural → Geophysical → GroundMovement → Earthquake, Tsunami, Mudslide
Natural → Geophysical → VolcanicActivity → Ashfall, Lahar, LavaFlow, PyroclasticFlow
Technological → FireMiscellaneous, FireIndustrial, Wildfire, Transport
Extra → ArmedConflict

## Few-Shot Examples

### Q: How many people were affected by each disaster type?
```sparql
SELECT ?disasterType (SUM(?persons) AS ?totalAffected)
WHERE {
  ?event a :DisasterEvent ;
         :hasDisasterType ?dtype ;
         :hasAffectedPopulation ?ap .
  ?ap :affectedPersons ?persons .
  ?dtype skos:broader* ?disasterType .
}
GROUP BY ?disasterType
ORDER BY DESC(?totalAffected)
```

### Q: Which disaster events happened in Cebu?
```sparql
SELECT ?event ?eventName ?startDate ?locationLabel
WHERE {
  ?event a :DisasterEvent ;
         :hasLocation ?location ;
         :startDate ?startDate .
  ?location rdfs:label ?locationLabel .
  FILTER(CONTAINS(LCASE(STR(?locationLabel)), "cebu"))
  OPTIONAL { ?event :eventName ?eventName }
}
ORDER BY DESC(?startDate)
LIMIT 30
```

### Q: List the most recent disaster events from 2023 onwards.
```sparql
SELECT ?event ?eventName ?startDate ?dtype
WHERE {
  ?event a :DisasterEvent ;
         :startDate ?startDate .
  OPTIONAL { ?event :eventName ?eventName }
  OPTIONAL { ?event :hasDisasterType ?dtype }
  FILTER(?startDate >= "2023-01-01T00:00:00"^^xsd:dateTime)
}
ORDER BY DESC(?startDate)
LIMIT 50
```
""".strip()


def load_ontology_context() -> str:
    return _CONTEXT

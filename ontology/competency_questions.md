# Competency Questions

### Namespace Prefix
These are the prefixes for the namespaces needed to run SPARQL queries against the knowledge graph.

```
PREFIX :     <https://sakuna.ph/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX cur:  <http://qudt.org/vocab/currency/>

```

---

### Disaster Event & Type Classification

**CQ1.** What major events are classified under the Tropical Cyclone type (a narrower type of Storm → Meteorological → Natural), and which of them also have related incidents explicitly recorded?

```
SELECT DISTINCT ?event ?eventName ?startDate ?hasRelatedIncident
    (IF (EXISTS { ?event :hasRelatedIncident ?any }, "Yes", "No") AS ?hasRelatedIncident)
WHERE {
    ?event  a                   :MajorEvent ;
            :hasDisasterType    :TropicalCyclone ;
            :startDate          ?startDate .

    OPTIONAL { ?event :eventName  ?eventName }
    OPTIONAL { ?event :hasRelatedIncident ?incident }
}
ORDER BY DESC(?startDate)


```

**CQ2.** Which disaster events (major or incident) are typed as Flash Flood or Riverine Flood (both narrower types of Flood → Hydrological), and what locations, from which regions, of the Philippines did they affect?

```
SELECT ?event ?eventName ?startDate ?location ?locLabel ?regionLabel ?disasterType
WHERE {
    VALUES ?disasterType { :FlashFlood :RiverineFlood }
    ?event  a                :DisasterEvent ;
            :hasDisasterType ?disasterType ;
            :startDate       ?startDate ;
            :hasLocation ?location .

    ?location rdfs:label ?locLabel .

    OPTIONAL { ?event :eventName ?eventName }
    
    ?location :isPartOf* ?region .
    ?region a :Region ;
            rdfs:label ?regionLabel .

}
ORDER BY DESC(?startDate)

```

**CQ3.** What is the full type hierarchy path from Mudslide up to its top-level parent, and how many disaster events are recorded under each level of that hierarchy?

```
SELECT ?ancestor ?ancestorLabel (COUNT(DISTINCT ?event) AS ?eventCount) 
WHERE {
  	:Mudslide skos:broader* ?ancestor .
  	OPTIONAL { 	?ancestor skos:prefLabel ?ancestorLabel }
  	OPTIONAL { 	?dt skos:broader* ?ancestor .
             	?event a 	:DisasterEvent ; 
        				:hasDisasterType ?dt }
} 
GROUP BY ?ancestor ?ancestorLabel ORDER BY DESC(?eventCount)
```

**CQ4.** Which disaster events are classified as Volcanic Activity subtypes (e.g., Ashfall, Lahar, Lava Flow, Pyroclastic Flow), and what provinces in Region V , Region III, Region VI were affected?

```
SELECT DISTINCT ?eventName ?startDate ?subtypeLabel ?provinceName ?regionName
WHERE {
  VALUES ?targetRegion { :0500000000 :0300000000 :0600000000}

    ?event a :DisasterEvent ;
                :hasDisasterType ?subtype ;
                :startDate ?startDate ; 
                :hasLocation ?location .

    ?subtype skos:broader* :VolcanicActivity .

    ?location :isPartOf* ?targetRegion .

    ?location :isPartOf* ?province .
    ?province a :Province ;
                rdfs:label ?provinceName .

    ?province :isPartOf+ ?region .
    ?region a :Region ;
            rdfs:label ?regionName .

    OPTIONAL { ?event    :eventName  ?eventName     }
    OPTIONAL { ?subtype  skos:prefLabel  ?subtypeLabel  }

}
ORDER BY ?subtype ?provinceName
```

**CQ5.** How many fire, transport, and armed conflict events or incidents were recorded in 2022 in Central Luzon and CALABARZON ?

```
SELECT ?eventType ?region (COUNT(DISTINCT ?event) AS ?eventCount)
WHERE {
    VALUES ?dtypes {:FireIndustrial :FireMiscellaneous :Wildfire :Transport :ArmedConflict }
    VALUES ?targetRegions {:0300000000 :0400000000}
    ?event 	a	:DisasterEvent ;
            :startDate 	?startDate ;
            :hasLocation ?location ;
            :hasDisasterType ?eventType .
    
    ?location 	:isPartOf* ?targetRegions .
    ?targetRegions rdfs:label ?region .
    
    ?eventType skos:broader* ?dtypes .
    
    FILTER (YEAR(?startDate) = 2022) 
} 
GROUP BY ?eventType ?region
ORDER BY DESC(?eventCount)
```

### Impact: Casualties & Population

**CQ6.** For disaster events affecting Western Visayas, what is the total number of displaced families and persons recorded across all events?

```
SELECT ?event ?eventName (SUM(?dfamilies) AS ?totalFamilies) (SUM(?dpersons) AS ?totalPersons) 
WHERE {
    ?event 	a	:DisasterEvent ;
            :startDate 	?startDate ;
            :hasLocation ?location ;
    		:hasAffectedPopulation ?ap .
    
    ?location 	:isPartOf* :0600000000 .
    ?ap			:hasLocation 	?location ;
    			:displacedPersons ?dpersons ;
    			:displacedFamilies ?dfamilies ; 
    
    OPTIONAL { ?event :eventName ?eventName }


} 
GROUP BY ?eventName ?event
ORDER BY DESC(?totalPersons) DESC(?totalFamilies)
```

**CQ7.** What casualty counts (dead, missing, injured) were recorded for Ground Movement events, broken down by province in the Cordillera Administrative Region (CAR)?

```
SELECT DISTINCT ?provName ?casualtyType (SUM(?casualtyCount) AS ?total) 
WHERE {
    ?event 	a	:DisasterEvent ;
            :hasLocation ?location ;
    		:hasDisasterType ?dtype ;
    		:hasCasualties ?cas .
    
    ?dtype		skos:broader* :GroundMovement .
    ?location 	:isPartOf* ?prov .
    ?prov		a		:Province ;
    			rdfs:label 	?provName ;
            	:isPartOf+  :1400000000 .
            
    ?cas		:hasLocation 	?location ;
                :casualtyCount ?casualtyCount ;
                :casualtyType  ?casualtyType .
 
} 
GROUP BY ?provName ?casualtyType
ORDER BY DESC(?total)
```

**CQ8.** How many evacuation centers were recorded for Tropical Cyclone events that affected Region IV-A (CALABARZON) in 2021, and from which sources are these attributed?

```
SELECT ?source (SUM(?evacCount) AS ?total) 
WHERE {
    ?event 	a	:DisasterEvent ;
    		:hasDisasterType ?dtype ;
    		:hasPreemptiveEvacuation ?pevac ;
    		prov:wasDerivedFrom/prov:wasAttributedTo ?source .
    
    ?dtype		skos:broader* :TropicalCyclone .
    ?location 	:isPartOf* 	:0400000000 .
    ?pevac		:hasLocation 	?location ;
                :evacuationCenters ?evacCount .
    
 
} 
GROUP BY ?source
ORDER BY DESC(?total)
```

### Impact: Damage

**CQ9.** # What is the recorded total infrastructure damage cost in M PHP for Flood events in Region XIII (Caraga)?


```
SELECT ?event ?eventName 
	(
    SUM(
        COALESCE(?comVal, 0) + 
        COALESCE(?croVal, 0) + 
        COALESCE(?infVal, 0) +
        COALESCE(?socVal, 0)
    ) AS ?total)
WHERE {
    ?event  a                       :DisasterEvent ;
            :hasDisasterType        ?dtype ;
            :hasInfrastructureDamage ?infra ;
            :hasLocation            ?location .

    ?dtype      skos:broader*   :Flood .
    ?location :isPartOf*   :1600000000 .
    ?infra  :hasLocation        ?location .
    
    ?location rdfs:label ?locationLabel

    OPTIONAL { ?infra :commercialDamageAmount    [ qudt:numericValue ?comVal ] }
    OPTIONAL { ?infra :crossSectoralDamageAmount [ qudt:numericValue ?croVal ] }
    OPTIONAL { ?infra :infraDamageAmount         [ qudt:numericValue ?infVal ] }
    OPTIONAL { ?infra :socialDamageAmount        [ qudt:numericValue ?socVal ] }
    OPTIONAL { ?event :eventName ?eventName }

}
GROUP BY ?event ?eventName
ORDER BY DESC(?total)
```

**CQ10.**  How many recorded houses were totally versus partially damaged by Meteorological events in provinces of Region VIII (Eastern Visayas)?

```
SELECT ?provName
	(SUM(?partially) AS ?totalPartially)	
	(SUM(?totally) AS ?totalTotally)
WHERE {
    ?event  a                       :DisasterEvent ;
            :hasDisasterType        ?dtype ;
            :hasHousingDamage 		?hou ;
            :hasLocation            ?location .

    ?dtype      skos:broader*   :Meteorological .
    ?location 	:isPartOf* ?prov .
    ?prov		a		:Province ;
    			rdfs:label 	?provName ;
            	:isPartOf+  :0800000000 .
    
    ?hou  		:hasLocation    ?location .
    
    ?location rdfs:label ?locationLabel

    OPTIONAL { ?hou :partiallyDamagedHouses ?partially }
    OPTIONAL { ?hou :totallyDamagedHouses ?totally }
    OPTIONAL { ?event :eventName ?eventName }

}
GROUP BY ?provName
```

**CQ11.** For Tropical Cyclone events affecting Region V (Bicol Region), what agriculture damage was recorded? Give either the total amount damage to agriculture or the production loss volume (in metric tons) and total production loss cost.

```
SELECT ?event ?eventName
	(SUM(?admg) AS ?admg)	
	(SUM(?plc) AS ?plc)
	(SUM(?plv) AS ?plv)
WHERE {
    ?event  a                       :DisasterEvent ;
            :hasDisasterType        ?dtype ;
            :hasAgricultureDamage 	?agri ;
            :hasLocation            ?location .

    ?dtype      skos:broader*   :TropicalCyclone .
    ?location 	:isPartOf* :0500000000 .
    ?agri  		:hasLocation    ?location .

    OPTIONAL { ?agri :productionLossCost 	[ qudt:numericValue ?plc] }
    OPTIONAL { ?agri :agriDamageAmount 		[ qudt:numericValue ?admg] }
    OPTIONAL { ?agri :productionLossVolume 	?plv }
    OPTIONAL { ?event :eventName ?eventName }

}
GROUP BY ?event ?eventName
```

### Impact: Service Disruptions

**CQ12.** Which Tropical Cyclone events caused seaport disruptions, what ports or terminals in which location were affected, and what was the latest port status recorded?

```
SELECT ?event ?eventName ?port ?locationLabel ?status
WHERE {
    ?event  a                       :DisasterEvent ;
            :hasDisasterType        ?dtype ;
            :hasSeaportDisruption 	?sd .

    ?dtype      skos:broader*   :TropicalCyclone .
    ?sd  		:hasLocation    ?location .
    ?location   rdfs:label 		?locationLabel

    OPTIONAL { ?sd :portOrTerminalName 	?port }
    OPTIONAL { ?sd :portStatus 			?status }
    OPTIONAL { ?event :eventName ?eventName }

}
```

**CQ13.** For Geophysical events, which airports in which locations experienced disruptions, and how long in hours were the flights cancelled for?

```
PREFIX ofn: <http://www.ontotext.com/sparql/functions/>
SELECT ?event ?eventName ?port ?locationLabel ?duration
WHERE {
    ?event  a                       :DisasterEvent ;
            :hasDisasterType        ?dtype ;
            :hasAirportDisruption 	?ad .

    ?dtype      skos:broader*   :Geophysical .
    ?ad  		:hasLocation    ?location .
    ?location   rdfs:label 		?locationLabel

    OPTIONAL { ?ad :portOrTerminalName 	?port }
    OPTIONAL { ?ad :cancellationDateTime 	?cdt }
    OPTIONAL { ?ad :resumptionDateTime 	?rdt }
    OPTIONAL { ?event :eventName ?eventName }
    BIND ( ofn:asHours(?rdt - ?cdt)  AS ?duration)

}

```

**CQ14.** Which disaster events caused class suspensions in the National Capital Region (NCR), and at what grade levels were suspensions imposed?

```
SELECT ?event ?eventName ?label ?fromLevel ?toLevel
WHERE {
    ?event 	a	:DisasterEvent ;
                :hasClassSuspension	?cls .

    ?cls :hasLocation ?location .
    ?location :isPartOf* :1300000000 .
    ?location rdfs:label ?label .

    OPTIONAL { ?event  :eventName     ?eventName }
    OPTIONAL { ?cls :fromClassLevel   ?fromLevel }
    OPTIONAL { ?cls :toClassLevel     ?toLevel   }
    }
```

### Response

**CQ15.** What assistance was provided in response to disaster events in Isabela, broken down by source, and which organizations contributed?

```
    
```

**CQ16.** Which disaster events triggered a Declaration of Calamity in provinces of Region XII (SOCCSKSARGEN), what type of declaration was issued , and on what date was it resolved?

```

```

**CQ17.** What rescue operations were conducted during Landslide (Rock) or Mudslide events in Cordillera Administrative Region (CAR), including rescue units deployed and equipment used?

```

```

### Preparedness & Provenance

**CQ18.** What preparedness activities were recorded for disaster events affecting municipalities with 1st income class in Luzon?

```

```

**CQ19.** What recovery measures were recorded for Flood (General) events in MIMAROPA Region or Region X (Northern Mindanao)?

```

```

**CQ20.** Which disaster events were reported in more than one source?

```

```

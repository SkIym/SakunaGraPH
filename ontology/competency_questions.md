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

**CQ1.** What major events are classified under the Tropical Cyclone type (a narrower type of Storm → Meteorological → Natural), and which of them also have related landslide incidents recorded?

```
SELECT ?event ?eventName ?startDate
WHERE {
    ?event  a                   :MajorEvent ;
            :hasDisasterType    :TropicalCyclone ;
            :startDate          ?startDate ;
            :hasRelatedIncident ?incident .
    ?incident  :hasDisasterType :LandslideWet .
    OPTIONAL { ?event :eventName  ?eventName }
    OPTIONAL { ?event :hasRelatedIncident ?incident }
}
ORDER BY DESC(?startDate)


```

**CQ2.** Which disaster events (major or incident) are typed as Flash Flood or Riverine Flood (both narrower types of Flood → Hydrological), and what locations, from which regions, of the Philippines did they affect?

```
SELECT ?event ?eventName ?startDate ?location ?locLabel ?regionLabel
WHERE {
    VALUES ?eventType { :Incident :MajorEvent }
    VALUES ?disasterType { :FlashFlood :RiverineFlood }
    ?event  a                ?eventType ;
            :hasDisasterType ?disasterType ;
            :startDate       ?startDate .
#    ?event :hasLocation ?location .
  	?event (
        :hasLocation
        | :hasImpact/:hasLocation
        | :hasResponse/:hasLocation
        | :hasRelatedIncident/:hasLocation
        | :hasPreparedness/:hasLocation
    ) ?location .

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

```

**CQ4.** Which disaster events are classified as Volcanic Activity subtypes (e.g., Ashfall, Lahar, Lava Flow, Pyroclastic Flow), and what provinces in Region V , Region III, Region VI were affected?

```

```

**CQ5.** How many fire, transport, and armed conflict events or incidents occurred in 2018 in Central Luzon?

```

```

### Impact: Casualties & Population

**CQ6.** For disaster events affecting Iloilo from 2023 to 2024, what is the total number of displaced families and persons recorded across all events?

```

```

**CQ7.** What casualty counts (dead, missing, injured) are recorded for Ground Movement events, broken down by province in the Cordillera Administrative Region (CAR)?

```

```

**CQ8.** How many evacuation centers were recorded for Tropical Cyclone events that affected Region IV-A (CALABARZON) in 2021, and from which sources are these attributed?

```

```

### Impact: Damage

**CQ9.** What is the total infrastructure damage cost (road and bridges, commercial, cross-sectoral) for Flash Flood events in Region XIII (Caraga)?

```

```

**CQ10.**  How many houses were totally versus partially damaged by Storm Surge events in provinces of Region VIII (Eastern Visayas), and what is the associated housing damage cost?

```

```

**CQ11.** Which disaster events caused both agriculture damage and infrastructure damage simultaneously, and what is the ratio of agriculture damage cost to infrastructure damage cost per event?

```

```

### Impact: Service Disruptions

**CQ12.** Which Tropical Cyclone events caused seaport disruptions, what ports or terminals were affected, and what was the port status  during the event?

```

```

**CQ13.** For Earthquake or Volcanic Activity events, which airports experienced disruptions, what airlines were affected, and when were flight services restored?

```

```

**CQ14.** Which disaster events caused class suspensions in the National Capital Region (NCR), and at what grade levels were suspensions imposed?

```

```

### Response

**CQ15.** What assistance was provided in response to disaster events in Region V (Bicol Region), broken down by source, and which organizations contributed?

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

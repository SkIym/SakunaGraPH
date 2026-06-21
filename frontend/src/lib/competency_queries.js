const P = `PREFIX :     <https://sakuna.ph/>
PREFIX org:  <https://sakuna.ph/org/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX cur:  <http://qudt.org/vocab/currency/>

`;

export const COMPETENCY_QUESTIONS = [
  {
    label: 'CQ1 — Tropical Cyclone events with related incidents',
    query: P + `SELECT DISTINCT ?event ?eventName ?startDate
    (IF (EXISTS { ?event :hasRelatedIncident ?any }, "Yes", "No") AS ?hasRelatedIncident)
WHERE {
    ?event  a                   :MajorEvent ;
            :hasDisasterType    :TropicalCyclone ;
            :startDate          ?startDate .

    OPTIONAL { ?event :eventName  ?eventName }
    OPTIONAL { ?event :hasRelatedIncident ?incident }
}
ORDER BY DESC(?startDate)`
  },
  {
    label: 'CQ2 — Flash Flood / Riverine Flood events by location & region',
    query: P + `SELECT ?event ?eventName ?startDate ?location ?locLabel ?regionLabel ?disasterType
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
ORDER BY DESC(?startDate)`
  },
  {
    label: 'CQ3 — Mudslide type hierarchy & event counts per level',
    query: P + `SELECT ?ancestor ?ancestorLabel (COUNT(DISTINCT ?event) AS ?eventCount)
WHERE {
    :Mudslide skos:broader* ?ancestor .
    OPTIONAL { ?ancestor skos:prefLabel ?ancestorLabel }
    OPTIONAL { ?dt skos:broader* ?ancestor .
               ?event a :DisasterEvent ;
                        :hasDisasterType ?dt }
}
GROUP BY ?ancestor ?ancestorLabel
ORDER BY DESC(?eventCount)`
  },
  {
    label: 'CQ4 — Volcanic subtypes affecting Regions III, V, VI',
    query: P + `SELECT DISTINCT ?eventName ?startDate ?subtypeLabel ?provinceName ?regionName
WHERE {
    VALUES ?targetRegion { :0500000000 :0300000000 :0600000000 }

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

    OPTIONAL { ?event   :eventName     ?eventName    }
    OPTIONAL { ?subtype skos:prefLabel ?subtypeLabel }
}
ORDER BY ?subtype ?provinceName`
  },
  {
    label: 'CQ5 — Fire / Transport / Armed Conflict events in 2022 (C. Luzon & CALABARZON)',
    query: P + `SELECT ?eventType ?region (COUNT(DISTINCT ?event) AS ?eventCount)
WHERE {
    VALUES ?dtypes { :FireIndustrial :FireMiscellaneous :Wildfire :Transport :ArmedConflict }
    VALUES ?targetRegions { :0300000000 :0400000000 }
    ?event  a            :DisasterEvent ;
            :startDate   ?startDate ;
            :hasLocation ?location ;
            :hasDisasterType ?eventType .

    ?location     :isPartOf* ?targetRegions .
    ?targetRegions rdfs:label ?region .
    ?eventType     skos:broader* ?dtypes .

    FILTER (YEAR(?startDate) = 2022)
}
GROUP BY ?eventType ?region
ORDER BY DESC(?eventCount)`
  },
  {
    label: 'CQ6 — Displaced families & persons in Western Visayas',
    query: P + `SELECT ?event ?eventName
    (SUM(?dfamilies) AS ?totalFamilies)
    (SUM(?dpersons)  AS ?totalPersons)
WHERE {
    ?event  a            :DisasterEvent ;
            :startDate   ?startDate ;
            :hasLocation ?location ;
            :hasAffectedPopulation ?ap .

    ?location :isPartOf* :0600000000 .
    ?ap       :hasLocation       ?location ;
              :displacedPersons  ?dpersons ;
              :displacedFamilies ?dfamilies .

    OPTIONAL { ?event :eventName ?eventName }
}
GROUP BY ?eventName ?event
ORDER BY DESC(?totalPersons) DESC(?totalFamilies)`
  },
  {
    label: 'CQ7 — Ground Movement casualties by province (CAR)',
    query: P + `SELECT DISTINCT ?provName ?casualtyType (SUM(?casualtyCount) AS ?total)
WHERE {
    ?event  a            :DisasterEvent ;
            :hasLocation ?location ;
            :hasDisasterType ?dtype ;
            :hasCasualties ?cas .

    ?dtype    skos:broader* :GroundMovement .
    ?location :isPartOf* ?prov .
    ?prov     a           :Province ;
              rdfs:label  ?provName ;
              :isPartOf+  :1400000000 .

    ?cas  :hasLocation  ?location ;
          :casualtyCount ?casualtyCount ;
          :isOfCasualtyType  ?casualtyType .
}
GROUP BY ?provName ?casualtyType
ORDER BY DESC(?total)`
  },
  {
    label: 'CQ8 — Evacuation centers for Tropical Cyclone in CALABARZON 2021',
    query: P + `SELECT ?source (SUM(?evacCount) AS ?total)
WHERE {
    ?event  a            :DisasterEvent ;
            :hasDisasterType ?dtype ;
            :hasPreemptiveEvacuation ?pevac ;
            prov:wasDerivedFrom+/prov:wasAttributedTo ?source .

    ?dtype    skos:broader* :TropicalCyclone .
    ?location :isPartOf*   :0400000000 .
    ?pevac    :hasLocation  ?location ;
              :evacuationCenters ?evacCount .
}
GROUP BY ?source
ORDER BY DESC(?total)`
  },
  {
    label: 'CQ9 — Infrastructure damage cost for Flood events in Region XIII',
    query: P + `SELECT ?event ?eventName
    (SUM(
        COALESCE(?comVal, 0) +
        COALESCE(?croVal, 0) +
        COALESCE(?infVal, 0) +
        COALESCE(?socVal, 0)
    ) AS ?total)
WHERE {
    ?event  a                        :DisasterEvent ;
            :hasDisasterType         ?dtype ;
            :hasInfrastructureDamage ?infra ;
            :hasLocation             ?location .

    ?dtype    skos:broader* :Flood .
    ?location :isPartOf*    :1600000000 .
    ?infra    :hasLocation  ?location .
    ?location rdfs:label    ?locationLabel .

    OPTIONAL { ?infra :commercialDamageAmount    [ qudt:numericValue ?comVal ] }
    OPTIONAL { ?infra :crossSectoralDamageAmount [ qudt:numericValue ?croVal ] }
    OPTIONAL { ?infra :infraDamageAmount         [ qudt:numericValue ?infVal ] }
    OPTIONAL { ?infra :socialDamageAmount        [ qudt:numericValue ?socVal ] }
    OPTIONAL { ?event :eventName ?eventName }
}
GROUP BY ?event ?eventName
ORDER BY DESC(?total)`
  },
  {
    label: 'CQ10 — Housing damage by Meteorological events in Region VIII',
    query: P + `SELECT ?provName
    (SUM(?partially) AS ?totalPartially)
    (SUM(?totally)   AS ?totalTotally)
WHERE {
    ?event  a                   :DisasterEvent ;
            :hasDisasterType    ?dtype ;
            :hasHousingDamage   ?hou ;
            :hasLocation        ?location .

    ?dtype    skos:broader* :Meteorological .
    ?location :isPartOf* ?prov .
    ?prov     a          :Province ;
              rdfs:label ?provName ;
              :isPartOf+ :0800000000 .

    ?hou :hasLocation ?location .
    ?location rdfs:label ?locationLabel .

    OPTIONAL { ?hou :partiallyDamagedHouses ?partially }
    OPTIONAL { ?hou :totallyDamagedHouses   ?totally   }
    OPTIONAL { ?event :eventName ?eventName }
}
GROUP BY ?provName`
  },
  {
    label: 'CQ11 — Agriculture damage by Tropical Cyclone in Region V (Bicol)',
    query: P + `SELECT ?event ?eventName
    (SUM(?admg) AS ?admg)
    (SUM(?plc)  AS ?plc)
    (SUM(?plv)  AS ?plv)
WHERE {
    ?event  a                    :DisasterEvent ;
            :hasDisasterType     ?dtype ;
            :hasAgricultureDamage ?agri ;
            :hasLocation         ?location .

    ?dtype    skos:broader* :TropicalCyclone .
    ?location :isPartOf*    :0500000000 .
    ?agri     :hasLocation  ?location .

    OPTIONAL { ?agri :productionLossCost   [ qudt:numericValue ?plc  ] }
    OPTIONAL { ?agri :agriDamageAmount     [ qudt:numericValue ?admg ] }
    OPTIONAL { ?agri :productionLossVolume ?plv }
    OPTIONAL { ?event :eventName ?eventName }
}
GROUP BY ?event ?eventName`
  },
  {
    label: 'CQ12 — Seaport disruptions caused by Tropical Cyclones',
    query: P + `SELECT ?event ?eventName ?port ?locationLabel ?status
WHERE {
    ?event  a                    :DisasterEvent ;
            :hasDisasterType     ?dtype ;
            :hasSeaportDisruption ?sd .

    ?dtype skos:broader* :TropicalCyclone .
    ?sd    :hasLocation  ?location .
    ?location rdfs:label ?locationLabel .

    OPTIONAL { ?sd    :portOrTerminalName ?port   }
    OPTIONAL { ?sd    :portStatus        ?status  }
    OPTIONAL { ?event :eventName         ?eventName }
}`
  },
  {
    label: 'CQ13 — Airport disruptions from Geophysical events (duration in hours)',
    query: `PREFIX ofn:  <http://www.ontotext.com/sparql/functions/>
` + P + `SELECT ?event ?eventName ?port ?locationLabel ?duration
WHERE {
    ?event  a                    :DisasterEvent ;
            :hasDisasterType     ?dtype ;
            :hasAirportDisruption ?ad .

    ?dtype skos:broader* :Geophysical .
    ?ad    :hasLocation  ?location .
    ?location rdfs:label ?locationLabel .

    OPTIONAL { ?ad    :portOrTerminalName  ?port }
    OPTIONAL { ?ad    :cancellationDateTime ?cdt }
    OPTIONAL { ?ad    :resumptionDateTime   ?rdt }
    OPTIONAL { ?event :eventName            ?eventName }
    BIND (ofn:asHours(?rdt - ?cdt) AS ?duration)
}`
  },
  {
    label: 'CQ14 — Class suspensions in NCR by grade level',
    query: P + `SELECT ?event ?eventName ?label ?fromLevel ?toLevel
WHERE {
    ?event  a                   :DisasterEvent ;
            :hasClassSuspension ?cls .

    ?cls      :hasLocation ?location .
    ?location :isPartOf*   :1300000000 .
    ?location rdfs:label   ?label .

    OPTIONAL { ?event :eventName       ?eventName }
    OPTIONAL { ?cls   :fromClassLevel  ?fromLevel }
    OPTIONAL { ?cls   :toClassLevel    ?toLevel   }
}`
  },
  {
    label: 'CQ15 — Assistance provided in Isabela by source & organization',
    query: P + `SELECT ?event ?eventName ?src ?contribution ?type ?org
WHERE {
    ?event  a              :DisasterEvent ;
            :hasAssistance ?ass ;
            prov:wasDerivedFrom+/prov:wasAttributedTo ?src .

    ?ass      :hasLocation ?location .
    ?location :isPartOf*   :0203100000 .

    OPTIONAL { ?ass   :itemTypeOrNeeds   ?type         }
    OPTIONAL { ?ass   :contributingOrg   ?org          }
    OPTIONAL { ?ass   :contributionAmount [ qudt:numericValue ?contribution ] }
    OPTIONAL { ?event :eventName         ?eventName    }
}`
  },
  {
    label: 'CQ16 — Declarations of Calamity in Region XII (SOCCSKSARGEN)',
    query: P + `SELECT ?provName
    (GROUP_CONCAT(
        CONCAT(
            COALESCE(STR(?eventName), "no name"), " — ",
            COALESCE(STR(?date), "unknown"), " — ",
            STR(?locationLabel)
        ); separator="; ") AS ?event_list)
WHERE {
    ?event  a                         :DisasterEvent ;
            :hasLocation              ?location ;
            :hasDeclarationOfCalamity ?dec .

    ?dec      :hasLocation ?location .
    ?prov     a            :Province ;
              rdfs:label   ?provName ;
              :isPartOf+   :1200000000 .
    ?location :isPartOf*   ?prov ;
              rdfs:label   ?locationLabel .

    OPTIONAL { ?dec   :declarationType ?type       }
    OPTIONAL { ?dec   :resolutionDate  ?date       }
    OPTIONAL { ?event :eventName       ?eventName  }
}
GROUP BY ?provName`
  },
  {
    label: 'CQ17 — Rescue operations in Mindanao (units & equipment)',
    query: P + `SELECT DISTINCT ?event ?eventName ?unit ?equip
WHERE {
    ?event  a          :DisasterEvent ;
            :hasRescue ?res .

    ?res      :hasLocation ?location .
    ?location :isPartOf*   :Mindanao .

    OPTIONAL { ?event :eventName      ?eventName }
    OPTIONAL { ?res   :rescueUnit     ?unit      }
    OPTIONAL { ?res   :rescueEquipment ?equip    }
}`
  },
  {
    label: 'CQ18 — 4th income class municipalities in Region 9 with preemptive evacuation',
    query: P + `SELECT
    (CONCAT(?munName, ", ", ?provName) AS ?loc)
    (IF (MAX(IF (EXISTS { ?event :hasPreemptiveEvacuation ?pre . }, 1, 0)) > 0, "Yes", "No") AS ?didpreempt)
WHERE {
    ?event  a               :DisasterEvent ;
            :hasLocation    ?mun .

    ?mun    a                     :Municipality ;
            rdfs:label            ?munName ;
            :isPartOf*            :0900000000 ;
            :incomeClassification ?class ;
            :isPartOf             ?parent .

    ?parent a          :Province ;
            rdfs:label ?provName .

    FILTER(?class = "4th")
}
GROUP BY ?mun ?munName ?provName`
  },
  {
    label: 'CQ19 — Fire incidents: DROMIC vs. EM-DAT (CRED) counts',
    query: P + `SELECT ?targetOrgs (COUNT(?event) AS ?number)
WHERE {
    VALUES ?targetTypes { :FireMiscellaneous :FireIndustrial }
    VALUES ?targetOrgs  { org:DROMIC org:CRED }
    ?event  a                :DisasterEvent ;
            :hasDisasterType ?targetTypes ;
            prov:wasDerivedFrom+/prov:wasAttributedTo ?targetOrgs .
}
GROUP BY ?targetOrgs`
  },
  {
    label: 'CQ20 — Events reported in more than one source',
    query: P + `SELECT ?e1 ?eventName1 ?s1 ?d1 ?e2 ?eventName2 ?s2 ?d2
WHERE {
    ?e1 a :DisasterEvent .
    ?e2 a :DisasterEvent .
    ?e1 prov:alternateOf ?e2 .

    ?e1 prov:wasDerivedFrom+/prov:wasAttributedTo ?s1 .
    ?e2 prov:wasDerivedFrom+/prov:wasAttributedTo ?s2 .

    ?s1 a prov:Organization .
    ?s2 a prov:Organization .

    ?e1 :startDate ?d1 .
    ?e2 :startDate ?d2 .

    OPTIONAL { ?e1 :eventName ?eventName1 }
    OPTIONAL { ?e2 :eventName ?eventName2 }

    FILTER(STR(?e1) < STR(?e2))
}`
  }
];

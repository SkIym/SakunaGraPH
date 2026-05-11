"""
SakunaGraPH — CQ Runner
Runs all 20 competency questions against your local GraphDB instance
and saves the results as a readable HTML file.

Requirements: pip install requests
Usage:        python run_cqs.py
Output:       cq_results.html  (saved next to this script, opened in browser)
"""

import requests
import webbrowser
import os
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────
ENDPOINT = "http://localhost:7200/repositories/SakunaGraph"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "cq_results.html")
# ─────────────────────────────────────────────────────────────

PREFIXES = """
PREFIX : <https://sakuna.ph/>
PREFIX baw: <https://raw.githubusercontent.com/beAWARE-project/ontology/master/beAWARE_ontology#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
"""

# Property path covering all sub-entity predicates that carry :hasLocation in NDRRMC data.
# Used inside FILTER EXISTS blocks to check whether an event touches a given region.
_SUB = ("(:hasAffectedPopulation|:hasCasualties|:hasHousingDamage|:hasInfrastructureDamage"
        "|:hasAgricultureDamage|:hasAssistance|:hasDeclarationOfCalamity"
        "|:hasPreemptiveEvacuation|:hasRescue|:hasClassSuspension|:hasClassDisruption"
        "|:hasRoadAndBridgesDamage|:hasSeaportDisruption|:hasAirportDisruption"
        "|:hasFlightDisruption)")

# FILTER EXISTS block that returns true when ?event has ANY location in a given region.
# Variables use the ?_ prefix so they stay local to the EXISTS and never leak outward.
_EXISTS_REGION_TEMPLATE = """\
  FILTER EXISTS {{
    {{ ?event :hasLocation ?_loc }} UNION {{
      ?event {sub} ?_x .
      ?_x :hasLocation ?_loc .
    }}
    ?_loc :isPartOf* ?_reg . ?_reg a :Region ; rdfs:label ?_rn .
    FILTER ({region_filter})
  }}"""


def _exists(region_filter: str) -> str:
    return _EXISTS_REGION_TEMPLATE.format(sub=_SUB, region_filter=region_filter)


SMOKE_QUERIES = [
    ("Triple count",
     "SELECT (COUNT(*) AS ?total) WHERE { ?s ?p ?o }"),

    ("DisasterEvent count",
     "SELECT (COUNT(?e) AS ?count) WHERE { ?e a :DisasterEvent }"),

    # Fixed: navigate location via hasLocation + isPartOf* instead of broken :hasRegion
    ("Region labels in data",
     f"""SELECT DISTINCT ?regionLabel WHERE {{
  ?e a :DisasterEvent .
  {{ ?e :hasLocation ?loc }} UNION {{
    ?e {_SUB} ?x .
    ?x :hasLocation ?loc .
  }}
  ?loc :isPartOf* ?reg . ?reg a :Region ; rdfs:label ?regionLabel .
}} ORDER BY ?regionLabel"""),

    ("Casualty type values",
     "SELECT DISTINCT ?casualtyType WHERE { ?e a :DisasterEvent ; :hasCasualties ?c . ?c :casualtyType ?casualtyType }"),

    ("Disaster type labels",
     "SELECT DISTINCT ?typeLabel WHERE { ?e a :DisasterEvent ; :hasDisasterType ?t . ?t skos:prefLabel ?typeLabel } ORDER BY ?typeLabel LIMIT 30"),
]

CQS = [
    # ── Type Hierarchy ────────────────────────────────────────────────────────

    ("CQ1", "Type Hierarchy", "Tropical Cyclone events + related incidents",
     "Events classified as Tropical Cyclone and whether they have linked incidents.",
     # Fixed: used IF(BOUND(?incident),...) caused one row per related incident.
     # Now uses EXISTS so each event appears exactly once.
     """SELECT DISTINCT ?event ?eventName ?startDate
       (IF(EXISTS { ?event :hasRelatedIncident ?any },"Yes","No") AS ?hasRelatedIncident)
     WHERE {
       ?event a :DisasterEvent ; :hasDisasterType :TropicalCyclone .
       OPTIONAL { ?event :eventName ?eventName }
       OPTIONAL { ?event :startDate ?startDate }
     } ORDER BY DESC(?startDate) LIMIT 50"""),

    ("CQ2", "Type Hierarchy", "Flash Flood or Riverine Flood events by region",
     "Flood subtype events and the regions they affected.",
     # Fixed: :hasRegion is never asserted in data; navigate via :hasLocation + :isPartOf*.
     # OPTIONAL wrapper keeps events that have no location data at all.
     f"""SELECT DISTINCT ?eventName ?startDate ?regionLabel WHERE {{
  ?event a :DisasterEvent ; :hasDisasterType ?type .
  VALUES ?type {{ :FlashFlood :RiverineFlood }}
  OPTIONAL {{ ?event :eventName ?eventName }}
  OPTIONAL {{ ?event :startDate ?startDate }}
  OPTIONAL {{
    {{ ?event :hasLocation ?loc }} UNION {{
      ?event {_SUB} ?x .
      ?x :hasLocation ?loc .
    }}
    ?loc :isPartOf* ?reg . ?reg a :Region ; rdfs:label ?regionLabel .
  }}
}} ORDER BY ?regionLabel LIMIT 50"""),

    ("CQ3", "Type Hierarchy", "Mudslide hierarchy path with event counts",
     "Full ancestor chain of Mudslide up to Natural, with event count per level.",
     """SELECT ?ancestor ?ancestorLabel (COUNT(DISTINCT ?event) AS ?eventCount) WHERE {
  :Mudslide skos:broader* ?ancestor .
  OPTIONAL { ?ancestor skos:prefLabel ?ancestorLabel }
  OPTIONAL { ?dt skos:broader* ?ancestor .
             ?event a :DisasterEvent ; :hasDisasterType ?dt }
} GROUP BY ?ancestor ?ancestorLabel ORDER BY DESC(?eventCount)"""),

    ("CQ4", "Type Hierarchy", "Volcanic Activity subtypes in Region V or II",
     "Ashfall, Lahar, LavaFlow, PyroclasticFlow events in Bicol or Cagayan Valley.",
     # Fixed: :hasProvince never asserted; navigate location via :hasLocation + :isPartOf*.
     # Province derived transitively from location (municipality → province → region).
     f"""SELECT DISTINCT ?eventName ?startDate ?subtypeLabel ?provinceName ?regionLabel WHERE {{
  ?event a :DisasterEvent ; :hasDisasterType ?sub .
  ?sub skos:broader* :VolcanicActivity .
  OPTIONAL {{ ?event :eventName ?eventName }}
  OPTIONAL {{ ?event :startDate ?startDate }}
  OPTIONAL {{ ?sub skos:prefLabel ?subtypeLabel }}
  {{ ?event :hasLocation ?loc }} UNION {{
    ?event {_SUB} ?x .
    ?x :hasLocation ?loc .
  }}
  ?loc :isPartOf* ?reg . ?reg a :Region ; rdfs:label ?regionLabel .
  FILTER (CONTAINS(LCASE(STR(?regionLabel)),"region v")
       || CONTAINS(LCASE(STR(?regionLabel)),"bicol")
       || CONTAINS(LCASE(STR(?regionLabel)),"region ii")
       || CONTAINS(LCASE(STR(?regionLabel)),"cagayan valley"))
  OPTIONAL {{
    ?loc :isPartOf* ?prov . ?prov a :Province ; rdfs:label ?provinceName .
  }}
}} LIMIT 50"""),

    ("CQ5", "Type Hierarchy", "Fire / Landslide / Armed conflict in 2018, Central Luzon",
     "Count of these disaster categories in Central Luzon during 2018.",
     # Fixed: :hasRegion never asserted; use FILTER EXISTS with full location navigation.
     # COUNT(DISTINCT ?event) avoids double-counting from multiple location paths.
     f"""SELECT ?topCategory (COUNT(DISTINCT ?event) AS ?count) WHERE {{
  ?event a :DisasterEvent ; :hasDisasterType ?dt ; :startDate ?startDate .
  ?dt skos:broader* ?topCategory .
  VALUES ?topCategory {{ :FireMiscellaneous :FireIndustrial :Wildfire
                         :LandslideDry :LandslideWet :ArmedConflict }}
  FILTER (YEAR(?startDate) = 2018)
{_exists("CONTAINS(LCASE(STR(?_rn)),\"central luzon\") || CONTAINS(LCASE(STR(?_rn)),\"region iii\")")}
}} GROUP BY ?topCategory ORDER BY DESC(?count)"""),

    # ── Casualties & Population ───────────────────────────────────────────────

    ("CQ6", "Casualties & Population", "Displaced families and persons in Region VIII",
     "Total displaced for all events in Eastern Visayas.",
     # Fixed: :hasRegion never asserted; FILTER EXISTS checks region membership without
     # binding extra variables, then OPTIONAL gathers population data per event.
     f"""SELECT ?eventName ?startDate
       (SUM(?fam) AS ?totalFamilies) (SUM(?per) AS ?totalPersons) WHERE {{
  ?event a :DisasterEvent .
  OPTIONAL {{ ?event :eventName ?eventName }}
  OPTIONAL {{ ?event :startDate ?startDate }}
{_exists("CONTAINS(LCASE(STR(?_rn)),\"region viii\") || CONTAINS(LCASE(STR(?_rn)),\"eastern visayas\")")}
  OPTIONAL {{
    ?event :hasAffectedPopulation ?ap .
    OPTIONAL {{ ?ap :displacedFamilies ?fam }}
    OPTIONAL {{ ?ap :displacedPersons ?per }}
  }}
}} GROUP BY ?eventName ?startDate ORDER BY DESC(?totalPersons) LIMIT 30"""),

    ("CQ7", "Casualties & Population", "Earthquake casualties by province in CAR",
     "Dead, missing, and injured counts for Earthquake events in the Cordillera.",
     # Fixed: :hasProvince never asserted. Location comes from casualty sub-entity
     # (NDRRMC) or from the event itself when the casualty has no location (GDA/EMDAT).
     # skos:broader* on Earthquake catches any defined sub-types.
     f"""SELECT ?eventName ?provinceName ?casualtyType (SUM(?count) AS ?total) WHERE {{
  ?event a :DisasterEvent ; :hasDisasterType ?type .
  ?type skos:broader* :Earthquake .
  OPTIONAL {{ ?event :eventName ?eventName }}
  ?event :hasCasualties ?c .
  ?c :casualtyType ?casualtyType ; :casualtyCount ?count .
  {{
    ?c :hasLocation ?loc .
  }} UNION {{
    FILTER NOT EXISTS {{ ?c :hasLocation ?_any }}
    ?event :hasLocation ?loc .
  }}
  ?loc :isPartOf* ?prov . ?prov a :Province ; rdfs:label ?provinceName .
  ?prov :isPartOf+ ?reg . ?reg a :Region ; rdfs:label ?rn .
  FILTER (CONTAINS(LCASE(STR(?rn)),"cordillera")
       || CONTAINS(LCASE(STR(?rn)),"car"))
}} GROUP BY ?eventName ?provinceName ?casualtyType ORDER BY ?provinceName LIMIT 50"""),

    ("CQ8", "Casualties & Population", "Preemptive evacuation for Typhoons in Region IV-A",
     "Total persons and families preemptively evacuated in CALABARZON.",
     # Fixed: :hasRegion never asserted; FILTER EXISTS checks region, then OPTIONAL
     # collects evacuation figures (NDRRMC data uses evacuationCenters, not preemptPersons).
     f"""SELECT ?eventName ?startDate
       (SUM(?per) AS ?totalPersons) (SUM(?fam) AS ?totalFamilies) WHERE {{
  ?event a :DisasterEvent ; :hasDisasterType :TropicalCyclone .
  OPTIONAL {{ ?event :eventName ?eventName }}
  OPTIONAL {{ ?event :startDate ?startDate }}
{_exists("CONTAINS(LCASE(STR(?_rn)),\"iv-a\") || CONTAINS(LCASE(STR(?_rn)),\"calabarzon\")")}
  OPTIONAL {{
    ?event :hasPreemptiveEvacuation ?pe .
    OPTIONAL {{ ?pe :preemptPersons ?per }}
    OPTIONAL {{ ?pe :preemptFamilies ?fam }}
  }}
}} GROUP BY ?eventName ?startDate ORDER BY DESC(?totalPersons) LIMIT 30"""),

    # ── Damage ────────────────────────────────────────────────────────────────

    ("CQ9", "Damage", "Infrastructure damage for Flash Flood in Region XIII",
     "Infrastructure and road/bridge damage records for Flash Flood events in Caraga.",
     # Fixed: :hasRegion never asserted; FILTER EXISTS checks region membership.
     f"""SELECT DISTINCT ?eventName ?startDate ?infraName ?infraDamageType
               ?roadBridgeName ?roadBridgeStatus WHERE {{
  ?event a :DisasterEvent ; :hasDisasterType :FlashFlood .
  OPTIONAL {{ ?event :eventName ?eventName }}
  OPTIONAL {{ ?event :startDate ?startDate }}
{_exists("CONTAINS(LCASE(STR(?_rn)),\"region xiii\") || CONTAINS(LCASE(STR(?_rn)),\"caraga\")")}
  OPTIONAL {{ ?event :hasInfrastructureDamage ?id .
    OPTIONAL {{ ?id :infraName ?infraName }}
    OPTIONAL {{ ?id :infraDamageType ?infraDamageType }} }}
  OPTIONAL {{ ?event :hasRoadAndBridgesDamage ?rd .
    OPTIONAL {{ ?rd :roadBridgeName ?roadBridgeName }}
    OPTIONAL {{ ?rd :roadBridgeStatus ?roadBridgeStatus }} }}
}} LIMIT 30"""),

    ("CQ10", "Damage", "Agriculture damage for Typhoons in Region VI",
     "Crop type, production loss volume and cost for Tropical Cyclone events in Western Visayas.",
     # Fixed: :hasRegion never asserted; FILTER EXISTS checks region membership.
     f"""SELECT ?eventName ?startDate ?agriDamageType
       (SUM(?vol) AS ?totalVolume) (SUM(?cost) AS ?totalCost) WHERE {{
  ?event a :DisasterEvent ; :hasDisasterType :TropicalCyclone .
  OPTIONAL {{ ?event :eventName ?eventName }}
  OPTIONAL {{ ?event :startDate ?startDate }}
{_exists("CONTAINS(LCASE(STR(?_rn)),\"region vi\") || CONTAINS(LCASE(STR(?_rn)),\"western visayas\")")}
  ?event :hasAgricultureDamage ?ad .
  OPTIONAL {{ ?ad :agriDamageType ?agriDamageType }}
  OPTIONAL {{ ?ad :productionLossVolume ?vol }}
  OPTIONAL {{ ?ad :productionLossCost ?cost }}
}} GROUP BY ?eventName ?startDate ?agriDamageType ORDER BY DESC(?totalCost) LIMIT 30"""),

    ("CQ11", "Damage", "Storm Surge housing damage in Region VIII",
     "Totally vs. partially damaged houses and cost for Storm Surge events.",
     # Fixed: :hasProvince never asserted. Location comes from housing-damage sub-entity
     # (NDRRMC) or from the event directly (GDA/EMDAT). Province derived transitively.
     f"""SELECT ?eventName ?startDate ?provinceName
       (SUM(?tot) AS ?totallyDamaged) (SUM(?par) AS ?partiallyDamaged)
       (SUM(?cost) AS ?housingCost) WHERE {{
  ?event a :DisasterEvent ; :hasDisasterType ?type .
  ?type skos:broader* :StormSurge .
  OPTIONAL {{ ?event :eventName ?eventName }}
  OPTIONAL {{ ?event :startDate ?startDate }}
  ?event :hasHousingDamage ?hd .
  {{
    ?hd :hasLocation ?loc .
  }} UNION {{
    FILTER NOT EXISTS {{ ?hd :hasLocation ?_any }}
    ?event :hasLocation ?loc .
  }}
  ?loc :isPartOf* ?reg . ?reg a :Region ; rdfs:label ?rn .
  FILTER (CONTAINS(LCASE(STR(?rn)),"region viii")
       || CONTAINS(LCASE(STR(?rn)),"eastern visayas"))
  OPTIONAL {{
    ?loc :isPartOf* ?prov . ?prov a :Province ; rdfs:label ?provinceName .
  }}
  OPTIONAL {{ ?hd :totallyDamagedHouses ?tot }}
  OPTIONAL {{ ?hd :partiallyDamagedHouses ?par }}
  OPTIONAL {{ ?hd :housingDamageAmount ?cost }}
}} GROUP BY ?eventName ?startDate ?provinceName ORDER BY DESC(?totallyDamaged) LIMIT 30"""),

    ("CQ12", "Damage", "Events with both agriculture AND infrastructure damage",
     "Events that caused both types of damage simultaneously.",
     """SELECT DISTINCT ?eventName ?startDate ?typeLabel WHERE {
  ?event a :DisasterEvent ; :hasAgricultureDamage ?ad ; :hasInfrastructureDamage ?id .
  OPTIONAL { ?event :eventName ?eventName }
  OPTIONAL { ?event :startDate ?startDate }
  OPTIONAL { ?event :hasDisasterType ?t . ?t skos:prefLabel ?typeLabel }
} ORDER BY DESC(?startDate) LIMIT 30"""),

    # ── Service Disruptions ───────────────────────────────────────────────────

    ("CQ13", "Service Disruptions", "Seaport disruptions from Tropical Cyclone events",
     "Port names and statuses for seaport disruptions linked to typhoons.",
     """SELECT ?eventName ?portOrTerminalName ?portStatus ?cancellationDateTime WHERE {
  ?event a :DisasterEvent ; :hasDisasterType :TropicalCyclone ;
         :hasSeaportDisruption ?sd .
  OPTIONAL { ?event :eventName ?eventName }
  OPTIONAL { ?sd :portOrTerminalName ?portOrTerminalName }
  OPTIONAL { ?sd :portStatus ?portStatus }
  OPTIONAL { ?sd :cancellationDateTime ?cancellationDateTime }
} LIMIT 30"""),

    ("CQ14", "Service Disruptions", "Airport/flight disruptions for Earthquake or Volcanic events",
     "Airport or flight disruption records for Earthquake and VolcanicActivity events.",
     # Fixed: NDRRMC data uses :hasFlightDisruption; ontology declares :hasAirportDisruption.
     # UNION covers both predicates so no events are missed.
     """SELECT ?eventName ?typeLabel ?portOrTerminalName
       ?affectedDescription ?cancellationDateTime ?resumptionDateTime WHERE {
  ?event a :DisasterEvent ; :hasDisasterType ?type .
  ?type skos:broader* ?top .
  VALUES ?top { :Earthquake :VolcanicActivity }
  { ?event :hasAirportDisruption ?ad } UNION { ?event :hasFlightDisruption ?ad }
  OPTIONAL { ?event :eventName ?eventName }
  OPTIONAL { ?type skos:prefLabel ?typeLabel }
  OPTIONAL { ?ad :portOrTerminalName ?portOrTerminalName }
  OPTIONAL { ?ad :affectedDescription ?affectedDescription }
  OPTIONAL { ?ad :cancellationDateTime ?cancellationDateTime }
  OPTIONAL { ?ad :resumptionDateTime ?resumptionDateTime }
} LIMIT 30"""),

    ("CQ15", "Service Disruptions", "Class suspensions in NCR",
     "Events causing class suspensions in Metro Manila and at what grade levels.",
     # Fixed: ontology defines :hasClassDisruption but NDRRMC data uses :hasClassSuspension.
     # UNION covers both. :hasRegion replaced by navigating suspension's own :hasLocation.
     """SELECT ?eventName ?startDate ?fromClassLevel ?toClassLevel
       ?cancellationDateTime ?resumptionDateTime WHERE {
  ?event a :DisasterEvent .
  { ?event :hasClassDisruption ?cs } UNION { ?event :hasClassSuspension ?cs }
  OPTIONAL { ?event :eventName ?eventName }
  OPTIONAL { ?event :startDate ?startDate }
  ?cs :hasLocation ?loc .
  ?loc :isPartOf* ?reg . ?reg a :Region ; rdfs:label ?rn .
  FILTER (CONTAINS(LCASE(STR(?rn)),"national capital")
       || CONTAINS(LCASE(STR(?rn)),"ncr")
       || CONTAINS(LCASE(STR(?rn)),"metro manila"))
  OPTIONAL { ?cs :fromClassLevel ?fromClassLevel }
  OPTIONAL { ?cs :toClassLevel ?toClassLevel }
  OPTIONAL { ?cs :cancellationDateTime ?cancellationDateTime }
  OPTIONAL { ?cs :resumptionDateTime ?resumptionDateTime }
} ORDER BY DESC(?startDate) LIMIT 30"""),

    # ── Response ──────────────────────────────────────────────────────────────

    ("CQ16", "Response", "Assistance in Region V (Bicol)",
     "Assistance records broken down by type and contributing organization.",
     # Fixed: :hasRegion never asserted. Location taken from assistance sub-entity
     # (NDRRMC) or from event directly when sub-entity has no location (GDA/EMDAT).
     f"""SELECT ?eventName ?startDate ?contributionType
       ?contributingOrg ?amountDSWD ?amountLGU ?allocatedFunds WHERE {{
  ?event a :DisasterEvent ; :hasAssistance ?a .
  OPTIONAL {{ ?event :eventName ?eventName }}
  OPTIONAL {{ ?event :startDate ?startDate }}
  {{
    ?a :hasLocation ?loc .
  }} UNION {{
    FILTER NOT EXISTS {{ ?a :hasLocation ?_any }}
    ?event :hasLocation ?loc .
  }}
  ?loc :isPartOf* ?reg . ?reg a :Region ; rdfs:label ?rn .
  FILTER (CONTAINS(LCASE(STR(?rn)),"region v")
       || CONTAINS(LCASE(STR(?rn)),"bicol"))
  OPTIONAL {{ ?a :contributionType ?contributionType }}
  OPTIONAL {{ ?a :contributingOrg ?contributingOrg }}
  OPTIONAL {{ ?a :amountDSWD ?amountDSWD }}
  OPTIONAL {{ ?a :amountLGU ?amountLGU }}
  OPTIONAL {{ ?a :allocatedFunds ?allocatedFunds }}
}} LIMIT 30"""),

    ("CQ17", "Response", "Declarations of Calamity in Region XII",
     "Declaration type and resolution date for events in SOCCSKSARGEN provinces.",
     # Fixed: :hasProvince never asserted. Location taken from declaration sub-entity
     # (NDRRMC) or from event directly (GDA/EMDAT). Province derived transitively.
     f"""SELECT ?eventName ?startDate ?provinceName
       ?declarationType ?resolutionNo ?resolutionDate WHERE {{
  ?event a :DisasterEvent ; :hasDeclarationOfCalamity ?d .
  OPTIONAL {{ ?event :eventName ?eventName }}
  OPTIONAL {{ ?event :startDate ?startDate }}
  {{
    ?d :hasLocation ?loc .
  }} UNION {{
    FILTER NOT EXISTS {{ ?d :hasLocation ?_any }}
    ?event :hasLocation ?loc .
  }}
  ?loc :isPartOf* ?reg . ?reg a :Region ; rdfs:label ?rn .
  FILTER (CONTAINS(LCASE(STR(?rn)),"region xii")
       || CONTAINS(LCASE(STR(?rn)),"soccsksargen"))
  OPTIONAL {{
    ?loc :isPartOf* ?prov . ?prov a :Province ; rdfs:label ?provinceName .
  }}
  OPTIONAL {{ ?d :declarationType ?declarationType }}
  OPTIONAL {{ ?d :resolutionNo ?resolutionNo }}
  OPTIONAL {{ ?d :resolutionDate ?resolutionDate }}
}} LIMIT 30"""),

    ("CQ18", "Response", "Rescue ops for Landslide/Mudslide in CAR",
     "Rescue units and equipment for landslide events in the Cordillera.",
     # Fixed: :hasRegion never asserted; FILTER EXISTS checks region via any location path.
     # GDA rescue sub-entities have no :hasLocation, so event's direct location is used.
     f"""SELECT ?eventName ?typeLabel ?rescueUnit ?rescueEquipment WHERE {{
  ?event a :DisasterEvent ; :hasDisasterType ?type .
  ?type skos:broader* ?top .
  VALUES ?top {{ :LandslideWet :Mudslide }}
  OPTIONAL {{ ?event :eventName ?eventName }}
  OPTIONAL {{ ?type skos:prefLabel ?typeLabel }}
{_exists("CONTAINS(LCASE(STR(?_rn)),\"cordillera\") || CONTAINS(LCASE(STR(?_rn)),\"car\")")}
  ?event :hasRescue ?r .
  OPTIONAL {{ ?r :rescueUnit ?rescueUnit }}
  OPTIONAL {{ ?r :rescueEquipment ?rescueEquipment }}
}} LIMIT 30"""),

    # ── Temporal Analysis ─────────────────────────────────────────────────────

    ("CQ19", "Temporal Analysis", "Decade-by-decade Typhoon frequency in Region VIII",
     "Typhoon count and total DEAD casualties per decade for Eastern Visayas events.",
     # Fixed: :hasRegion never asserted; FILTER EXISTS checks region membership.
     # COUNT(DISTINCT ?event) avoids double-counting from multiple OPTIONAL casualty rows.
     f"""SELECT ?decade (COUNT(DISTINCT ?event) AS ?eventCount) (SUM(?count) AS ?totalDead) WHERE {{
  ?event a :DisasterEvent ; :hasDisasterType :TropicalCyclone ; :startDate ?startDate .
{_exists("CONTAINS(LCASE(STR(?_rn)),\"region viii\") || CONTAINS(LCASE(STR(?_rn)),\"eastern visayas\")")}
  BIND (FLOOR(YEAR(?startDate) / 10) * 10 AS ?decade)
  OPTIONAL {{
    ?event :hasCasualties ?c .
    ?c :casualtyType ?ct ; :casualtyCount ?count .
    FILTER (LCASE(STR(?ct)) = "dead")
  }}
}} GROUP BY ?decade ORDER BY ?decade"""),

    # ── Provenance ────────────────────────────────────────────────────────────

    ("CQ20", "Provenance", "DGeog-sourced events (provenance check)",
     "All events sourced from the GDA archive, with source reference citations and casualty figures.",
     # Fixed: original query used GROUP BY + SUM on optional variables, causing 0 rows.
     # Switched to prov:wasDerivedFrom :GDA (present on every GDA event) + DISTINCT,
     # with :reference and casualty data as optional details.
     """SELECT DISTINCT ?eventName ?startDate ?reference ?casualtyType ?casualtyCount WHERE {
  ?event a :DisasterEvent ; prov:wasDerivedFrom :GDA .
  OPTIONAL { ?event :eventName ?eventName }
  OPTIONAL { ?event :startDate ?startDate }
  OPTIONAL { ?event :reference ?reference }
  OPTIONAL {
    ?event :hasCasualties ?c .
    ?c :casualtyType ?casualtyType .
    OPTIONAL { ?c :casualtyCount ?casualtyCount }
  }
} ORDER BY ?startDate LIMIT 30"""),
]


def run_query(query):
    """Run a SPARQL query, return (cols, rows, error)."""
    try:
        r = requests.post(
            ENDPOINT,
            data=(PREFIXES + query).encode("utf-8"),
            headers={
                "Content-Type": "application/sparql-query; charset=utf-8",
                "Accept": "application/sparql-results+json",
            },
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        return data["head"]["vars"], data["results"]["bindings"], None
    except Exception as e:
        return [], [], str(e)


def shorten(uri):
    import re
    m = re.search(r"[#/]([^#/]+)$", uri)
    return m.group(1) if m else uri


def cell_val(binding):
    if not binding:
        return ""
    val = binding.get("value", "")
    if binding.get("type") == "uri":
        val = shorten(val)
    return (val[:80] + "…") if len(val) > 80 else val


def make_table(cols, rows):
    if not rows:
        return "<p class='empty'>No rows returned.</p>"
    html = "<div class='tbl-wrap'><table><thead><tr>"
    for c in cols:
        html += f"<th>{c}</th>"
    html += "</tr></thead><tbody>"
    for row in rows[:100]:
        html += "<tr>"
        for c in cols:
            v = row.get(c, {})
            html += f"<td title='{v.get('value','')}'>{cell_val(v)}</td>"
        html += "</tr>"
    html += "</tbody></table></div>"
    count = len(rows)
    note = f"Showing 100 of {count} rows." if count > 100 else f"{count} row{'s' if count != 1 else ''} returned."
    html += f"<p class='rownote'>{note}</p>"
    return html


def build_html(smoke_results, cq_results, run_time):
    ok    = sum(1 for item in cq_results if not item[6] and item[5])
    empty = sum(1 for item in cq_results if not item[6] and not item[5])
    errors = sum(1 for item in cq_results if item[6])

    cats = list(dict.fromkeys(item[1] for item in cq_results))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>SakunaGraPH — CQ Results</title>
<style>
body{{font-family:system-ui,sans-serif;font-size:14px;color:#111;max-width:1100px;margin:0 auto;padding:1.5rem 1rem;background:#f9f9f7}}
h1{{font-size:22px;font-weight:600;margin-bottom:4px}}
.meta{{font-size:12px;color:#666;margin-bottom:1.5rem}}
.summary{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:1.5rem}}
.scard{{background:#fff;border:1px solid #e5e5e5;border-radius:8px;padding:12px;text-align:center}}
.scard .val{{font-size:28px;font-weight:600}}
.scard .lbl{{font-size:11px;color:#888;margin-top:2px}}
.ok .val{{color:#0F6E56}}.warn .val{{color:#BA7517}}.err .val{{color:#D85A30}}
.smoke{{background:#fff;border:1px solid #e5e5e5;border-radius:8px;padding:1rem;margin-bottom:1.5rem}}
.smoke h2{{font-size:15px;font-weight:600;margin-bottom:0.75rem}}
.stitle{{font-size:12px;font-weight:600;margin:10px 0 4px}}
.cat{{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#888;margin:1.5rem 0 .5rem;padding-bottom:4px;border-bottom:1px solid #e5e5e5}}
.cq{{background:#fff;border:1px solid #e5e5e5;border-radius:8px;margin-bottom:8px;overflow:hidden}}
.cq-hdr{{display:flex;align-items:center;gap:8px;padding:10px 14px;background:#fafafa;border-bottom:1px solid #f0f0f0}}
.cq-id{{font-size:12px;font-weight:600;color:#666;min-width:38px}}
.badge{{font-size:11px;font-weight:500;padding:2px 8px;border-radius:10px}}
.b-ok{{background:#E1F5EE;color:#0F6E56}}
.b-empty{{background:#FAEEDA;color:#854F0B}}
.b-error{{background:#FAECE7;color:#993C1D}}
.cq-title{{font-size:13px;font-weight:600;flex:1}}
.cq-rows{{font-size:11px;color:#999}}
.cq-body{{padding:12px 14px}}
.cq-desc{{font-size:12px;color:#666;margin-bottom:8px}}
.err-box{{font-size:12px;color:#D85A30;background:#FAECE7;padding:8px;border-radius:6px;margin-bottom:8px;word-break:break-all}}
.tbl-wrap{{overflow-x:auto;max-height:280px;overflow-y:auto;border:1px solid #e5e5e5;border-radius:6px}}
table{{width:100%;font-size:12px;border-collapse:collapse}}
th{{background:#f5f5f5;padding:5px 8px;text-align:left;font-weight:600;position:sticky;top:0;border-bottom:1px solid #e5e5e5;white-space:nowrap}}
td{{padding:4px 8px;border-bottom:1px solid #f0f0f0;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:#fafafa}}
.empty{{font-size:12px;color:#999;font-style:italic;margin-top:4px}}
.rownote{{font-size:11px;color:#999;margin-top:4px}}
</style>
</head>
<body>
<h1>SakunaGraPH — Competency Question Results</h1>
<p class="meta">Endpoint: {ENDPOINT} &nbsp;|&nbsp; Run at: {run_time}</p>

<div class="summary">
  <div class="scard ok"><div class="val">{ok}</div><div class="lbl">with results</div></div>
  <div class="scard warn"><div class="val">{empty}</div><div class="lbl">0 rows</div></div>
  <div class="scard err"><div class="val">{errors}</div><div class="lbl">errors</div></div>
  <div class="scard"><div class="val">20</div><div class="lbl">total CQs</div></div>
</div>

<div class="smoke"><h2>Smoke Tests</h2>"""

    for title, cols, rows, err in smoke_results:
        html += f"<p class='stitle'>{title}</p>"
        if err:
            html += f"<div class='err-box'>{err}</div>"
        else:
            html += make_table(cols, rows)
    html += "</div>"

    for cat in cats:
        html += f"<div class='cat'>{cat}</div>"
        for item in cq_results:
            cid, c, title, desc, cols, rows, err = item
            if c != cat:
                continue
            if err:
                badge, blbl = "b-error", "error"
            elif rows:
                badge, blbl = "b-ok", "results"
            else:
                badge, blbl = "b-empty", "0 rows"
            rowcount = f"{len(rows)} rows" if rows else ""
            html += f"""<div class="cq">
  <div class="cq-hdr">
    <span class="cq-id">{cid}</span>
    <span class="badge {badge}">{blbl}</span>
    <span class="cq-title">{title}</span>
    <span class="cq-rows">{rowcount}</span>
  </div>
  <div class="cq-body">
    <p class="cq-desc">{desc}</p>
    {"<div class='err-box'>"+err+"</div>" if err else ""}
    {make_table(cols, rows)}
  </div>
</div>"""

    html += "</body></html>"
    return html


def main():
    print("=" * 60)
    print("  SakunaGraPH — CQ Runner")
    print(f"  Endpoint: {ENDPOINT}")
    print("=" * 60)

    print("\n[Smoke tests]")
    smoke_results = []
    for title, q in SMOKE_QUERIES:
        print(f"  {title}... ", end="", flush=True)
        cols, rows, err = run_query(q)
        if err:
            print(f"ERROR: {err}")
        else:
            first = list(rows[0].values())[0].get("value", "") if rows else "—"
            print(f"OK  ({first})")
        smoke_results.append((title, cols, rows, err))

    print("\n[Competency Questions]")
    cq_results = []
    for cid, cat, title, desc, q in CQS:
        print(f"  {cid} — {title[:50]}... ", end="", flush=True)
        cols, rows, err = run_query(q)
        if err:
            print("ERROR")
        elif rows:
            print(f"{len(rows)} rows")
        else:
            print("0 rows")
        cq_results.append((cid, cat, title, desc, cols, rows, err))

    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html = build_html(smoke_results, cq_results, run_time)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n{'='*60}")
    print(f"  Results saved to: {OUTPUT_FILE}")
    print(f"  Opening in browser...")
    print(f"{'='*60}")
    webbrowser.open(f"file:///{OUTPUT_FILE}")


if __name__ == "__main__":
    main()

**BASIC PIPELINE**

***Extraction***
- Generate csv by extracting relevant info from the source
- Archive from Geography: XLSX -> CSV with renamed columns based on ontology 
    - To run: `python ./mappers/geog_archive_mapper.py`
- PSGC Shapefiles: SHP -> TTL via a mapper
    - To run: `python ./mappers/psgc_datefile_mapper.py`

***Transformation***
- mappings dir contains the rml mappings for each source
- udfs dir contains the user-defined functions needed for each source
- basically adheres to the ontology classes and relationships
- To run: `python -m morph_kgc ./config.ini`

***Loading***
- load ttl files from transformation stage to graphDB. This hasn't been automated yet.



**ABOUT DATA**

***GEOG ARCHIVE***
TO do:
- Also map announcements released

DONE:
- Date / Time
- Disaster Type / Subtype
- Location
- Location Granularity (while transforming to RDF)

***PSGC***

To do:

- Fix isPartOf relations with Cotabato clusters
- just link to the external geoJSON URI since WKT literals bloat the data

Considerations:
- Mapping done down to municipal level only
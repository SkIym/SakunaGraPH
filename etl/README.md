**BASIC PIPELINE**

***Extraction***
- Generate csv by extracting relevant info from the source
- Archive from Geography: XLSX -> CSV with renamed columns based on ontology 
    - To run: `python ./mappers/geog_archive_mapper.py`
- PSGC Shapefiles: SHP -> TTL via a mapper
    - To run: `python ./mappers/psgc_datefile_mapper.py`
- NDRRMC Reports: PDF ->  CSV 
    - Download from the drive or run `python ./parsers/ndrrmc_parser.py`
    - Make sure the csv and json files are in etl/data/ndrrmc

***Transformation***
- mappings dir contains the rml mappings for each source
- udfs dir contains the user-defined functions needed for each source
- basically adheres to the ontology classes and relationships
- GDA: `python -m morph_kgc ./config.ini`
- NDRRMC: `python -m ppl.run_ndrrmc`

***Loading***
- load ttl files from transformation stage to graphDB. This hasn't been automated yet.



**ABOUT DATA**

***GEOG ARCHIVE***

DONE:
- Date / Time
- Disaster Type / Subtype
- Location
- Location Granularity (while transforming to RDF)

***PSGC***

To do:

- Way to link the external geoJSON URI to ??? 

Considerations:
- Mapping done down to municipal level only
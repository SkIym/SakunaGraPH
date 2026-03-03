**BASIC PIPELINE**

***Extraction***
- Generate csv by extracting relevant info from the source
- Archive from Geography: XLSX -> CSV with renamed columns based on ontology 
    - To run: `python ./mappers/geog_archive_mapper.py`
- PSGC Shapefiles: SHP -> TTL via a mapper
    - To run: `python ./mappers/psgc_datefile_mapper.py`
- NDRRMC Reports: PDF ->  CSV 
    - Download from the drive or run `python ./parse/ndrrmc-v311.py`
    - Make sure the csv and json files are in etl/data/ndrrmc

***Transformation***
- mappings dir contains the rml mappings for each source
- udfs dir contains the user-defined functions needed for each source
- basically adheres to the ontology classes and relationships
- GDA: `python -m morph_kgc ./config.ini`
- NDRRMC: `python -m pipeline.run_ndrrmc`

***Loading***
- load ttl files from transformation stage to graphDB. This hasn't been automated yet.



**ABOUT DATA**

***NDRRMC***

To do:
- Implement OCR for image-based / scanned PDFs
- Manually correct hasType values for major events (the event corresponding to report)
- Fix casualty type forward filling (move erratic location values and filter by casualty type only)

***GEOG ARCHIVE***

DONE:
- Date / Time
- Disaster Type / Subtype
- Location
- Location Granularity (while transforming to RDF)

***PSGC***

To do:

- Use datafile instead of shapefile

Considerations:
- Mapping done down to municipal level only
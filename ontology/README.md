- Latest: sakunagraph

TO DO:
- use SKOS concept scheme for EM-dat clasification and the major/incident split in disaster events
- convert string-typed categorical data properties to object properites poiting to skos concepts (castualtyType, damgclassifcaiton, dmgtype, AGENCY! (dswd, ocd, etc.))
- Remove owl:someValuesFrom restrictions (subclass of data prop restricis in protege), clutters the file :<
- hasType/hasSubtype to hasDisasterType, and add hasEventRole that has Major or Incident range (refer to first bullet point)
- remove Brangay as a class, rely on hasBarangay data prop then reason out on the paper.

DONE:
- rename IRIs
- cleanup ontology pitfalls (minor pitfalls are disregarded due to design considerations, only important and critical addressed)
    - refactor or remove properties without domain or range 
    - add disjointnenss between classes / properties 
        - Natural and Technological Disasters 
        - Major Events and Incidents
    - fix multiple domain issue and make it union
    - refactor inverse property relationships 
- find existing disaster domain ontology to extend using current
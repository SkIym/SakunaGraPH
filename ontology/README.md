- Latest: sakunagraph

TO DO:
- determine ways to store multiple paired values and attributes (itemCost, itemQty, )
- align with PROV-O

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
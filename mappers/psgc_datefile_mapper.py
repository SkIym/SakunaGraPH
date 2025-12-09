import geopandas as gpd
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, GEO


regions_shp_path = "../shapefiles/PH_Adm1_Regions.shp"
provinces_shp_path = "../shapefiles/PH_Adm2_ProvDists.shp"
municities_shp_path = "../shapefiles/PH_Adm3_MuniCities.shp"

gdf_regions = gpd.read_file(regions_shp_path, layer='PH_Adm1_Regions.shp')
# gdf_provinces = gpd.read_file(provinces_shp_path, layer='PH_Adm2_ProvDists.shp')
# gdf_municities = gpd.read_file(municities_shp_path, layer='PH_Adm3_MuniCities.shp')


g = Graph()
SKG = Namespace("https://sakuna.ph/")

g.bind("", SKG)


# print(gpd.list_layers(regions_shp_path))

# ===================== REGIONS

for _, row in gdf_regions.iterrows():

    # print(row)
    # print("-----------------")

    uri = URIRef(SKG[row['adm1_en'].replace(" ", "_")]) # Location URI
    psgc = row['adm1_psgc']
    admLevel = "Region"

    g.add((uri, RDF.type, SKG["Region"]))
    g.add((uri, RDFS.label, Literal(row['adm1_en'])))
    g.add((uri, URIRef(SKG["psgc"]), Literal(psgc)))
    g.add((uri, URIRef(SKG["admLevel"]), Literal(admLevel)))


    geom_wkt = row['geometry'].wkt
    geom_uri = URIRef(SKG[row['adm1_en'].replace(" ", "_")] + "_geom")

    g.add((geom_uri, RDF.type, GEO.Geometry))
    g.add((geom_uri, GEO.asWKT, geom_wkt))

    g.add((uri, GEO.hasGeometry, geom_uri))


g.serialize(destination='regions.ttl')


# ===================== PROVINCES

# for _, row in gdf_provinces.iterrows():

#     print(row)

#     uri = URIRef(SKG[row['adm2_en'].replace(" ", "_")]) # Location URI
#     psgc = row['adm2_psgc']
#     admLevel = "Province"
#     geom_wkt = row['geometry'].wkt

#     print("-----------------")


# ===================== MUNICIPALITIES/CITIES

# for _, row in gdf_municities.iterrows():

#     print(row)

#     uri = URIRef(SKG[row['adm3_en'].replace(" ", "_")]) # Location URI
#     psgc = row['adm3_psgc']
#     admLevel = "Municipality" if row['geo_level'] == 'Mun' else "City"
#     geom_wkt = row['geometry'].wkt

#     print("-----------------")
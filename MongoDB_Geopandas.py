# coding: cp1252
'''
created: 20.05.2021
'''

#Imports
import pymongo
from pymongo import GEO2D
import pandas as pd
import numpy as np
import geopandas as gpd

#Connect to DB
import dns.resolver
dns.resolver.default_resolver=dns.resolver.Resolver(configure=False)
dns.resolver.default_resolver.nameservers=['8.8.8.8']

client = pymongo.MongoClient("Mongo_DB_Cluster")
db = client.test

#Define DB and Collections
mydb = client["Geodatenbanken"]
mycol = mydb["citybikejson"]

radnetz_col = mydb["radnetz"]

bezirksgrenzen_col = mydb["bezirksgrenzen"]
bezirksgrenzen_data = list(bezirksgrenzen_col.find({}))

abstellanlagen_col = mydb["fahrradabstellanlagen"]

radindex = mydb['radindex'] #wird erst befüllt

#Index first Collection
mycol["geometry"].create_index([("coordinates", GEO2D)])

#Print elements of Collection
for x in mycol.find():
  print(x)

for x in mycol.find({},{ "_id": 0, "name": 1, "address": 1 }):
  print(x)

#Group elements - Citybikestationen in Wien
pipeline = [
  {"$group": {"_id" : "$properties.BEZIRK", #group by BEZIRK
              "stationen_pro_bezirk": {"$sum" : 1}}} #count
]

citybike_json_bezirk = list(mycol.aggregate(pipeline))

#Make Pandas Dataframe
citybike_json_bezirk = pd.DataFrame(citybike_json_bezirk)
citybike_bezirk = citybike_json_bezirk.sort_values('_id')
citybike_bezirk.set_index('_id')
for x in range(1,24):
    if x not in list(citybike_bezirk['_id']):
        citybike_bezirk = \
            citybike_bezirk.append({'_id': x, 'stationen_pro_bezirk' : 0},
                               ignore_index=True)

citybike_bezirk = citybike_bezirk.set_index('_id')
print(citybike_bezirk)

#Radnetz pro Bezirk
radnetz_data = list(radnetz_col.find({}))
radnetz_dataframe = gpd.GeoDataFrame.from_features(radnetz_data)

bezirksgrenzen_dataframe = gpd.GeoDataFrame.from_features(bezirksgrenzen_data)
bezirksgrenzen_col["geometry"].create_index([("coordinates", GEO2D)])

#Intersection - welche Linien vom Radnetz in welchem Bezirk?
intersection = gpd.sjoin(radnetz_dataframe,bezirksgrenzen_dataframe, op='intersects')
bezirks_linien = np.zeros(shape=(23))
intersection['length'] = intersection.geometry.length

beznr_index = intersection.columns.get_loc("BEZNR")
length_index = intersection.columns.get_loc('length')

for i,line in intersection.iterrows():
    bezirks_linien[line[beznr_index]-1]+=line[length_index]
print(bezirks_linien[0])

#Abstellanlagen
abstellanlagen_data = list(radnetz_col.find({}))
abstellanlagen_col["geometry"].create_index([("coordinates", GEO2D)])

bezirksgrenzen_dataframe.sort_values('BEZ')
bezirksgrenzen_dataframe = bezirksgrenzen_dataframe.reset_index(drop=True)

#Radindex Collection insert
stationen_pro_bezirk_index = citybike_bezirk.columns.get_loc('stationen_pro_bezirk')

mydb.drop_collection(radindex)
radindex = mydb['radindex']


for document in bezirksgrenzen_col.find():
    radindex.insert_one(
        {'properties' : { "BEZNR" : document['properties']['BEZNR'], "BEZNAME" : document['properties']['NAMEK'],
                          "flaeche" : document['properties']['FLAECHE'],
                          'count_abstellanlagen' : int(abstellanlagen_col.count_documents({"geometry": {"$geoWithin": {"$geometry":
                                {"type": "Polygon", "coordinates": document['geometry']['coordinates']}}}})),
        "count_citybike_stellplätze" : int(citybike_bezirk.iloc[document['properties']['BEZNR']-1, stationen_pro_bezirk_index]),
         "länge_radwege_pro_bezirk" : bezirks_linien[document['properties']['BEZNR']-1]},
        'geometry': dict(document['geometry'])
    })

radindex["geometry"].create_index([("coordinates", GEO2D)])

radindex_data = list(radindex.find({}))
radindex_dataframe = gpd.GeoDataFrame.from_features(radindex_data)
radindex_dataframe.to_file(r"radindex.json",driver="GeoJSON")

# Geospatial_MongoDB

This repository uses pymongo, pandas, numpy and geopandas to manipulate data of a MongoDB. To use this you need a working MongoDB Cluster (which you can get for free).
Since the geospatial functionalities of MongoDB/pymongo are limited geopandas is used to manipulate the data.

In the file you can see two different ways of how to aggregate data. The first option is to group the data with a pymongo pipeline.
The second one ("#Radnetz pro Bezirk/district") uses geopandas to calculate the length of lines within different districts (INTERSECTION).

The last step is to insert the new data into the new collection "radindex". This collection is also exported as geojson.

https://github.com/geopandas/geopandas
https://github.com/mongodb/mongo-python-driver

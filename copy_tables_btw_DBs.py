

import os
import glob
import pandas as pd
import db_connect
import sqlalchemy as sal
import csv
import psycopg2
from sqlalchemy import *
import sqlalchemy as sal
from sqlalchemy import exc
from sqlalchemy.pool import NullPool
import geopandas as gpd
from shapely import wkb



# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

## function to transform Geometry from text to LINESTRING
def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)


# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_RM2015()
cur_HAIG = conn_HAIG.cursor()


## get table "aree_parcheggi" from DB "EcoTripRM_2015" @ server 192.168.132.18
aree_parcheggi = pd.read_sql_query(
    ''' SELECT *
        FROM zone.aree_parcheggi ''', conn_HAIG)


## copy table "aree parcheggi" to the DB "HAIG_ROMA" @ server 192.168.134.36

# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.1.0.1:5432/HAIG_ROMA', poolclass=NullPool)
connection = engine.connect()
aree_parcheggi.to_sql("aree_parcheggi", con=connection, schema="zone",
                   if_exists='append')


#################################################################
#################################################################

route = pd.read_sql_query(
    ''' SELECT *
        FROM public.route''', conn_HAIG)
route['geometry'] = route.apply(wkb_tranformation, axis=1)
route.drop(['geom'], axis=1, inplace= True)
route = gpd.GeoDataFrame(route)



import os
os.getcwd()

import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point
import folium
import osmnx as ox
import networkx as nx
import math
import momepy
from funcs_network_FK import roads_type_folium
from shapely import geometry
from shapely.geometry import Point, Polygon
import psycopg2
import db_connect
import datetime
from datetime import datetime
from datetime import date
from datetime import datetime
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import sqlalchemy as sal
import geopy.distance
from shapely import wkb

import multiprocessing as mp
from multiprocessing import Process, freeze_support, Manager
from time import sleep
from collections import deque
from multiprocessing.managers import BaseManager
import contextlib
from multiprocessing import Manager
from multiprocessing import Pool

import dill as Pickle
from joblib import Parallel, delayed
from joblib.externals.loky import set_loky_pickler
set_loky_pickler('pickle')
from multiprocessing import Pool,RLock

# today date
today = date.today()
today = today.strftime("%b-%d-%Y")

########################################################################################
########## DATABASE OPERATIONS #########################################################
########################################################################################

# connect to the ROMA DB with Viasat data
conn_HAIG = db_connect.connect_HAIG_Viasat_RM_2019()
cur_HAIG = conn_HAIG.cursor()

# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

## function to transform Geometry from text to LINESTRING
def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)

# Create an SQL connection engine to the output DB (Roma)
engine = sal.create_engine('postgresql://postgres:superuser@192.168.132.18:5432/HAIG_Viasat_RM_2019')


### loaf grapho of OpenStreetMap for RM_2019
os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')
os.getcwd()
## load grafo
file_graphml = 'Roma__Italy_70km.graphml'
grafo_ALL = ox.load_graphml(file_graphml)
## ox.plot_graph(grafo_ALL)
gdf_nodes_ALL, gdf_edges_ALL = ox.graph_to_gdfs(grafo_ALL)


# ###  to a DB and populate the DB RM_2019 ###
# connection = engine.connect()
# gdf_edges_ALL['geom'] = gdf_edges_ALL['geometry'].apply(wkb_hexer)
# gdf_edges_ALL.drop('geometry', 1, inplace=True)
# gdf_edges_ALL.to_sql("OSM_edges", con=connection, schema="public",
#                    if_exists='append')
#
# gdf_nodes_ALL['geom'] = gdf_nodes_ALL['geometry'].apply(wkb_hexer)
# gdf_nodes_ALL.drop('geometry', 1, inplace=True)
# gdf_nodes_ALL.to_sql("OSM_nodes", con=connection, schema="public",
#                    if_exists='append')
#
# # create index on the column (u,v) togethers in the table 'gdf_edges_ALL' ###
# # Multicolumn Indexes ####
#
# cur_HAIG.execute("""
# CREATE INDEX edges_UV_idx ON public."OSM_edges"(u,v);
# """)
# conn_HAIG.commit()
#
# conn_HAIG.close()
# cur_HAIG.close()



# get all ID terminal of Viasat data
all_VIASAT_IDterminals = pd.read_sql_query(
    ''' SELECT *
        FROM public.obu''', conn_HAIG)
all_VIASAT_IDterminals['idterm'] = all_VIASAT_IDterminals['idterm'].astype('Int64')
# make a list of all IDterminals (GPS ID of Viasata data) each ID terminal (track) represent a distinct vehicle
all_ID_TRACKS = list(all_VIASAT_IDterminals.idterm.unique())

### get a sample of FCD data for only one IDTERM
idterm = '4243118'

all_NODES = []
idterm = str(idterm)
# print('VIASAT GPS track:', track_ID)
viasat_data = pd.read_sql_query('''
              SELECT * FROM public.dataraw 
              WHERE idterm = '%s' 
              ''' % idterm, conn_HAIG)

### add new field with the NEARET NODE ID from OpenStreetMap
for row in viasat_data[['latitude', 'longitude']].itertuples(index=True):
    point = float(row.latitude), float(row.longitude)
    # print("lat", "lon:", point)
    nearest_node = ox.get_nearest_node(grafo_ALL, point, return_dist=False)
    node = row.Index, nearest_node
    all_NODES.append(node)


### make a dataframe
all_NODES = pd.DataFrame(all_NODES)
## rename columns
all_NODES = all_NODES.rename(columns={0: 'index', 1:'node'})
viasat_data = pd.concat([viasat_data, all_NODES], axis=1)


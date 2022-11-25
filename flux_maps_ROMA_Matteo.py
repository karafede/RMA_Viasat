
import os
os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')
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
import momepy
from shapely import wkb


# today date
today = date.today()
today = today.strftime("%b-%d-%Y")

os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')
os.getcwd()

########################################################################################
########## DATABASE OPERATIONS #########################################################
########################################################################################

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_Viasat_RM_2019()
cur_HAIG = conn_HAIG.cursor()


# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

## function to transform Geometry from text to LINESTRING
def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)

# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/HAIG_Viasat_RM_2019')

## load EDGES from OSM
gdf_edges = pd.read_sql_query('''
                            SELECT u,v, length, geom
                            FROM net.edges ''',conn_HAIG)
gdf_edges['geometry'] = gdf_edges.apply(wkb_tranformation, axis=1)
gdf_edges.drop(['geom'], axis=1, inplace= True)
gdf_edges = gpd.GeoDataFrame(gdf_edges)


gdf_nodes = pd.read_sql_query('''
                            SELECT *
                            FROM net.nodes ''',conn_HAIG)
gdf_nodes['geometry'] = gdf_nodes.apply(wkb_tranformation, axis=1)
gdf_nodes.drop(['geom'], axis=1, inplace= True)
gdf_nodes = gpd.GeoDataFrame(gdf_nodes)


## eventually....remove duplicates
gdf_edges.drop_duplicates(['u', 'v'], inplace=True)
# gdf_edges.plot()

#### this is the mapmatching ONLY for the date of the 09 October 2019 (wednesday) #########
###########################################################################################
###############################################################
##### or..... can also make grouping direclty into the DB #####

matched_data = pd.read_sql_query('''
                       WITH data AS(
                       SELECT  
                          mapmatching.u, mapmatching.v,
                               mapmatching.timedate, mapmatching.mean_speed, 
                               mapmatching.idtrace, mapmatching.sequenza,
                               mapmatching.idtrajectory,
                               dataraw.speed, dataraw.vehtype
                          FROM mapmatching
                          LEFT JOIN dataraw 
                                      ON mapmatching.idtrace = dataraw.id  
                                       WHERE date(mapmatching.timedate) = '2019-10-09' 
                                       /* WHERE EXTRACT(MONTH FROM mapmatching.timedate) = '03'*/
                                       AND dataraw.vehtype::bigint = 2
                          )
                       SELECT u, v, COUNT(*)
                       FROM  data
                       GROUP BY u, v
                       ''', conn_HAIG)


## get counts ("passaggi") across each EDGE
all_counts_uv = matched_data

# compute a relative frequeny (how much the edge was travelled compared to the total number of tracked vehicles...in %)
max_counts = max(all_counts_uv['count'])
all_counts_uv['frequency'] = (all_counts_uv['count']/max_counts)*100
all_counts_uv['frequency'] = round(all_counts_uv['frequency'], 0)
all_counts_uv['frequency'] = all_counts_uv.frequency.astype('int')

## rescale all data by an arbitrary number
all_counts_uv['scales'] = (all_counts_uv['count']/max_counts) * 7


## Normalize to 1 and get loads
all_counts_uv["load(%)"] = round(all_counts_uv["count"]/max(all_counts_uv["count"]),4)*100


## merge edges for congestion with the road network to get the geometry
all_counts_uv = pd.merge(all_counts_uv, gdf_edges, on=['u', 'v'], how='left')
all_counts_uv.drop_duplicates(['u', 'v'], inplace=True)

## sort by "frequency"
all_counts_uv.sort_values('frequency', ascending=True, inplace= True)


## make a geodataframe
all_counts_uv = gpd.GeoDataFrame(all_counts_uv)


####################################################################################
### create basemap (Roma)
ave_LAT = 41.888009265234906
ave_LON = 12.500281904062206
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
####################################################################################

folium.GeoJson(
all_counts_uv[['u','v', 'count', 'scales', 'frequency', 'load(%)', 'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': 'red',
        'color': 'red',
        'weight':  x['properties']['scales'],
        'fillOpacity': 1,
        },
highlight_function=lambda x: {'weight':3,
        'color':'blue',
        'fillOpacity':1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'load(%)']),
    ).add_to(my_map)

path = 'D:/ENEA_CAS_WORK/ROMA_2019/'
my_map.save(path + "WED_09_October_2019_HEAVY_Roma.html")

### save first as geojson file
all_counts_uv.to_file(filename='matched_routes_ROMA_09_October_2019.geojson', driver='GeoJSON')


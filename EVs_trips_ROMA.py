

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

os.chdir('D:/ENEA_CAS_WORK/BRESCIA')
os.getcwd()


# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex


## function to transform Geometry from text to LINESTRING
def wkb_tranformation(line):
    return wkb.loads(line.geom, hex=True)


########################################################################################
########## DATABASE OPERATIONS #########################################################
########################################################################################

# connect to BRESCIA DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_ROMA()
cur_HAIG = conn_HAIG.cursor()

### load OSM edges
gdf_edges = pd.read_sql_query('''
                            SELECT u,v, length, geom
                            FROM net.edges ''', conn_HAIG)
gdf_edges['geometry'] = gdf_edges.apply(wkb_tranformation, axis=1)
gdf_edges.drop(['geom'], axis=1, inplace=True)
gdf_edges = gpd.GeoDataFrame(gdf_edges)

## eventually....remove duplicates
gdf_edges.drop_duplicates(['u', 'v'], inplace=True)
# gdf_edges.plot()

# get all ID terminal of Viasat data
obu = pd.read_sql_query('''
                        SELECT *                                          
                        FROM obu                        
                        ''', conn_HAIG)

# get all ID terminal of Viasat data
I3_BMW = pd.read_sql_query('''
                        SELECT
                        idterm, idvehcategory, brand, anno                        
                        FROM obu
                        WHERE idvehcategory ILIKE 'i3%' and
                        brand = 'BMW'
                        ''', conn_HAIG)

# get all ID terminal of Viasat data
electric_veh = pd.read_sql_query('''
                        SELECT
                        idterm, idvehcategory, brand, anno                      
                        FROM obu
                        WHERE idvehcategory ILIKE ANY(ARRAY['ELE%', 'leaf%', 'zoe%', 'IPACE%', 'model%'])
                        ''', conn_HAIG)

hydrogen_veh = pd.read_sql_query('''
                        SELECT
                        idterm, idvehcategory, brand, anno                      
                        FROM obu
                        WHERE idvehcategory ILIKE ANY(ARRAY['h2%'])
                        ''', conn_HAIG)

# electric_veh = pd.concat([electric_veh, I3_BMW])

# get all ID terminal of Viasat data
idterms = pd.read_sql_query(
    ''' SELECT DISTINCT idterm
        FROM public.mapmatching ''', conn_HAIG)

### transform into integers
idterms['idterm'] = idterms['idterm'].astype('int')
### check if IDs of Electriv vehicles are within the map-matched vehicles
matched_IDTERMS = electric_veh[electric_veh.idterm.isin(list(pd.to_numeric(idterms.idterm)))]
### make a list of "matched_IDTERMS"
matched_IDTERMS = list(matched_IDTERMS.idterm.unique())
## EVs ID ----> [4489912 (SMART), 5916227 (ZOE), 4480628 (LEAF), 4368379 (SMART), 3319839 (LEAF), 5049280 (ZOE)]


#### get map-matched data ONLY for selected ELECTIC VEHICLES
matched_data_all = pd.read_sql_query('''
                        WITH data AS(
                       SELECT  
                          mapmatching.u, mapmatching.v,
                               mapmatching.timedate, mapmatching.mean_speed, 
                               mapmatching.idtrace, mapmatching.sequenza,
                               mapmatching.idtrajectory, mapmatching.idterm,
                               dataraw.speed, dataraw.vehtype
                          FROM mapmatching
                          LEFT JOIN dataraw 
                                      ON mapmatching.idtrace = dataraw.id  
                                       WHERE date(mapmatching.timedate) = '2019-10-09' 
                                       /* WHERE EXTRACT(MONTH FROM mapmatching.timedate) = '03'*/
                                       AND dataraw.vehtype::bigint = 1 
                                       /* WHERE mapmatching.idterm in (3554511, 4278931, 4497703)*/
                                        /* WHERE mapmatching.idterm in (3329564) */
                          )
                      SELECT u, v, idterm, count(*)                     
                      from data
                      group by u,v, idterm
                       ''', conn_HAIG)

#### get map-matched data ONLY for selected ELECTIC VEHICLES
matched_data = pd.read_sql_query('''
                        WITH data AS(
                       SELECT  
                          mapmatching.u, mapmatching.v,
                               mapmatching.timedate, mapmatching.mean_speed, 
                               mapmatching.idtrace, mapmatching.sequenza,
                               mapmatching.idtrajectory, mapmatching.idterm,
                               dataraw.speed, dataraw.vehtype
                          FROM mapmatching
                          LEFT JOIN dataraw 
                                      ON mapmatching.idtrace = dataraw.id  
                                       /* WHERE date(mapmatching.timedate) = '2019-03-14' */
                                       /* WHERE EXTRACT(MONTH FROM mapmatching.timedate) = '03'*/
                                       /* AND dataraw.vehtype::bigint = 2 */
                                       /* WHERE mapmatching.idterm in (3554511, 4278931, 4497703)*/
                                       WHERE mapmatching.idterm in (4443922)
                          )
                      SELECT u, v, idterm, count(*)                     
                      from data
                      group by u,v, idterm
                       ''', conn_HAIG)

matched_data['idterm'] = matched_data['idterm'].astype('int')

### add info from EV model
matched_data = pd.merge(matched_data, electric_veh[['idterm', 'idvehcategory']], on=['idterm'], how='left')
matched_data = pd.merge(matched_data, obu[['idterm', 'idvehcategory']], on=['idterm'], how='left')

########################################################
##### build the map ####################################
all_counts_uv = matched_data
all_counts_uv = pd.merge(all_counts_uv, gdf_edges, on=['u', 'v'], how='left')
## remove none values
all_counts_uv = all_counts_uv.dropna()
all_counts_uv = gpd.GeoDataFrame(all_counts_uv)
all_counts_uv.drop_duplicates(['u', 'v'], inplace=True)
# all_counts_uv.plot()

## rescale all data by an arbitrary number
all_counts_uv["scales"] = (all_counts_uv["count"] / max(all_counts_uv["count"])) * 7
## Normalize to 1 and get loads
all_counts_uv["load(%)"] = round(all_counts_uv["count"] / max(all_counts_uv["count"]), 4) * 100

####################################################################################
### create basemap (Roma)
ave_LAT = 41.888009265234906
ave_LON = 12.500281904062206
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='cartodbpositron')
####################################################################################


folium.GeoJson(
    all_counts_uv[['u', 'v', 'count', 'idvehcategory', 'scales', 'load(%)', 'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': 'blue',
        'color': 'blue',
        'weight': x['properties']['scales'],
        'fillOpacity': 1,
    },
    highlight_function=lambda x: {'weight': 3,
                                  'color': 'red',
                                  'fillOpacity': 1
                                  },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['u', 'v', 'load(%)', 'idvehcategory']),
).add_to(my_map)

path = 'D:/ENEA_CAS_WORK/ROMA_2019/'
my_map.save(path + "DACIA_SNADERO_4443922_Roma.html")

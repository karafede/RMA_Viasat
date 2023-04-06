
import os
os.getcwd()
os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')

import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point
import folium
import osmnx as ox
import networkx as nx
import math
# import momepy
# from funcs_network_FK import roads_type_folium
from shapely import geometry
from shapely.geometry import Point, Polygon
import psycopg2
import db_connect
import datetime
from datetime import datetime
from datetime import date
from datetime import datetime
# from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import sqlalchemy as sal
from sqlalchemy import exc
from sqlalchemy.pool import NullPool
import geopy.distance

import multiprocessing as mp
from multiprocessing import Process, freeze_support, Manager
from time import sleep
from collections import deque
from multiprocessing.managers import BaseManager
import contextlib
from multiprocessing import Manager
from multiprocessing import Pool

# import dill as Pickle
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

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_ROMA()
cur_HAIG = conn_HAIG.cursor()


# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.1.0.1:5432/HAIG_ROMA', poolclass=NullPool)

## create extension postgis on the database HAIG_Viasat_RM_2019  (only one time)
# erase existing table
# cur_HAIG.execute("DROP TABLE IF EXISTS routecheck CASCADE")
# conn_HAIG.commit()

"""
lat_lon = pd.read_sql_query(
    ''' SELECT latitude, longitude
        FROM public.dataraw ''', conn_HAIG)
        
        
borders = pd.read_sql_query(
    ''' SELECT *
        FROM public.routecheck_trenta_bis 
        where routecheck_trenta_bis.border = 'in'
         ''', conn_HAIG)



# get all ID terminal of Viasat data
all_VIASAT_IDterminals = pd.read_sql_query(
    ''' SELECT *
        FROM public.idterm_portata ''', conn_HAIG)
all_VIASAT_IDterminals['idterm'] = all_VIASAT_IDterminals['idterm'].astype('Int64')
all_VIASAT_IDterminals['portata'] = all_VIASAT_IDterminals['portata'].astype('Int64')

# make a list of all IDterminals (GPS ID of Viasata data) each ID terminal (track) represent a distinct vehicle
all_ID_TRACKS = list(all_VIASAT_IDterminals.idterm.unique())
with open("D:/ENEA_CAS_WORK/ROMA_2019/all_idterms.txt", "w") as file:
    file.write(str(all_ID_TRACKS))

"""

with open("D:/ENEA_CAS_WORK/ROMA_2019/all_idterms.txt", "r") as file:
      all_ID_TRACKS = eval(file.readline())

## reload 'all_ID_TRACKS' as list
# with open("D:/ENEA_CAS_WORK/ROMA_2019/all_idterms_new.txt", "r") as file:
#     all_ID_TRACKS = eval(file.readline())


# track_ID = '2704220'   ### fleet
# track_ID = '4457330'   ### car
# track_ID = '4137321'   ### car
# track_ID = '2750102'
# track_ID = '2704220'
# track_ID = '5403701'



# track_ID = '4460340'   # MPia & Valentina
# track_ID = '3130436'    # MPia & Valentina
# track_ID = '5922139'    # MPia & Valentina
# track_ID = '5912730'    # MPia & Valentina  # dovrebbe essere un in e out... unico spostamento di passaggio nel mese
# track_ID = '4336611'    # MPia & Valentina
# track_ID = '4475232'    # MPia & Valentina
# track_ID = '4494697'    # MPia & Valentina
# track_ID =  '4425586'     # MPia & Valentina   # trajectory '76278843'
# track_ID =  '5221906'   # 5221906_1
# track_ID =  '3102463'   # 43558483
# track_ID =  '5044370'   # 43558483
# track_ID = '4397646'
# track_ID = '3106788'
# track_ID = '3103312'
# track_ID = '3109045'
# track_ID = '3135499'
# track_ID = '5442341'



"""
## load grafo


import os
os.chdir('D:/ENEA_CAS_WORK/ROMA_2019/data')
os.getcwd()

file_graphml = 'Roma__Italy_70km.graphml'
grafo_ALL = ox.load_graphml(file_graphml)
## ox.plot_graph(grafo_ALL)
# gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)
gdf_nodes_ALL, gdf_edges_ALL = ox.graph_to_gdfs(grafo_ALL)
gdf_edges_ALL.plot()

### get extent of Rome get extent of viasat data
p1 = Point(min(gdf_nodes_ALL.x) - ext, min(gdf_nodes_ALL.y) -ext )
## bottom-right corner
p2 = Point(max(gdf_nodes_ALL.x) + ext, min(gdf_nodes_ALL.y) -ext)
## bottom-left corner
p3 = Point(max(gdf_nodes_ALL.x) + ext, max(gdf_nodes_ALL.y) +ext)
## top-left corner
p4 = Point(min(gdf_nodes_ALL.x) - ext, max(gdf_nodes_ALL.y) +ext)

"""

## set external border
ext =  +0.03 #+0.10 ## (3km)


Lat_Max = 42.1010
Lat_Min = 41.5588
Lon_Min = 12.0870
Lon_Max = 13.1900


## ---> define an extent area containing our Viasat data
## top-right corner
p1 = Point(12.0870 + ext, 41.5588 +ext)  # (x,y)  OK
## bottom-right corner
p2 = Point(13.1900 - ext, 41.5588 +ext)    #
## bottom-left corner
p3 = Point(13.1900 - ext, 42.1010 -ext)
## top-left corner
p4 = Point(12.0870 + ext, 42.1010 -ext)



from math import radians, cos, sin, asin, sqrt
def great_circle_track_node(lon_end, lat_end, lon_start, lat_start):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radiants
    lon1, lat1, lon2, lat2 = map(radians, [lon_end, lat_end, lon_start, lat_start])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 1000  # from kilometers to meters

distance_border_to_border = great_circle_track_node(11.9214+ext, 41.56645+ext, 13.055-ext, 42.13107-ext)


# Initialize a test GeoDataFrame where geometry is a list of points
ROMA_extent = gpd.GeoDataFrame([['box', p1],
                                  ['box', p2],
                                  ['box', p3],
                                  ['box', p4]],
                                 columns=['shape_id', 'geometry'],
                                 geometry='geometry')

# Extract the coordinates from the Point object
ROMA_extent['geometry'] = ROMA_extent['geometry'].apply(lambda x: x.coords[0])
# Group by shape ID
#  1. Get all of the coordinates for that ID as a list
#  2. Convert that list to a Polygon
ROMA_extent = ROMA_extent.groupby('shape_id')['geometry'].apply(lambda x: Polygon(x.tolist())).reset_index()
# Declare the result as a new a GeoDataFrame
ROMA_extent = gpd.GeoDataFrame(ROMA_extent, geometry = 'geometry')
# ROMA_extent.plot()



"""
os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')
os.getcwd()
ROMA_extent.to_file(filename='ROMA_extent.geojson', driver='GeoJSON')
import geopandas as gpd
df = gpd.read_file('ROMA_extent.geojson')
"""

## trip on the border
# track_ID = "4414079"
# idtrajectory = 44602132
# trip = "4414079_2"


def func(arg):
    last_track_idx, track_ID = arg

## to be used when the query stop. Start from the last saved index
# for last_track_idx, track_ID in enumerate(all_ID_TRACKS):
    print(track_ID)
    track_ID = str(track_ID)
    # print('VIASAT GPS track:', track_ID)
    viasat_data = pd.read_sql_query('''
                SELECT * FROM public.dataraw 
                WHERE idterm = '%s' ''' % track_ID, conn_HAIG)
    # remove duplicate GPS tracks (@ same position)
    viasat_data.drop_duplicates(['latitude', 'longitude', 'timedate'], inplace=True, keep='last')
    #  viasat_data.drop_duplicates(['timedate'], inplace=True, keep='last')
    # sort data by timedate:
    viasat_data['timedate'] = viasat_data['timedate'].astype('datetime64[ns]')
    ## consider only progressive >= 0
    viasat_data = viasat_data[viasat_data['progressive'] >=0 ]
    # sort by 'timedate'
    viasat_data = viasat_data.sort_values('timedate')
    ### remove data with "speed" ==0  and "odometer" != 0 AT THE SAME TIME!
    # viasat_data = viasat_data[~((viasat_data['progressive'] != 0) & (viasat_data['speed'] == 0))]
    ### select only VIASAT point with accuracy ("grade") between 1 and 22
    # viasat_data = viasat_data[(1 <= viasat_data['grade']) & (viasat_data['grade'] <= 15)]
    if len(viasat_data) == 0:
        print('============> no VIASAT data for that day ==========')

########################################################################################
########################################################################################
########################################################################################

    # if (len(viasat_data) > 3) and sum(viasat_data.progressive)> 0:  # <----
    if len(viasat_data) > 0:  # <----
        fields = ["id", "idrequest", "longitude", "latitude", "progressive", 'panel', 'grade', "timedate", "speed", "vehtype"]
        # viasat = pd.read_csv(viasat_data, usecols=fields)
        viasat = viasat_data[fields]
        ## add a field for "anomalies"
        viasat['anomaly'] = '0123456'
        viasat['border'] = 'to_define'
        # transform "datetime" into seconds
        # separate date from time
        # transform object "datetime" into  datetime format
        viasat['timedate'] = viasat['timedate'].astype('datetime64[ns]')
        base_time = datetime(1970, 1, 1)
        viasat['totalseconds'] = pd.to_datetime(viasat['timedate'], format='% M:% S.% f')
        viasat['totalseconds'] = pd.to_datetime(viasat['totalseconds'], format='% M:% S.% f') - base_time
        viasat['totalseconds'] = viasat['totalseconds'].dt.total_seconds()
        viasat['totalseconds'] = viasat.totalseconds.astype('int')
        # date
        viasat['date'] = viasat['timedate'].apply(lambda x: x.strftime("%Y-%m-%d"))
        # date and month
        viasat['year_month'] = viasat['timedate'].apply(lambda x: x.strftime("%Y-%m"))
        # year
        viasat['year'] = viasat['timedate'].apply(lambda x: x.strftime("%Y"))
        # hour
        viasat['hour'] = viasat['timedate'].apply(lambda x: x.hour)
        # minute
        viasat['minute'] = viasat['timedate'].apply(lambda x: x.minute)
        # seconds
        viasat['seconds'] = viasat['timedate'].apply(lambda x: x.second)

        #### select only YEAR 2019 ######
        #################################
        # viasat = viasat[viasat.year_month == '2019-03']  ## March
        # viasat = viasat[viasat.year_month == '2019-11']  ## November
        if len(viasat) > 0:
            viasat = viasat.sort_values(['timedate', 'idrequest'])
            # viasat = viasat.sort_values('idrequest')
            # make one field with time in seconds
            viasat['path_time'] = viasat['hour'] * 3600 + viasat['minute'] * 60 + viasat['seconds']
            viasat = viasat.reset_index()
            # make difference in totalaseconds from the start of the first trip of each TRACK_ID (need this to compute trips)
            viasat['path_time'] = viasat['totalseconds'] - viasat['totalseconds'][0]
            viasat = viasat[["id", "longitude", "latitude", "progressive", "path_time", "totalseconds",
                             "panel", "grade", "speed", "hour", "timedate", "vehtype", "anomaly", "border"]]
            viasat['last_totalseconds'] = viasat.totalseconds.shift()   # <-------
            viasat['last_progressive'] = viasat.progressive.shift()  # <-------
            ## set nan values to -1
            viasat.last_totalseconds = viasat.last_totalseconds.fillna(-1)   # <-------
            viasat.last_progressive = viasat.last_progressive.fillna(-1)  # <-------
            ## get only VIASAT data where difference between two consecutive points is > 600 seconds (10 minutes)
            ## this is to define the TRIP after a 'long' STOP time
            viasat1 = viasat
            ## compute difference in seconds between two consecutive tracks
            diff_time = viasat.path_time.diff()
            viasat_data['next_speed'] = viasat_data.speed.shift(-1)
            viasat_data['next_progressive'] = viasat_data.progressive.shift(-1)
            diff_time = diff_time.fillna(0)
            VIASAT_TRIPS_by_ID = pd.DataFrame([])
            row = []
            ## define a list with the starting indices of each new TRIP
            for i in range(len(diff_time)):
                if viasat_data.vehtype.iloc[0] == 1:
                    if (diff_time.iloc[i] >= 300):  ## 30 min  ## 1800 seconds (interval between two trips)  (or progressive == 0 and vehtype == 1)
                            # and (viasat_data.panel.iloc[i - 1] == 0 or viasat_data.panel.iloc[i] == 0)):
                        row.append(i)
                if viasat_data.vehtype.iloc[0] == 2:
                    if (diff_time.iloc[i] >= 300):   ## 30 min ## 1800 seconds (interval between two trips)
                            # and (viasat_data.panel.iloc[i - 1] == 0 or viasat_data.panel.iloc[i] == 0)):
                          #   and (viasat_data.next_speed.iloc[i] > 0 and
                          #       viasat_data.speed.iloc[i] == 0)):
                        row.append(i)
            # get next element of the list row
            if len(row) > 0:
                row.append("end")
                # split Viasat data by TRIP (for a given ID..."idterm")
                for idx, j in enumerate(row):
                    # print(j)
                    # print(idx)
                    # assign an unique TRIP ID
                    TRIP_ID = str(track_ID) + "_" + str(idx)
                    print(TRIP_ID)

                    if j == row[0]:  # first TRIP
                        lista = [i for i in range(0,j)]
                        # print(lista)
                        ## get  subset of VIasat data for each list:
                        viasat = viasat1.iloc[lista, :]
                        viasat['TRIP_ID'] = TRIP_ID
                        viasat['idtrajectory'] = viasat.id.iloc[0]
                        # print(viasat)
                        VIASAT_TRIPS_by_ID = VIASAT_TRIPS_by_ID.append(viasat)

                    if (idx > 0 and j != 'end'):   # intermediate TRIPS
                        lista = [i for i in range(row[idx - 1], row[idx])]
                        # print(lista)
                        ## get  subset of VIasat data for each list:
                        viasat = viasat1.iloc[lista, :]
                        viasat['TRIP_ID'] = TRIP_ID
                        viasat['idtrajectory'] = viasat.id.iloc[0]
                        # print(viasat)
                        VIASAT_TRIPS_by_ID = VIASAT_TRIPS_by_ID.append(viasat)

                    if j == "end":  # last trip for that ID
                        # lista = [i for i in range(row[idx-1], len(viasat))]
                        lista = [i for i in range(row[idx-1], len(viasat1))]
                        # print(lista)
                        ## get  subset of VIasat data for each list:
                        viasat = viasat1.iloc[lista, :]
                        viasat['TRIP_ID'] = TRIP_ID
                        viasat['idtrajectory'] = viasat.id.iloc[0]
                        # print(viasat)
                        ## if first "progressive > 0 and panel == 0, then remove (not true for fleets)
                        # if (viasat.iloc[0].progressive > 0 & viasat.iloc[0].panel == 0 & viasat.iloc[0].vehtype == 1):
                        #    viasat = viasat.drop(viasat.index[[0]])
                        ## append all TRIPS by ID
                        VIASAT_TRIPS_by_ID = VIASAT_TRIPS_by_ID.append(viasat)
                        os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')
                        # VIASAT_TRIPS_by_ID.to_csv('VIASAT_TRIPS_by_ID_4460340.csv', sep=',')
                        # VIASAT_TRIPS_by_ID.to_csv('VIASAT_TRIPS_by_ID_3130436.csv', sep=',')
                        # VIASAT_TRIPS_by_ID.to_csv('VIASAT_TRIPS_by_ID_4494697.csv', sep=',')



                    #############################################
                    ### add anomaly codes from Carlo Liberto ####
                    #############################################

                        # VIASAT_TRIPS_by_ID = VIASAT_TRIPS_by_ID[
                        #    ~((VIASAT_TRIPS_by_ID['panel'] == 0) & (VIASAT_TRIPS_by_ID['speed'] == 0))]
                        # final_TRIPS = pd.DataFrame([])
                        ## get unique trips
                        ### get counts for selected edges ###
                        ### counts all TRIP-ID and remove those one appears only TWICE
                        counts_TRIP_ID = VIASAT_TRIPS_by_ID.groupby(VIASAT_TRIPS_by_ID[['TRIP_ID']].columns.tolist(), sort=False).size().reset_index().rename(columns={0: 'counts'})
                        counts_TRIP_ID = counts_TRIP_ID[counts_TRIP_ID.counts > 2]
                        all_TRIPS = list(counts_TRIP_ID.TRIP_ID.unique())
                        VIASAT_TRIPS_by_ID['last_panel'] = VIASAT_TRIPS_by_ID.panel.shift()
                        VIASAT_TRIPS_by_ID['last_lon'] = VIASAT_TRIPS_by_ID.longitude.shift()
                        VIASAT_TRIPS_by_ID['last_lat'] = VIASAT_TRIPS_by_ID.latitude.shift()
                        VIASAT_TRIPS_by_ID['last_totalseconds'] = VIASAT_TRIPS_by_ID.totalseconds.shift()
                        # VIASAT_TRIPS_by_ID['next_totalseconds'] = VIASAT_TRIPS_by_ID.totalseconds.shift(-1)
                        VIASAT_TRIPS_by_ID['next_timedate'] = VIASAT_TRIPS_by_ID.timedate.shift(-1)
                        ## set nan values to -1
                        VIASAT_TRIPS_by_ID.last_panel= VIASAT_TRIPS_by_ID.last_panel.fillna(-1)
                        VIASAT_TRIPS_by_ID.last_lon = VIASAT_TRIPS_by_ID.last_lon.fillna(-1)
                        VIASAT_TRIPS_by_ID.last_lat = VIASAT_TRIPS_by_ID.last_lat.fillna(-1)
                        VIASAT_TRIPS_by_ID['last_panel'] = VIASAT_TRIPS_by_ID.last_panel.astype('int')
                        ## loop all over the TRIPS
                        # all_TRIPS = ["4497310_11"]
                        for idx_trip, trip in enumerate(all_TRIPS):
                            ## trip = "3135499_215"
                            VIASAT_TRIP = VIASAT_TRIPS_by_ID[VIASAT_TRIPS_by_ID.TRIP_ID == trip]

                            ## ---->>>> compute increments of the distance (in meters) <<<<-----------------
                            VIASAT_TRIP['increment'] = VIASAT_TRIP.progressive - VIASAT_TRIP.last_progressive


                            VIASAT_TRIP.reset_index(drop=True, inplace=True)
                            # print(VIASAT_TRIP)
                            timeDiff = VIASAT_TRIP.totalseconds.iloc[0] - VIASAT_TRIP.last_totalseconds.iloc[0]
                            if (idx_trip == 0):
                                progr = 0
                            else:
                                progr = VIASAT_TRIP.progressive.iloc[0] - VIASAT_TRIP.last_progressive.iloc[0]

                            for idx_row, row in VIASAT_TRIP.iterrows():
                                coords_1 = (row.latitude, row.longitude)
                                coords_2 = (row.last_lat, row.last_lon)
                                lDist = (geopy.distance.geodesic(coords_1, coords_2).km)*1000  # in meters
                                ####### PANEL ###################################################
                                if (row.panel == 1 and row.last_panel == 1):  # errore on-on
                                    s = (list(row.anomaly))
                                    s[0] = "E"
                                    s = "".join(s)
                                    VIASAT_TRIP.at[len(VIASAT_TRIP)-1, "anomaly"] = s
                                    # set the intermediates anomaly to "I"
                                    s = (list(row.anomaly))
                                    s[0] = "I"
                                    s = "".join(s)
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                    s = (list(row.anomaly))
                                    s[0] = "S"
                                    s = "".join(s)
                                    VIASAT_TRIP.at[0, "anomaly"] = s
                            # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)

                                elif (row.panel == 0 and row.last_panel == -1):
                                    s = (list(row.anomaly))
                                    s[0] = "E"
                                    s = "".join(s)
                                    # VIASAT_TRIP.at[0, "anomaly"] = s
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                            # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)
                                elif (row.panel == 0 and row.last_panel == 0):  # off-off
                                    s = (list(row.anomaly))
                                    s[0] = "E"
                                    s = "".join(s)
                                    # VIASAT_TRIP.at[0, "anomaly"] = s
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                    s = (list(row.anomaly))
                                    s[0] = "E"
                                    s = "".join(s)
                                    VIASAT_TRIP.at[len(VIASAT_TRIP) - 1, "anomaly"] = s
                                elif (row.panel == 0 and row.last_panel == 1):  # ON-off
                                    s = (list(row.anomaly))
                                    s[0] = "E"
                                    s = "".join(s)
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                            # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)

                                elif (row.panel == 1 and row.last_panel == -1):
                                    s = (list(row.anomaly))
                                    s[0] = "I"
                                    s = "".join(s)
                                    # VIASAT_TRIP.at[0, "anomaly"] = s
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                    # print(VIASAT_TRIP)
                            # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)
                                elif (row.panel == 1 and row.last_panel == 0):
                                    s = (list(row.anomaly))
                                    s[0] = "E"
                                    s = "".join(s)
                                    VIASAT_TRIP.at[len(VIASAT_TRIP)-1, "anomaly"] = s
                                    s = (list(row.anomaly))
                                    s[0] = "S"
                                    s = "".join(s)
                                    # VIASAT_TRIP.at[0, "anomaly"] = s
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                    # print(VIASAT_TRIP)
                            # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)

                          ####### TRAVEL time > 10 min ###############################################
                                if (row.last_panel ==1 and row.panel ==1 and timeDiff > 10*60):
                                    s = list(VIASAT_TRIP.iloc[0].anomaly)
                                    s[0] = "S"
                                    s[4] = "T"
                                    s = "".join(s)
                                    VIASAT_TRIP.at[0, "anomaly"] = s
                                    s = list(VIASAT_TRIP.iloc[len(VIASAT_TRIP) - 1].anomaly)
                                    s[0] = "E"
                                    s[4] = "T"
                                    s = "".join(s)
                                    VIASAT_TRIP.at[len(VIASAT_TRIP) - 1, "anomaly"] = s
                            # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)

                                if (row.grade <= 15):
                                    s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                    s[1] = "Q"
                                    s = "".join(s)
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                elif (row.grade > 15):
                                    s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                    s[1] = "q"
                                    s = "".join(s)
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                if (lDist > 0 and VIASAT_TRIP["anomaly"].iloc[idx_row] != "S"):
                                    if (progr / lDist < 0.9):
                                        s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                        s[2] = "c"
                                        s = "".join(s)
                                        VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                    elif (progr / lDist > 10 and progr > 2200):
                                        s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                        s[3] = "C"
                                        s = "".join(s)
                                        VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                if (timeDiff > 0 and 3.6 * 1000 * progr / timeDiff > 250):
                                    s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                    s[5] = "V"
                                    s = "".join(s)
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                if (row.panel != 1 and progr > 10000):
                                    s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                    s[0] = "S"
                                    s = "".join(s)
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                    s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                    s[6] = "D"
                                    s = "".join(s)
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                                    VIASAT_TRIP.at[len(VIASAT_TRIP) - 1, "anomaly"] = s
                                    s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                    s[0] = "E"
                                    s = "".join(s)
                                    VIASAT_TRIP.at[len(VIASAT_TRIP) - 1, "anomaly"] = s
                                elif (row.panel != 0 and progr <= 0):
                                    s = list(VIASAT_TRIP.iloc[idx_row].anomaly)
                                    s[6] = "d"
                                    s = "".join(s)
                                    VIASAT_TRIP["anomaly"].iloc[idx_row] = s
                            ### add all TRIPS together ##################
                            # final_TRIPS = final_TRIPS.append(VIASAT_TRIP)

                            ### remove columns and add terminal ID
                            # VIASAT_TRIP['track_ID'] = track_ID
                            VIASAT_TRIP['idterm'] = track_ID
                            VIASAT_TRIP['segment'] = VIASAT_TRIP.index

                            #####################################################################################
                            ## ------->>>>>>>>>> ################################################################
                            ## check if a trip is within the geographical extent of the Viasat data
                            # print("track_id-------------->>", track_ID, "trip---------->>:", trip)
                            geometry = [Point(xy) for xy in zip(VIASAT_TRIP.longitude, VIASAT_TRIP.latitude)]
                            df = GeoDataFrame(VIASAT_TRIP, geometry=geometry)
                            # make a buffer around each Viasat point (track)
                            df_buffer = df
                            df_buffer.reset_index(drop=True, inplace=True)
                            # create an index column
                            df_buffer["ID"] = df_buffer.index
                            buffer_diam = 0.00200  ## (200 meters)
                            buffer = df_buffer.buffer(buffer_diam)  ## buffer diameter == 200 meters
                            buffer_viasat = pd.DataFrame(buffer)
                            buffer_viasat.columns = ['geometry']
                            buffer_viasat = gpd.GeoDataFrame(buffer_viasat)
                            ## make intersection between Viasat buffered pints and border polygon
                            within_border = list(
                                (gpd.sjoin(ROMA_extent, buffer_viasat, how="left", op="intersects")).index_right)
                            VIASAT_TRIP_index = set(list(VIASAT_TRIP['segment']))
                            ## traces inside the border "cornice" or "frame"
                            border_index = set(within_border)
                            missing = list(sorted(VIASAT_TRIP_index - border_index))

                            # Le possibilità sono 4:
                            # 1) border_out (il veicolo è dentro la zona dati ma la sua destinazione sta fuori)
                            # 2) border_in (l'origine del veicolo è fuori dalla zona dati)
                            # 3) in_out (il veicolo ha origine e destinazione all'ESTERNO della zona dati)
                            # 4) in (il veicolo ha origine e destinazione all'INTERNO della zona dati)

                            ext = 0.03

                            """
                            Lat_Max = max(lat_lon.latitude)
                            Lat_Min = min(lat_lon.latitude)
                            Lon_Max = max(lat_lon.longitude)
                            Lon_Min = min(lat_lon.longitude)
                            """

                            Lat_Max = 42.1010
                            Lat_Min = 41.5588
                            Lon_Min = 12.0870
                            Lon_Max = 13.1900

                            Lat_Max_Int = Lat_Max - ext
                            Lat_Min_Int = Lat_Min + ext
                            Lon_Min_Int = Lon_Min + ext
                            Lon_Max_Int = Lon_Max - ext



                            if (len(missing) > 0):
                                #  1) first point near/inside the border
                                #  2) last point inside the border
                                # if (VIASAT_TRIP[VIASAT_TRIP.segment == min(VIASAT_TRIP.segment)][['segment']].iloc[0][0] not in missing and
                                #      VIASAT_TRIP[VIASAT_TRIP.segment == max(VIASAT_TRIP.segment)][['segment']].iloc[0][0] not in missing):
                                if (VIASAT_TRIP[VIASAT_TRIP.segment == min(VIASAT_TRIP.segment)][['segment']].iloc[0][0] in missing and
                                      VIASAT_TRIP[VIASAT_TRIP.segment == max(VIASAT_TRIP.segment)][['segment']].iloc[0][0] in missing):
                                    VIASAT_TRIP['border'] = "in_out"
                                    print("--------------------------> in_out ----------------")

                                elif (VIASAT_TRIP[VIASAT_TRIP.segment == min(VIASAT_TRIP.segment)][['segment']].iloc[0][0] in missing):
                                    VIASAT_TRIP['border'] = "border_in"
                                    print("--------------------------> border_in ----------------")
                                    # 1) first point inside the border
                                    # 2) last point  far from the border
                                elif (VIASAT_TRIP[VIASAT_TRIP.segment == max(VIASAT_TRIP.segment)][['segment']].iloc[0][0] in missing):
                                    VIASAT_TRIP['border'] = "border_out"
                                    print("--------------------------> border_out ----------------")
                                elif (VIASAT_TRIP[VIASAT_TRIP.segment == min(VIASAT_TRIP.segment)][['segment']].iloc[0][0] not in missing and
                                      VIASAT_TRIP[VIASAT_TRIP.segment == max(VIASAT_TRIP.segment)][['segment']].iloc[0][0] not in missing):
                                    VIASAT_TRIP['border'] = "in"
                                    print("--------------------------> in ----------------")

                            elif (len(missing) == 0):
                                VIASAT_TRIP['border'] = "in"
                                print("--------------------------> in ----------------")

                            #####################################################################################
                            ## ------->>>>>>>>>> ################################################################


                            VIASAT_TRIP.drop(['last_panel', 'last_lon', 'last_lat',    # <------
                                              'last_totalseconds', 'last_progressive', 'geometry', 'ID'], axis=1,
                                             inplace=True)

                            #### Connect to database using a context manager and populate the DB ####
                            try:
                                connection = engine.connect()
                                VIASAT_TRIP.to_sql("routecheck_cinque", con=connection, schema="public",
                                                   if_exists='append')
                                # VIASAT_TRIP.to_sql("routecheck_test_fk", con=connection, schema="public",
                                #                   if_exists='append')
                                # VIASAT_TRIP.to_sql("routecheck_5922139", con=connection, schema="public",
                                #                    if_exists='append')
                                # VIASAT_TRIP.to_sql("routecheck_3130436", con=connection, schema="public",
                                #                  if_exists='append')
                                # VIASAT_TRIP.to_sql("routecheck_5044370", con=connection, schema="public",
                                #                    if_exists='append')


                                connection.close()
                            except exc.OperationalError:
                                print('OperationalError')

                                connection = engine.connect()
                                VIASAT_TRIP.to_sql("routecheck_cinque", con=connection, schema="public",
                                                   if_exists='append')
                                # VIASAT_TRIP.to_sql("routecheck_test_fk", con=connection, schema="public",
                                #                   if_exists='append')
                                # VIASAT_TRIP.to_sql("routecheck_5922139", con=connection, schema="public",
                                #                    if_exists='append')
                                # VIASAT_TRIP.to_sql("routecheck_3130436", con=connection, schema="public",
                                #                  if_exists='append')
                                # VIASAT_TRIP.to_sql("routecheck_5044370", con=connection, schema="public",
                                #                    if_exists='append')
                                connection.close()


################################################
##### run all script using multiprocessing #####
################################################

## check how many processer we have available:
# print("available processors:", mp.cpu_count())

if __name__ == '__main__':
    # pool = mp.Pool(processes=mp.cpu_count()) ## use all available processors
    pool = mp.Pool(processes=40)     ## use 55 processors
    print("++++++++++++++++ POOL +++++++++++++++++", pool)
    results = pool.map(func, [(last_track_idx, track_ID) for last_track_idx, track_ID in enumerate(all_ID_TRACKS)])
    pool.close()
    pool.close()
    pool.join()

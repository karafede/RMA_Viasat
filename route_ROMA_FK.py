
#####################################
### build route from routecheck #####
#####################################

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
from shapely.geometry import Point, LineString, shape
from shapely import wkt
import psycopg2
import db_connect
import datetime
from datetime import datetime
from datetime import date
from datetime import datetime
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import sqlalchemy as sal
from sqlalchemy import exc
import sqlalchemy as sal
from sqlalchemy.pool import NullPool
import geopy.distance
import momepy
from shapely import wkb
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from PIL import Image


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


# connect to new DB
# conn_HAIG = db_connect.connect_HAIG_Viasat_RM_2019()
conn_HAIG = db_connect.connect_HAIG_ROMA()
cur_HAIG = conn_HAIG.cursor()

# Create an SQL connection engine to the output DB
# engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/HAIG_Viasat_RM_2019', poolclass=NullPool)
engine = sal.create_engine('postgresql://postgres:superuser@10.1.0.1:5432/HAIG_ROMA', poolclass=NullPool)

### erase existing table...if exists....
# cur_HAIG.execute("DROP TABLE IF EXISTS route CASCADE")
# conn_HAIG.commit()

# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

## function to transform Geometry from text to LINESTRING
def wkb_tranformation(line):
   return wkb.loads(line.geom, hex=True)



######-----------------------------------------------######
#### define distance between two tuples of coordinates ####
######-----------------------------------------------######

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


"""

## get all terminals corresponding to 'cars' and 'fleet' (from routecheck_2019)
ID_vehicles = pd.read_sql_query('''
               SELECT idterm, vehtype
               FROM public.routecheck
               /*WHERE vehtype = '1'*/
               ''', conn_HAIG)
# make an unique list
idterms = list(ID_vehicles.idterm.unique())
os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')
## save 'all_ID_TRACKS' as list
with open("idterms_2019.txt", "w") as file:
    file.write(str(idterms))
    
"""

# ## reload all 'idterms' as list
with open("D:/ENEA_CAS_WORK/ROMA_2019/idterms_2019.txt", "r") as file:
   idterms = eval(file.readline())


# with open("D:/ENEA_CAS_WORK/ROMA_2019/idterms_2019_new.txt", "r") as file:
#   idterms = eval(file.readline())


## --> check this anomaly (speeds are all zeros and start trip was wrong, panel wrong)
# idterm = '5403701'
# idtrajectory = 102279375


# idterm = '5438102'
# idtrajectory = 89701185


# idterm = '3218952'
# idtrajectory = 52275542


# idterm = '5420595'
# idtrajectory = 89013622
# idtrajectory = 81474972

# idterm = '4197670'
# idtrajectory = 27763107
# idtrajectory = 32173142


## previously filtered "negative" incremente"
## filter "progressive < 0

## progressive start too high fro the next trip
# idterm = "3555410"
# idtrajectory =28469578

## progressive start too high
# idterm = "4278482"
# idtrajectory = 61282760

## prgressive never reset for vehtype  == car
# idterm = "4416327"
# idtrajectory = 49363380

## vehicle type == 2 (fleet).... next progressive too too high (outlier)
# idterm = "2750102"
# idtrajectory = 114686843

## vehicle type == 2 (fleet).... next progressive too too low (outlier)
# idterm  = "2740445"
# idtrajectory = 121512767

## bias very large (on-off in-between two different far aways days)  (vehtype == 2)
# idterm = "7200969"
# idtrajectory = 112444841

## bias very large (on-off in-between two different far aways days)  (vehtype == 2)
# idterm = "2749321"
# idtrajectory = 121497577

## trip on the border
# idterm = "4414079"
# idtrajectory = 44602132


def func(arg):
    last_idterm_idx, idterm = arg

# for last_track_idx, idterm in enumerate(idterms_cars):
    print(idterm)
    idterm = str(idterm)
    # print('VIASAT GPS track:', track_ID)
    viasat_data = pd.read_sql_query('''
                SELECT * FROM public.routecheck 
                WHERE idterm = '%s' ''' % idterm, conn_HAIG)
    if len(viasat_data) > 0:
        ## sort data by increasing timedate
        viasat_data = viasat_data.sort_values('timedate')
        ## round the filed "progressive" down to nearest 100
        viasat_data['progressive'] = (viasat_data['progressive'] / 100).astype(int) * 100
        ## add a field with the "NEXT timedate" in seconds
        viasat_data['next_totalseconds'] = viasat_data.totalseconds.shift(-1)
        viasat_data['next_timedate'] = viasat_data.timedate.shift(-1)
        viasat_data['next_totalseconds'] = viasat_data['next_totalseconds'].astype('Int64')
        viasat_data['next_totalseconds'] = viasat_data['next_totalseconds'].fillna(0)
        viasat_data['next_lon'] = viasat_data.longitude.shift(-1)  # longitude of the next trip
        viasat_data['next_lat'] = viasat_data.latitude.shift(-1)  # latitude of the next trip
        ## trip distance in meters (sum of the increment of the "progressive"
        ## add a field with the "previous progressive"
        viasat_data['last_progressive'] = viasat_data.progressive.shift()  # <-------
        viasat_data['last_progressive'] = viasat_data['last_progressive'].astype('Int64')
        viasat_data['last_progressive'] = viasat_data['last_progressive'].fillna(0)
        ## add a field with the next progressive
        viasat_data['next_progressive'] = viasat_data.progressive.shift(-1)
        viasat_data['next_progressive'] = viasat_data['next_progressive'].astype('Int64')
        viasat_data['next_progressive'] = viasat_data['next_progressive'].fillna(0)
        ## add filed with next "idtrajectory"
        viasat_data['next_idtrajectory'] = viasat_data.idtrajectory.shift(-1)
        viasat_data['next_idtrajectory'] = viasat_data['next_idtrajectory'].astype('Int64')
        all_trips = list(viasat_data.idtrajectory.unique())
        ### initialize an empty dataframe
        # route_ROMA = pd.DataFrame([])
        for idx, idtrajectory in enumerate(all_trips):

            # print(idtrajectory)
            ## filter data by idterm and by idtrajectory (trip)
            data = viasat_data[viasat_data.idtrajectory == idtrajectory]

            ## group by TRIP_ID, check numbers of line, if > 1 then only get the one with larger number of lines
            counts_TRIP_ID = data.groupby(data[['TRIP_ID']].columns.tolist(),
                                                        sort=False).size().reset_index().rename(columns={0: 'counts'})
            data = data[data.TRIP_ID == counts_TRIP_ID[counts_TRIP_ID.counts == max(counts_TRIP_ID.counts)].TRIP_ID[0]]
            ### zip the coordinates into a point object and convert to a GeoData Frame ####
            if len(data) > 1:
                ## find outliers
                # q = data["progressive"].quantile(0.99)
                # data = data[data["progressive"] <= q]
                ## find threshold level for speed for speed
                # q_speed = data["speed"].quantile(0.99)
                ## process data only if instant speeed is at least of 10 km/h
                if ( (len(data) > 1) ):
                    geometry = [Point(xy) for xy in zip(data.longitude, data.latitude)]
                    df = GeoDataFrame(data, geometry=geometry)
                    # Aggregate these points with the GroupBy
                    df = df.groupby(['idtrajectory'])['geometry'].apply(lambda x: LineString(x.tolist()))
                    df = GeoDataFrame(df, geometry='geometry')
                    # df.plot()
                    df.columns = ['geometry']
                    idtrace_o = data[data.segment == min(data.segment)][['id']].iloc[0][0]
                    idtrace_d = data[data.segment == max(data.segment)][['id']].iloc[0][0]
                    # latitude_o = data[data.segment == min(data.segment)][['latitude']].iloc[0][0]  ## at the ORIGIN
                    # longitude_o = data[data.segment == min(data.segment)][['longitude']].iloc[0][0]  ## at the ORIGIN
                    # latitude_d = data[data.segment == max(data.segment)][['latitude']].iloc[0][0]  ## at the DESTINATION
                    # longitude_d = data[data.segment == max(data.segment)][['longitude']].iloc[0][0]  ## at the DESTINATION
                    timedate_o = data[data.segment == min(data.segment)][['timedate']].iloc[0][0]  ## at the ORIGIN
                    ## compute increments of the distance (in meters)
                    data['increment'] = data.progressive - data.last_progressive

                    ### ---->>>> moved into the routecheck algorithm......

                    """
                    if (sum(data['progressive']) > 0):
                        try:
                            ## ------>>> remove rows with wrong progressive and therefore timedate  <<<<<----------
                            idx_to_remove = data['increment'][data.increment < 0].index
                            ## check the position of the negative increment within the dataframe
                            new_data = data.reset_index(drop=True)
                            if ( (new_data['increment'][new_data.increment < 0].index[0] == len(data)-1)  or
                                    (new_data['increment'][new_data.increment < 0].index[0] == len(data)-2) or
                                     (new_data['increment'][new_data.increment < 0].index[0] == len(data)-3) or
                                      (new_data['increment'][new_data.increment < 0].index[0] == len(data)-4) or
                                       (new_data['increment'][new_data.increment < 0].index[0] == len(data) -5)
                            ):
                                data = data
                            else:
                                list_idx_to_remove = list( range(data.index[0], idx_to_remove[0]+1) )
                                data.drop(list_idx_to_remove, axis=0, inplace=True)
                        except IndexError:
                            pass
                    """

                    ## check if idterm is assigned to a fleet and always set starting increment to 0
                    if data['vehtype'].iloc[0] == 2:
                        data['increment'].iloc[0] = 0
                    ## check if ANY data['increment'] is < 0
                    # increment = pd.DataFrame(data.increment >= 0)
                    ## check if not all values are TRUE
                    # if not increment['increment'].all():
                    ## only consider poitive increments of the distance...if so, then sum all the increments (skip the first element ?)
                    tripdistance_m = sum(data['increment'][1:len(data['increment'])][data.increment > 0])
                    ## trip time in seconds (duration)
                    time_o = data[data.segment == min(data.segment)][['path_time']].iloc[0][0]
                    time_d = data[data.segment == max(data.segment)][['path_time']].iloc[0][0]
                    triptime_s = time_d - time_o
                    checkcode = data[data.segment == min(data.segment)][['anomaly']].iloc[0][0] ## at the ORIGIN
                    border_flag = data[data.segment == min(data.segment)][['border']].iloc[0][0]  ## at the ORIGIN
                    ## intervallo di tempo tra un l'inizio di due viaggi successivi
                    breaktime_s = data[data.segment == max(data.segment)][['next_timedate']].iloc[0][0] - \
                                  data[data.segment == max(data.segment)][['timedate']].iloc[0][0]
                    breaktime_s = breaktime_s.total_seconds()
                    if breaktime_s < 0:
                        breaktime_s = None
                    ### get distance between the position of two consecutive TRIPS (from END of a TRIP to START of a NEW TRIP)
                    lon_end = data[data.segment == max(data.segment)][['longitude']].iloc[0][0]  # longitude at the END of a TRIP
                    lat_end = data[data.segment == max(data.segment)][['latitude']].iloc[0][0]
                    lon_start = data[data.segment == max(data.segment)][['next_lon']].iloc[0][0]  # longitude at the START of a NEW TRIP
                    lat_start = data[data.segment == max(data.segment)][['next_lat']].iloc[0][0]
                    ### find distance between coordinates of two consecutive TRIPS in METERS!!!
                    deviation_pos = great_circle_track_node(lon_end, lat_end, lon_start, lat_start)
                    # calculate mean speed during the trip
                    # speed_trip = (tripdistance_m / 1000) / (triptime_s / 3600)  ## km/h
                    speed_trip = round ((tripdistance_m / triptime_s)*3.6 , 1)  ## km/h
                    diff_last_next_progr = abs(data[data.segment == max(data.segment)][['next_progressive']].iloc[0][0] - \
                                                           data[data.segment == max(data.segment)][['progressive']].iloc[0][0])
                    ##---->>>> only consider "diff_last_next_progr" < 500000 meters <<<<------
                    # if ( diff_last_next_progr < 500000 ):
                    print("speed trip (km/h):", speed_trip, "-------", "triptime (min):", triptime_s/60, "-------", "trip distance (km):", tripdistance_m/1000, "-------", "ID trajectory:",  idtrajectory,"-------", "idterm:", idterm,
                          "-------", "diff_progr:", diff_last_next_progr,
                          "-------", "bias(meters):", deviation_pos)

                    ### build the final dataframe ("route" table)
                    if tripdistance_m > 0 and triptime_s <= 25200 and breaktime_s > 60 and triptime_s > 0 and speed_trip > 1 and np.mean(data.speed) > 0:    ## <------
                        df_ROUTE = pd.DataFrame({'idtrajectory': [idtrajectory],
                                                 'idterm': [idterm],
                                                 'idtrace_o': [idtrace_o],
                                                 'idtrace_d': [idtrace_d],
                                                 # 'latitude_o': [latitude_o],
                                                 # 'longitude_o': [longitude_o],
                                                 # 'latitude_d': [latitude_d],
                                                 # 'longitude_d': [longitude_d],
                                                 'timedate_o': [timedate_o],
                                                 'tripdistance_m': [tripdistance_m],
                                                 'triptime_s': [triptime_s],
                                                 'checkcode': [checkcode],
                                                 'breaktime_s': [breaktime_s],
                                                 'border_flag': [border_flag]})
                        geom = df['geometry'].apply(wkb_hexer)
                        df_ROUTE['geom'] = geom.iloc[0]
                        df_ROUTE['deviation_pos_m'] = deviation_pos
                        df_ROUTE['diff_last_next_progr'] = diff_last_next_progr
                        # route_ROMA = route_ROMA.append(df_ROUTE)

                        try:
                            connection = engine.connect()
                            df_ROUTE.to_sql("route", con=connection, schema="public",
                                               if_exists='append')
                            connection.close()
                        except exc.OperationalError:
                            print('OperationalError')
                            connection = engine.connect()
                            df_ROUTE.to_sql("route", con=connection, schema="public",
                                            if_exists='append')
                            connection.close()


################################################
##### run all script using multiprocessing #####
################################################

## check how many processer we have available:
# print("available processors:", mp.cpu_count())

if __name__ == '__main__':
    # pool = mp.Pool(processes=mp.cpu_count()) ## use all available processors
    pool = mp.Pool(processes=50)     ## use 60 processors
    print("++++++++++++++++ POOL +++++++++++++++++", pool)
    results = pool.map(func, [(last_idterm_idx, idterm) for last_idterm_idx, idterm in enumerate(idterms)])
    pool.close()
    pool.close()
    pool.join()


#####################################################
##### convert "geometry" field into LINESTRING ######
#####################################################

'''
## Convert the `'geom'` column back to Geometry datatype, from text

with engine.connect() as conn, conn.begin():
    print(conn)
    sql = """ALTER TABLE public."route"
                                  ALTER COLUMN geom TYPE Geometry(LINESTRING, 4326)
                                    USING ST_SetSRID(geom::Geometry, 4326)"""
    conn.execute(sql)

'''

###################################################
###################################################
###################################################
###################################################
###################################################
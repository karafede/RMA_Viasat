

#### from Gaetano Valenti
#### modified by Federico Karagulian

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
import kmeans_clusters
import dbscan_clusters
import db_connect

## Connect to an existing database
conn_HAIG = db_connect.connect_HAIG_Viasat_RM_2019()
cur_HAIG = conn_HAIG.cursor()

# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/HAIG_Viasat_RM_2019')


from sklearn.metrics import silhouette_score
from sklearn.datasets import load_iris
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from scipy.spatial.distance import cdist
import math


gid= []
longitude =[]
latitude =[]
density=[]
p1 =[]
sep=","

#for numZone in range(3400, 4001, 200):
for numZone in range(800, 1000, 200):

    print("zoning: "+str(numZone))
    #Query to create db_residencePY
    cur_HAIG.execute("DROP TABLE IF EXISTS zones.zone_py_temp CASCADE")
    cur_HAIG.execute("CREATE  TABLE zones.zone_py_temp "
    "(gid integer  NOT NULL, "
    " zone integer  NOT NULL)");


    #Query the database to obtain the list of idterm
    cur_HAIG.execute("SELECT gid, CASE WHEN p1 is null THEN 0 ELSE p1 END, ST_X(ST_Transform(ST_PointOnSurface(geom), 32632)), "
                "ST_Y(ST_Transform(ST_PointOnSurface(geom), 32632)), "
                "CASE WHEN p1 is null THEN 0 ELSE p1/(ST_Area(ST_Transform(geom, 32632))/1000000) END "
                "FROM zones.istat_indicatori_sezcenc_prov_rm_map;")
    records = cur_HAIG.fetchall()
    i=0
    for row in records:
        gid.append(row[0])
        longitude.append(
            row[2])
        latitude.append(row[3])
        p1.append(row[1])
        density.append(1+int(row[4]))
        print(int(row[4]))
        print(i, gid[i], longitude[i], latitude[i], density[i])
        i=i+1

    X = np.array(list(zip(longitude, latitude)))
    dens=np.array(density)
    model = KMeans(n_clusters=numZone).fit(X, sample_weight=dens)
    #model = KMeans(n_clusters=numZone).fit(X)
    labels = model.labels_
    #print(len(labels))
    for lab in range(len(labels)):
        #print(lab, labels[lab], gid[lab])
        input = "(" + str(gid[lab]) + sep + str(labels[lab])+")";
        #print(input)
        cur_HAIG.execute("INSERT INTO zones.zone_py_temp (gid, zone)" + " VALUES " + input + "");
    #plt.scatter(X[:, 0], X[:, 1], c=labels, s=50, cmap='viridis');
    print("creating "+str(numZone)+" zone table.....")
    cur_HAIG.execute("DROP TABLE IF EXISTS zones.zone"+str(numZone)+"_py; "
                "CREATE TABLE zones.zone"+str(numZone)+"_py AS ( "
                "SELECT c.zone, sum(c.p1) as p1, sum(c.a2) as a2, sum(c.a6) as a6, sum(c.a7) as a7, sum(c.pf1) as pf1, sum(c.e1) as e1, sum(c.e3) as e3, "
                "(sum(c.p1)/ST_Area(ST_Union(ST_Transform(c.geom, 32632)))*(1000*1000)) ::integer as density, "
                "ST_Area(ST_Union(ST_Transform(c.geom, 32632)))::integer as area_m2, ST_Multi(ST_Union(c.geom)):: Geometry(MULTIPOLYGON, 4326) as geom "
                "FROM ( SELECT a.*, b.zone "
                "FROM zones.istat_indicatori_sezcenc_prov_rm_map a "
                "inner join zones.zone_py_temp b on a.gid=b.gid) c "
                "GROUP BY c.zone "
                "ORDER BY c.zone); "
                "DROP TABLE IF EXISTS zones.zone_py_temp;");
    conn_HAIG.commit()
# Close communication with the database
cur_HAIG.close()
conn_HAIG.close()

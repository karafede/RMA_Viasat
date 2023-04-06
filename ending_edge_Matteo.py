

import os

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
from shapely import geometry
from shapely.geometry import Point, Polygon
from geoalchemy2 import Geometry, WKTElement
import geopy.distance
import csv

os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')
os.getcwd()
## load grafo
file_graphml = 'Roma__Italy_70km.graphml'
grafo_ALL = ox.load_graphml(file_graphml)
## ox.plot_graph(grafo_ALL)
## get edges and node
gdf_nodes_ALL, gdf_edges_ALL = ox.graph_to_gdfs(grafo_ALL)
# gdf_edges_ALL.plot()

## import your data dataframe with ID veicolo, latitude, longitude
your_data = pd.read_csv("your_data.csv")

## get an area around your data in order to make a subset of the grafo_ALL
ext = 0.030  ## these are degrees
## top-right corner
p1 = Point(np.min(your_data.longitude) - ext, np.min(your_data.latitude) - ext)
## bottom-right corner
p2 = Point(np.max(your_data.longitude) + ext, np.min(your_data.latitude) - ext)
## bottom-left corner
p3 = Point(np.max(your_data.longitude) + ext, np.max(your_data.latitude) + ext)
## top-left corner
p4 = Point(np.min(your_data.longitude) - ext, np.max(your_data.latitude) + ext)

# Initialize a GeoDataFrame where geometry is a list of points
your_data_extent = gpd.GeoDataFrame([['box', p1],
                                  ['box', p2],
                                  ['box', p3],
                                  ['box', p4]],
                                 columns=['shape_id', 'geometry'],
                                 geometry='geometry')

# Extract the coordinates from the Point object
your_data_extent['geometry'] = your_data_extent['geometry'].apply(lambda x: x.coords[0])
# Group by shape ID
#  1. Get all of the coordinates for that ID as a list
#  2. Convert that list to a Polygon
your_data_extent = your_data_extent.groupby('shape_id')['geometry'].apply(lambda x: Polygon(x.tolist())).reset_index()
# Declare the result as a new a GeoDataFrame
your_data_extent = gpd.GeoDataFrame(your_data_extent, geometry='geometry')
# viasat_extent.plot()

# reset indices
your_data.reset_index(drop=True, inplace=True)

# create an index column
your_data["ID"] = your_data.index

# for polygon extent of your_data, find intersecting nodes then induce a subgraph
for polygon in your_data_extent['geometry']:
    intersecting_nodes = gdf_nodes_ALL[gdf_nodes_ALL.intersects(polygon)].index
    grafo = grafo_ALL.subgraph(intersecting_nodes)
    # fig, ax = ox.plot_graph(grafo)
## get geodataframes for edges and nodes from the subgraph
## get graph only within the extension of the rectangular polygon
if len(grafo) > 0:
    gdf_nodes, gdf_edges = ox.graph_to_gdfs(grafo)

## get nearest node to the GPS track along the same edge
edge_df = pd.DataFrame([])
for i in range(len(your_data)):
    lat = float(your_data.latitude[i])
    lon = float(your_data.longitude[i])
    point = (lat, lon)
    ## get geometry and the extremes (nodes) of the edge crossed by your point
    geom, u, v = ox.get_nearest_edge(grafo, point)
    edge = [(u,v), point]
    edge = pd.DataFrame(edge, columns=['u', 'v', 'latitude', 'longitude'])
    ## store data into a dataframe
    edge_df = edge_df.append(edge)

## merge your dataframe of edges with all the information about the road
edge_df = pd.merge(edge_df, gdf_edges, on=['u', 'v'], how='left')
## save data.....



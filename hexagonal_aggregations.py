

import os
os.chdir('D:\\Federico\\Mobility')
os.getcwd()

import pandas as pd
import geopandas as gpd
import folium
import db_connect
import numpy as np
from shapely.geometry import Point, Polygon
from geopandas import GeoDataFrame
from shapely import wkb
from osgeo import ogr
import sqlalchemy as sal
from sqlalchemy.pool import NullPool
import fiona
from shapely.geometry import shape
from pyproj import Transformer

print(pd.__version__)
print(gpd.__version__)
# pip install geopandas==0.10.2
### python version (3.8.1)
# tin in terminal ----> python -V

import pyproj
print(pyproj.__version__)
## pyproj directory is (----> returns the valid data directory)
proj_data_dir = pyproj.datadir.get_data_dir()
# pyproj.datadir.set_data_dir(proj_data_dir)


# connect to ROMA DB
conn_HAIG = db_connect.connect_HAIG_ROMA()
cur_HAIG = conn_HAIG.cursor()
# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.1.0.1:5432/HAIG_ROMA', poolclass=NullPool)


# Function to generate WKB hex
def wkb_hexer(line):
    return line.wkb_hex

## function to transform Geometry from text to LINESTRING
def wkb_tranformation(line):
   return wkb.loads(line.geometry, hex=True)


## load new GG elaboration
most_visited_location = pd.read_csv("D:/Federico/Mobility/platform_mobility/home_locations_GG.csv")
# rename columns
most_visited_location.rename({'latitude': 'lat'}, axis=1, inplace=True)
most_visited_location.rename({'longitude': 'lng'}, axis=1, inplace=True)

#################-------------------------------------------------- ##############################################
##### build the HEXAGNAL TESSELLATION over ROMA  (MOST VISITED LOCATIONS) ####################################

# define distance between two coordinates
def haversine(coord1, coord2):
    # Coordinates in decimal degrees (e.g. 43.60, -79.49)
    lon1, lat1 = coord1
    lon2, lat2 = coord2
    R = 6371000  # radius of Earth in meters
    phi_1 = np.radians(lat1)
    phi_2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    a = np.sin(delta_phi / 2.0) ** 2 + np.cos(phi_1) * np.cos(phi_2) * np.sin(delta_lambda / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    meters = R * c  # output distance in meters
    km = meters / 1000.0  # output distance in kilometers
    meters = round(meters)
    km = round(km, 3)
    # print(f"Distance: {meters} m")
    # print(f"Distance: {km} km")
    return meters


# define the extension of the Trajectory file
xmin = min(most_visited_location.lng)
xmax = max(most_visited_location.lng)
ymin = min(most_visited_location.lat)
ymax = max(most_visited_location.lat)

EW = haversine((xmin, ymin), (xmax, ymin))
NS = haversine((xmin, ymin), (xmin, ymax))
# diamter of each hexagon in the grid = 600 metres
d = 600
# horizontal width of hexagon = w = d* np.sin(60)
w = d * np.sin(np.pi / 3)
# Approximate number of hexagons per row = EW/w
n_cols = int(EW / w) + 1
# Approximate number of hexagons per column = NS/d
n_rows = int(NS / d) + 10

# Make a hexagonal grid to cover the entire area
from matplotlib.patches import RegularPolygon

w = (xmax - xmin) / n_cols  # width of hexagon
d = w / np.sin(np.pi / 3)  # diameter of hexagon
array_of_hexes = []
for rows in range(0, n_rows):
    hcoord = np.arange(xmin, xmax, w) + (rows % 2) * w / 2
    vcoord = [ymax - rows * d * 0.75] * n_cols
    for x, y in zip(hcoord, vcoord):  # , colors):
        hexes = RegularPolygon((x, y), numVertices=6, radius=d / 2, alpha=0.2, edgecolor='k')
        verts = hexes.get_path().vertices
        trans = hexes.get_patch_transform()
        points = trans.transform(verts)
        array_of_hexes.append(Polygon(points))


# hex_grid_ROMA = gpd.GeoDataFrame({'geometry': array_of_hexes}, crs="EPSG:4326")  #CRS:3857
hex_grid_ROMA = gpd.GeoDataFrame({'geometry': array_of_hexes}, crs="EPSG:4326")  #CRS:32632
# hex_grid_ROMA = gpd.GeoDataFrame({'geometry': array_of_hexes}, crs="EPSG:3857")
# hex_grid_ROMA = hex_grid_ROMA.to_crs('epsg:6347')

hex_grid_ROMA.plot()
## save geojson file of the hexagon tessellation
hex_grid_ROMA['index_hex'] = hex_grid_ROMA.index
hex_grid_ROMA.to_file(filename='hex_grid_600m_ROMA.geojson', driver='GeoJSON')



#################################################################################
#################################################################################
# create basemap ROMA
ave_LAT = 41.90368331095105
ave_LON = 12.487932714627279
my_map = folium.Map([ave_LAT, ave_LON], zoom_start=11, tiles='OpenStreetMap')
#################################################################################

## transform the "home locations" table into a geodataframe
geometry = [Point(xy) for xy in zip(most_visited_location.lng, most_visited_location.lat)]
geo_tdf = GeoDataFrame(most_visited_location, geometry=geometry, crs="EPSG:3857")   #CRS:3857
# geo_tdf = GeoDataFrame(most_visited_location, geometry=geometry, crs="EPSG:4326")   #CRS:3857



# get hexagons containing the home locations (gdf_tdf)
tdf_hex = gpd.sjoin(hex_grid_ROMA, geo_tdf, how='right', predicate='contains') ### <---- OK
df_tdf_hex = pd.DataFrame(tdf_hex)
del(tdf_hex)
## remove NA values
df_tdf_hex = df_tdf_hex[df_tdf_hex['index_left'].notna()]
df_tdf_hex['index_left'] = df_tdf_hex.index_left.astype('int')
## counts how many 'index_left" occur...the frequency (basically.....how many home in each hexagon)
hex_counts = df_tdf_hex[['index_left']].groupby(df_tdf_hex[['index_left']].columns.tolist(), sort=False).size().reset_index().rename(columns={0:'counts_hex'})
hex_counts['overall_counts'] = hex_counts['counts_hex']


hex_counts.rename({'index_left': 'index_hex'}, axis=1, inplace=True)
hex_counts = pd.merge(hex_grid_ROMA, hex_counts[['index_hex', 'overall_counts']], on=['index_hex'], how='right')


## save geojson file of the hexagon corresponding to home locations
## change crs....
# hex_counts = hex_counts.set_crs(3857, allow_override=True)
# hex_counts = hex_counts.set_crs(3035, allow_override=True)
hex_counts.crs
hex_counts.to_file(filename='hex_counts_600m_ROMA.geojson', driver='GeoJSON')
hex_counts.plot()
AAA = gpd.read_file('hex_counts_600m_ROMA.geojson')
AAA.plot()

"""
style_hex = {'fillColor': '#00000000', 'color': '#00FFFFFF'}
style_tdf_hex = {'fillColor': '#00000000', 'color': '#ff0000'}
style_hex_counts = {'fillColor': '#00000000', 'color': '#ff0000'}


###############################################################################################
#####---- make colored hexagons for most frequent home locations --------------------##########

## rescale all data by an arbitrary number
hex_counts["scales"] = round(((hex_counts.overall_counts/max(hex_counts.overall_counts)) * 3) + 0.1 ,4)
AAA = hex_counts.sort_values(by=['overall_counts'])
AAA = pd.DataFrame(AAA)


import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

vmin = min(hex_counts.scales)  
vmax = max(hex_counts.scales)
# Try to map values to colors in hex
norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
mapper = plt.cm.ScalarMappable(norm=norm, cmap=plt.cm.cool)  # scales of Reds (or "coolwarm" , "bwr", "cool", YlOrRd)  gist_yarg --> grey to black
hex_counts['colors'] = hex_counts['scales'].apply(lambda x: mcolors.to_hex(mapper.to_rgba(x)))



folium.GeoJson(
hex_counts[['overall_counts', 'colors', 'scales', 'geometry']].to_json(),
    style_function=lambda x: {
        'fillColor': x['properties']['colors'],
        'color': x['properties']['colors'],
        'weight':  0.4,
        'fillOpacity': 0.4,
        },
highlight_function=lambda x: {'weight':1,
        'color':'blue',
        'fillOpacity':0.1
    },
    # fields to show
    tooltip=folium.features.GeoJsonTooltip(
        fields=['overall_counts']),
    ).add_to(my_map)


folium.TileLayer('cartodbdark_matter').add_to(my_map)
folium.LayerControl().add_to(my_map)
my_map.save("hex_counts_600m_ROMA.html")

"""

######## ------------------------- ###################################################################
#####################################################################################
#################################################################################
###############################################################################
############################################################################


### ------>>> add POPULATION COUNTS to the tessellation according to the city councils......
### read geojson file of Zone Censuarie in Roma (population data from ISTAT)
zone_censuarie_ROMA_2011 = gpd.read_file("D:/Federico/Mobility/population_2020/zone_censuarie_ROMA_2011.geojson")
regione_LAZIO_2022 = gpd.read_file("D:/Federico/Mobility/population_2020/regione_LAZIO_2022.geojson")
# regione_LAZIO_2022.plot()

# zone_censuarie_ROMA_2011.plot()
## combine hexagonal cells with ZONE CENSUARIE
## reprojection geodataframe zone_censuarie_ROMA_2011 from  EPSG:32632 into EPSG:4326
zone_censuarie_ROMA_2011 = zone_censuarie_ROMA_2011.to_crs(4326)

## read population data and merge into sections
population_data_2011 = pd.read_csv('D:/Federico/Mobility/population_2020/R12_indicatori_2011_sezioni.csv', sep = ';')
## select field of interest
population_data_2011 = population_data_2011[['CODREG', 'CODPRO', 'PROVINCIA', 'CODCOM', 'COMUNE',
       'PROCOM', 'SEZ2011', 'NSEZ', 'P1', 'A2', 'A6', 'A7', 'PF1', 'E1', 'E3']]
zone_censuarie_ROMA_2011 = zone_censuarie_ROMA_2011[['COD_REG', 'COD_ISTAT', 'PRO_COM', 'SEZ2011', 'SEZ', 'ACE',
       'Shape_Leng', 'Shape_Area', 'geometry']]
## merge data on the common field "SEZ2011"
zone_censuarie_ROMA_2011 = pd.merge(zone_censuarie_ROMA_2011, population_data_2011, on=['SEZ2011'], how='left')
## get only data for the province of ROMA
zone_censuarie_ROMA_2011 = zone_censuarie_ROMA_2011[zone_censuarie_ROMA_2011['PROVINCIA'] == 'Roma']
# zone_censuarie_ROMA_2011.plot()


##### ------- proceed with the intersection of zone censuali into the hexagons ----- #################
## reload hexagonal tesselation over ROMA
hex_grid_ROMA = gpd.read_file("D:/Federico/Mobility/hex_grid_600m_ROMA.geojson")
#### --->>> crop the hexagonal grid over the Regione Lazio
hex_grid_ROMA = gpd.overlay(hex_grid_ROMA, regione_LAZIO_2022, how='intersection')
# hex_grid_ROMA.plot()


## save clipped hexagonal grid:
## make a column with the index of the hexagons
hex_grid_ROMA.to_file(filename='hex_grid_ROMA_600m.geojson', driver='GeoJSON')

zone_censuarie_ROMA_2011.reset_index(inplace=True)
zone_censuarie_ROMA_2011.rename({'index': 'index_cens'}, axis=1, inplace=True)
zone_censuarie_ROMA_2011.to_file(filename='zone_censuarie_ROMA_2011.geojson', driver='GeoJSON')


## load hexagonal grid and zone censuarie only for the Province of ROMA
hex_ROMA = fiona.open('D:/Federico/Mobility/hex_grid_ROMA_600m.geojson')
cens_ROMA = fiona.open('D:/Federico/Mobility/zone_censuarie_ROMA_2011.geojson')


##### ----> aggregate data into each hexagon
geom_hex = [ shape(feat["geometry"]) for feat in hex_ROMA ]
geom_cens = [ shape(feat["geometry"]) for feat in cens_ROMA ]

############################################################################################################
###  --- >>>> find the percentage of intersection of each "zona censuale" with each hexagon <<<----##########

## initialize an empty list
intersections_table = []
for i, gcens in enumerate(geom_cens):
    for j, ghex in enumerate(geom_hex):
        if gcens.intersects(ghex):
            perc = (gcens.intersection(ghex).area/gcens.area)*100
            print(i, j, (gcens.intersection(ghex).area/gcens.area)*100)
            df_intersection = pd.DataFrame({'index_cens': [i],
                                     'index_hex': [j],
                                     'percentage': [perc]})
            intersections_table.append(df_intersection)

df_intersection = pd.concat(intersections_table)


## Save data to .csv file
df_intersection.to_csv('percentages_intersections_hex_zone_censusarie_ROMA.csv')
# df_intersection = pd.read_csv('percentages_intersections_hex_zone_censusarie_ROMA.csv')

## merge intersection percentages with censuary data
zone_censuarie_ROMA_2011 = gpd.read_file(filename='zone_censuarie_ROMA_2011.geojson', driver='GeoJSON')
# zone_censuarie_ROMA_2011.plot()
df_intersection = pd.merge(df_intersection, zone_censuarie_ROMA_2011, on=['index_cens'], how='left')
df_intersection = df_intersection[df_intersection['P1'].notna()]   ## P1 ---> population number
## compute the effective contribution of Population number falling in each hexagon from each censuary zone
df_intersection['POP_TOT_HEX'] = round(((df_intersection.percentage)*(df_intersection.P1))/100, 0)

df_intersection = df_intersection.sort_values('index_hex', ascending=False)


## aggregate all population number within the same hexagonal cell:
aggregated_hex = df_intersection[['index_hex', 'POP_TOT_HEX']].groupby(['index_hex'], as_index=False).agg('sum')
# aggregated_hex = aggregated_hex.sort_values('index_cens', ascending=False)
## make a column with the index of the hexagons
# aggregated_hex.reset_index(inplace=True)
aggregated_hex['POP_TOT_HEX'] = aggregated_hex.POP_TOT_HEX.astype('int')

aggregated_hex = pd.merge(hex_counts, aggregated_hex, on=['index_hex'], how='left')

# aggregated_hex.plot()
aggregated_hex = gpd.GeoDataFrame(aggregated_hex)
aggregated_hex = aggregated_hex.to_crs(epsg=4326)
# AAA = pd.DataFrame(aggregated_hex)
# AAA = AAA.sort_values('overall_counts', ascending=False)
aggregated_hex['importance'] = round((aggregated_hex['overall_counts'] / max(aggregated_hex.overall_counts))*100, 1)

## save aggregated data into a .geojson file
aggregated_hex.to_file(filename='platform_mobility/mycomponent_tessellation/hexagons_aggr_POP_ROMA_2011.geojson', driver='GeoJSON')

## add "var name" in front of the .geojson file, in order to properly loat it into the index.html file
with open("platform_mobility/mycomponent_tessellation/hexagons_aggr_POP_ROMA_2011.geojson", "r+") as f:
    old = f.read()  # read everything in the file
    f.seek(0)  # rewind
    f.write("var hex_counts = \n" + old)  # assign the "var name" in the .geojson file


aggregated_hex.to_file(filename='platform_mobility/hexagons_aggr_POP_ROMA_2011.geojson', driver='GeoJSON')
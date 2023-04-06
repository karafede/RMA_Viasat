

import os
import glob
import pandas as pd
import db_connect
import sqlalchemy as sal
import csv
import psycopg2
os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')
cwd = os.getcwd()

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_ROMA()
cur_HAIG = conn_HAIG.cursor()


##########################################################
### Check mapmatching DB #################################
##########################################################

#### check how many TRIP ID we have ######################

# get all ID terminal of Viasat data
all_VIASAT_TRIP_IDs = pd.read_sql_query(
    ''' SELECT DISTINCT "TRIP_ID" 
        FROM public.mapmatching_2019 ''', conn_HAIG)


###################################################################
###### only for 09 October 2019 ###################################
# all_VIASAT_TRIP_IDs = pd.read_sql_query(
#     ''' SELECT "TRIP_ID"
#         FROM public.mapmatching_new ''', conn_HAIG)

###### only for 09 October 2019 ###################################
###################################################################
# ## get all ID terminal of Viasat data  (from routecheck_2019)
# all_VIASAT_IDterminals = pd.read_sql_query(
#     ''' SELECT "idterm"
#         FROM public.routecheck
#         WHERE date(routecheck.timedate) = '2019-10-09'  ''', conn_HAIG)
# ## make a list of all IDterminals (GPS ID of Viasata data) each ID terminal (track) represent a distinct vehicle
# all_ID_TRACKS = list(all_VIASAT_IDterminals.idterm.unique())
# ## all_ID_TRACKS = [int(i) for i in all_ID_TRACKS]
# ## save 'all_ID_TRACKS' as list
# with open("all_ID_TRACKS_09_October_2019.txt", "w") as file:
#     file.write(str(all_ID_TRACKS))


# make a list of all unique trips
all_TRIP_IDs = list(all_VIASAT_TRIP_IDs.TRIP_ID.unique())

print("all matched records:", len(all_VIASAT_TRIP_IDs))
print("trip number:", len(all_TRIP_IDs))

## get all terminals (unique number of vehicles)
idterm = list((all_VIASAT_TRIP_IDs.TRIP_ID.str.split('_', expand=True)[0]).unique())
## make all elements floats
idterm = [int(i) for i in idterm]
print("vehicle number:", len(idterm))

## reload 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/ROMA_2019/all_ID_TRACKS_2019.txt", "r") as file:
    all_ID_TRACKS = eval(file.readline())
###### only for 09 October 2019 ###################################
# with open("D:/ENEA_CAS_WORK/ROMA_2019/all_ID_TRACKS_09_October_2019.txt", "r") as file:
#     all_ID_TRACKS = eval(file.readline())
print(len(all_ID_TRACKS))
## make difference between all idterm and matched idterms lists
all_ID_TRACKS_DIFF = list(set(all_ID_TRACKS) - set(idterm))
print(len(all_ID_TRACKS_DIFF))
# ## save 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/ROMA_2019/all_ID_TRACKS_2019_new.txt", "w") as file:
    file.write(str(all_ID_TRACKS_DIFF))
# with open("D:/ENEA_CAS_WORK/ROMA_2019/all_ID_TRACKS_09_October_2019_new.txt", "w") as file:
#     file.write(str(all_ID_TRACKS_DIFF))



##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
### intial number of TRIPS from routecheck_2019 on 09 October 2019 ###########################
all_VIASAT_TRIP_routecheck = pd.read_sql_query(
     ''' SELECT "TRIP_ID"
         FROM public.routecheck
         WHERE date(routecheck.timedate) = '2019-10-09'  ''', conn_HAIG)
## make difference between al TRIP_ID from routechek and matched TRIP_IDs #####################
all_VIASAT_TRIP_routecheck =  list(all_VIASAT_TRIP_routecheck.TRIP_ID.unique())
DIFF_TRIPS = list(set(all_VIASAT_TRIP_routecheck) - set(all_TRIP_IDs))
print("DIFF number of TRIPS:", len(DIFF_TRIPS))
print("unmatched trips:", (len(DIFF_TRIPS) / len(all_VIASAT_TRIP_routecheck) )*100, "%", sep='')


######################################
######################################
######################################
### check the size of a table ########
######################################

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('public.mapmatching_all') )''', conn_HAIG)


#### check size of the tables

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('dataraw') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('routecheck_2019') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('public."OSM_edges"') )''', conn_HAIG)

pd.read_sql_query('''
SELECT pg_size_pretty( pg_relation_size('public.idterm_portata') )''', conn_HAIG)

### check the size of the WHOLE DB "HAIG_Viasat_SA"
pd.read_sql_query('''
SELECT pg_size_pretty( pg_database_size('HAIG_Viasat_RM_2019') )''', conn_HAIG)


####################################################################################################
####################################################################################################
################################################################################
### Check accuracy_2019 on the DB from ROMA #################################
#################################################################

#### check how many TRIP ID we have ######################

# get all ID terminal of Viasat data
all_VIASAT_TRIP_IDs = pd.read_sql_query(
    ''' SELECT "TRIP_ID" 
        FROM public.accuracy_2019 ''', conn_HAIG)

# make a list of all unique trips
all_TRIP_IDs = list(all_VIASAT_TRIP_IDs.TRIP_ID.unique())

print(len(all_VIASAT_TRIP_IDs))
print("trip number:", len(all_TRIP_IDs))

## get all terminals (unique number of vehicles)
idterm = list((all_VIASAT_TRIP_IDs.TRIP_ID.str.split('_', expand=True)[0]).unique())
print("vehicle number:", len(idterm))


## reload 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/ROMA_2019/all_ID_TRACKS_2019.txt", "r") as file:
    all_ID_TRACKS = eval(file.readline())
print(len(all_ID_TRACKS))
## make difference between all idterm and matched idterms
all_ID_TRACKS_DIFF = list(set(all_ID_TRACKS) - set(idterm))
print(len(all_ID_TRACKS_DIFF))
# ## save 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/ROMA_2019/all_ID_TRACKS_2019_new.txt", "w") as file:
    file.write(str(all_ID_TRACKS_DIFF))


################################################################
##### some checks on FCD data and trips ########################

idterm = '2704220'
idterm = '2750102'

viasat_dataraw = pd.read_sql_query('''
            SELECT * FROM public.dataraw 
            WHERE idterm = '%s' ''' % idterm, conn_HAIG)
viasat_dataraw.drop_duplicates(['latitude', 'longitude'], inplace=True)
# sort data by timedate:
viasat_dataraw['timedate'] = viasat_dataraw['timedate'].astype('datetime64[ns]')
# sort by 'timedate'
viasat_dataraw = viasat_dataraw.sort_values('timedate')


viasat_data = pd.read_sql_query('''
            SELECT * FROM public.routecheck 
            WHERE idterm = '%s' ''' % idterm, conn_HAIG)
viasat_data.drop_duplicates(['latitude', 'longitude'], inplace=True)
# sort data by timedate:
viasat_data['timedate'] = viasat_data['timedate'].astype('datetime64[ns]')
# sort by 'timedate'
viasat_data = viasat_data.sort_values('timedate')
trips = list(viasat_data.idtrajectory.unique())



route = pd.read_sql_query('''
            SELECT * FROM public.route
            WHERE idterm = '%s' ''' % idterm, conn_HAIG)

# route = pd.read_sql_query('''
#             SELECT * FROM public.route
#             WHERE idterm = '%s'
#             AND idtrajectory = '123275963'  ''' % idterm, conn_HAIG)


############################################################################
############################################################################
#########------------------------------------------- #######################
#########--------------------------------------------#######################
############################################################################
############################################################################
############################################################################
#######----------------------------------------------#######################
### GET all IDs of ELECTIC VEHICLES ########################################
############################################################################
############################################################################
############################################################################


# connect to new DB to be populated with Viasat data after route-check
# conn_HAIG = db_connect.connect_HAIG_BRESCIA()
conn_HAIG = db_connect.connect_HAIG_ROMA()
# conn_HAIG = db_connect.connect_HAIG_CATANIA()
# conn_HAIG = db_connect.connect_HAIG_SALERNO()
cur_HAIG = conn_HAIG.cursor()



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
AAA = pd.read_sql_query('''
                        SELECT
                        idterm, idvehcategory, brand, anno                    
                        FROM obu
                        WHERE idvehcategory ILIKE 'e%' 
                        ''', conn_HAIG)


# get all ID terminal of Viasat data
electric_veh = pd.read_sql_query('''
                        SELECT
                        idterm, idvehcategory, brand, anno                      
                        FROM obu
                        WHERE idvehcategory ILIKE ANY(ARRAY['ELE%', 'leaf%', 'zoe%', 'IPACE%', 'model%'])
                        ''', conn_HAIG)


electric_veh = pd.concat([electric_veh, I3_BMW])



# get all ID terminal of Viasat data
idterms = pd.read_sql_query(
    ''' SELECT DISTINCT idterm
        FROM public.mapmatching ''', conn_HAIG)

### transform into integers
idterms['idterm'] = idterms['idterm'].astype('int')
### check if IDs of Electriv vehicles are within the map-matched vehicles
matched_IDTERMS = electric_veh[electric_veh.idterm.isin(list(pd.to_numeric(idterms.idterm)))]


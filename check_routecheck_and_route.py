
import os
import glob
import pandas as pd
import db_connect
import sqlalchemy as sal
import csv
import psycopg2

os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')

# connect to new DB to be populated with Viasat data after route-check
conn_HAIG = db_connect.connect_HAIG_ROMA()
cur_HAIG = conn_HAIG.cursor()


##########################################################
### Check routecheck DB ##################################
##########################################################

#### check how many TRIP ID we have ######################

# get all ID terminal of Viasat data
idterm = pd.read_sql_query(
    ''' SELECT DISTINCT "idterm" 
        FROM public.routecheck_trenta_bis ''', conn_HAIG)

# make a list of all unique trips
processed_idterms = list(idterm.idterm.unique())
## transform all elements of the list into integers
processed_idterms = list(map(int, processed_idterms))
print(len(processed_idterms))

## reload 'all_idterms' as list
with open("D:/ENEA_CAS_WORK/ROMA_2019/all_idterms.txt", "r") as file:
    all_ID_TRACKS = eval(file.readline())
print(len(all_ID_TRACKS))
## make difference between all idterm and processed idterms

all_ID_TRACKS_DIFF = list(set(all_ID_TRACKS) - set(processed_idterms))
print(len(all_ID_TRACKS_DIFF))

# ## save 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/ROMA_2019/all_idterms_new.txt", "w") as file:
    file.write(str(all_ID_TRACKS_DIFF))




# get all ID terminal of Viasat data
idtrajectory = pd.read_sql_query(
    ''' SELECT DISTINCT "idtrajectory" 
        FROM public.routecheck_trenta ''', conn_HAIG)

# make a list of all unique trips
processed_idtrajectory = list(idtrajectory.idtrajectory.unique())
## transform all elements of the list into integers
processed_idtrajectory = list(map(int, processed_idtrajectory))
print(len(processed_idtrajectory))

##########################################################
### Check route DB #######################################
##########################################################

#### check how many TRIP ID we have ######################

# get all ID terminal of Viasat data
idterm = pd.read_sql_query(
    ''' SELECT DISTINCT "idterm" 
        FROM public.route_cinque ''', conn_HAIG)

# make a list of all unique trips
processed_idterms = list(idterm.idterm.unique())
## transform all elements of the list into integers
processed_idterms = list(map(int, processed_idterms))
print(len(processed_idterms))

## reload 'all_idterms' as list
with open("D:/ENEA_CAS_WORK/ROMA_2019/idterms_2019.txt", "r") as file:
    all_ID_TRACKS = eval(file.readline())
print(len(all_ID_TRACKS))
## make difference between all idterm and processed idterms

all_ID_TRACKS_DIFF = list(set(all_ID_TRACKS) - set(processed_idterms))
print(len(all_ID_TRACKS_DIFF))

# ## save 'all_ID_TRACKS' as list
with open("D:/ENEA_CAS_WORK/ROMA_2019/idterms_2019_new.txt", "w") as file:
    file.write(str(all_ID_TRACKS_DIFF))


# get all ID terminal of Viasat data
idtrajectory = pd.read_sql_query(
    ''' SELECT DISTINCT "idtrajectory" 
        FROM public.route_trenta ''', conn_HAIG)

# make a list of all unique trips
processed_idtrajectory = list(idtrajectory.idtrajectory.unique())
## transform all elements of the list into integers
processed_idtrajectory = list(map(int, processed_idtrajectory))
print(len(processed_idtrajectory))




##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################


#### check idtrajectory corssing ara joust one time.....
# get all ID terminal of Viasat data
idtrajectory_routes = pd.read_sql_query(
    ''' SELECT idterm, idtrajectory
        FROM public.route_trenta ''', conn_HAIG)


## make join with dataraw and group by.......
all_routes_idterm = pd.read_sql_query('''
                       SELECT  
                        route_trenta.idtrajectory, route_trenta.idterm,
                        count (*)
                          FROM public.route_trenta
                          LEFT JOIN dataraw 
                                      ON route_trenta.idterm =  dataraw.idterm
                                       group by route_trenta.idtrajectory, route_trenta.idterm  
                                       /*limit 100000*/
                       ''', conn_HAIG)


## sort data
all_routes_idterm.sort_values(['idterm', 'count'], ascending=True, inplace=True)
## remove rows with same "idterm" and "count".. and keep only the one that APPEAR ONCE!!
# ghost_routes = all_routes_idterm.drop_duplicates(subset=['count', 'idterm'])
ghost_routes = all_routes_idterm.drop_duplicates(subset=['idterm'])
## percentages of ghosts users among the total number of trips
percentage_ghost_users = (len(ghost_routes)/len(all_routes_idterm))*100

all_routes_idterm_duplicate = all_routes_idterm

##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################

all_routes_idterm_bis = pd.read_sql_query('''
                       SELECT  
                        route_trenta.idtrajectory, route_trenta.idterm
                          FROM public.route_trenta
                          LEFT JOIN dataraw 
                                      ON route_trenta.idterm =  dataraw.idterm
                                       group by route_trenta.idtrajectory , route_trenta.idterm
                                        /*limit 100000*/
                       ''', conn_HAIG)

all_routes_idterm_bis.sort_values(['idterm'], ascending=True, inplace=True)


### get counts of terminals (idterms/users) referred to each trip
group = all_routes_idterm_bis.groupby(['idterm']).size()
group = pd.DataFrame(group)
group.reset_index(inplace=True)
group.rename({0: 'counts'}, axis=1, inplace=True)

group.sort_values(['counts'], ascending=True, inplace=True)
# make distribution...

#

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

fig = plt.hist(group['counts'], bins= 300, range=[0, 300])
plt.title('Distribution of Counts')
plt.xlabel("counts")
plt.ylabel("Frequency")
plt.savefig("Distribution_counts_route_trenta.png")
plt.close()



filtered_groups_lessthan_one = group[group.counts ==1 ]   ## two ways (trip in & trip out)
(len(filtered_groups_lessthan_one) ) /len(group)*100


filtered_groups_lessthan_one = group[group.counts >1 ]
fig = plt.hist(filtered_groups_lessthan_one['counts'], bins= 50, range=[0, 300])
plt.title('Distribution of Counts')
plt.xlabel("counts")
plt.ylabel("Frequency (idterm <=1)")
plt.savefig("Distribution_counts_route_trenta_less_than_one.png")
plt.close()

filtered_groups_lessthan_two = group[group.counts ==2 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_two) ) /len(group)*100


filtered_groups_lessthan_two = group[group.counts >=2 ]
fig = plt.hist(filtered_groups_lessthan_two['counts'], bins= 50, range=[0, 300])
plt.title('Distribution of Counts')
plt.xlabel("counts")
plt.ylabel("Frequency (idterm <=2)")
plt.savefig("Distribution_counts_route_trenta_less_than_two.png")
plt.close()

filtered_groups_lessthan_three = group[group.counts ==3 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_three) ) /len(group)*100


filtered_groups_lessthan_three = group[group.counts >3 ]
fig = plt.hist(filtered_groups_lessthan_three['counts'], bins= 50, range=[0, 300])
plt.title('Distribution of Counts')
plt.xlabel("counts")
plt.ylabel("Frequency (idterm <=3)")
plt.savefig("Distribution_counts_route_trenta_less_than_three.png")
plt.close()



filtered_groups_lessthan_four = group[group.counts ==4 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_four) ) /len(group)*100

filtered_groups_lessthan_four = group[group.counts >4 ]
fig = plt.hist(filtered_groups_lessthan_four['counts'], bins= 50, range=[0, 300])
plt.title('Distribution of Counts')
plt.xlabel("counts")
plt.ylabel("Frequency (idterm <=4)")
plt.savefig("Distribution_counts_route_trenta_less_than_four.png")
plt.close()

filtered_groups_lessthan_five = group[group.counts ==5 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_five) ) /len(group)*100

filtered_groups_lessthan_six = group[group.counts ==6 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_six) ) /len(group)*100


filtered_groups_lessthan_seven = group[group.counts ==7 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_seven) ) /len(group)*100


filtered_groups_lessthan_10 = group[group.counts ==10 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_10) ) /len(group)*100


filtered_groups_lessthan_20 = group[group.counts ==20 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_20) ) /len(group)*100

filtered_groups_lessthan_30 = group[group.counts ==30 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_30) ) /len(group)*100

filtered_groups_lessthan_50 = group[group.counts ==50 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_50) ) /len(group)*100

filtered_groups_lessthan_70 = group[group.counts ==70 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_70) ) /len(group)*100

filtered_groups_lessthan_75 = group[group.counts ==75 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_75) ) /len(group)*100


filtered_groups_lessthan_100 = group[group.counts ==100 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_100) ) /len(group)*100

filtered_groups_lessthan_120 = group[group.counts ==120 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_120) ) /len(group)*100


filtered_groups_lessthan_150 = group[group.counts ==150 ]   ## two ways (trip in & trip out
(len(filtered_groups_lessthan_150) ) /len(group)*100


##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################

"""
routes_idterm = pd.read_sql_query('''
                       SELECT  
                        route_trenta.idtrajectory, route_trenta.idterm
                          FROM public.route_trenta
                          LEFT JOIN dataraw 
                                      ON route_trenta.idterm =  dataraw.idterm
                                      limit 10000
                       ''', conn_HAIG)



AAA = routes_idterm.groupby(['idtrajectory']).size()
AAA = pd.DataFrame(AAA)
AAA.reset_index(inplace=True)
AAA.rename({0: 'counts'}, axis=1, inplace=True)
"""







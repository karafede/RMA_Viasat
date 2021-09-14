


import os
import pandas as pd
import db_connect
import sqlalchemy as sal
import csv
import psycopg2


# connect to new DB with name "HAIG_ROMA" to be populated with data
conn_HAIG = db_connect.connect_HAIG_ROMA()
cur_HAIG = conn_HAIG.cursor()


### erase existing table...if exists....
# cur_HAIG.execute("DROP TABLE IF EXISTS route CASCADE")
# conn_HAIG.commit()



### also create this kind of connection to the DB to load data into the DB
engine = sal.create_engine('postgresql://postgres:superuser@10.1.0.1:5432/HAIG_ROMA', poolclass=NullPool)

## df_route = table created with pandas
df_route = 999999


### connect to the DB and upload your table (the whole table)
connection = engine.connect()
df_route.to_sql("routecheck", con=connection, schema="public",
                   if_exists='append')
connection.close()


### read data from the DB
viasat_data = pd.read_sql_query('''
                    SELECT  
                       route.u, route.v,
                            route.timedate, route.mean_speed, 
                            route.idtrace, route.sequenza,
                            route.idtrajectory,
                       FROM route
                       WHERE date(mapmatching_2019.timedate) = '2019-11-21'
                       /*WHERE EXTRACT(MONTH FROM mapmatching_2019.timedate) = '02'*/
                       /*AND dataraw.vehtype::bigint = 1*/
                       /*LIMIT 10000*/
                    ''', conn_HAIG)

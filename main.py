
import os
import pandas as pd
from load_DB import obu
from load_DB import upload_DB
from load_DB import idterm_vehtype_portata
from routecheck_viasat_ROMA_FK import func
import glob
import db_connect
import sqlalchemy as sal
import multiprocessing as mp

os.chdir('D:/ViaSat/VIASAT_RM/obu')
cwd = os.getcwd()
## load OBU data (idterm, vehicle type and other metadata)
obu_CSV = "VST_ENEA_ROMA_ANAG_20191209.csv"

### upload Viasat data for into the DB (only dataraw, NOT OBU data!!!)
extension = 'csv'
os.chdir('D:/ViaSat/VIASAT_RM')
viasat_filenames = glob.glob('*.{}'.format(extension))

# connect to new DB to be populated with Viasat data
conn_HAIG = db_connect.connect_HAIG_Viasat_RM_2019()
cur_HAIG = conn_HAIG.cursor()


#########################################################################################
### upload OBU data into the DB. Create table with idterm, vehicle type and put into a DB

obu(obu_CSV)

### upload viasat data into the DB  # long time run...
upload_DB(viasat_filenames)


###########################################################
### ADD a SEQUENTIAL ID to the dataraw table ##############
###########################################################

# long time run...
## create a consecutive ID for each row
cur_HAIG.execute("""
alter table "dataraw" add id serial PRIMARY KEY
     """)
conn_HAIG.commit()

#### add an index to the "idterm"

cur_HAIG.execute("""
CREATE index dataraw_idterm_idx on public.dataraw(idterm);
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index dataraw_timedate_idx on public.dataraw(timedate);
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index dataraw_vehtype_idx on public.dataraw(vehtype);
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index dataraw_id_idx on public.dataraw("id");
""")
conn_HAIG.commit()

########################################################################################
#### create table with 'idterm', 'vehtype' and 'portata' and load into the DB ##########

idterm_vehtype_portata()   # long time run...

#########################################################################################
##### create table routecheck ###########################################################

## multiprocess.....
os.chdir('D:/ENEA_CAS_WORK/ROMA_2019')
os.getcwd()

# Create an SQL connection engine to the output DB
engine = sal.create_engine('postgresql://postgres:superuser@10.0.0.1:5432/HAIG_Viasat_RM_2019')

# get all ID terminal of Viasat data
all_VIASAT_IDterminals = pd.read_sql_query(
    ''' SELECT *
        FROM public.obu''', conn_HAIG)
all_VIASAT_IDterminals['idterm'] = all_VIASAT_IDterminals['idterm'].astype('Int64')
all_VIASAT_IDterminals['anno'] = all_VIASAT_IDterminals['anno'].astype('Int64')
all_VIASAT_IDterminals['portata'] = all_VIASAT_IDterminals['portata'].astype('Int64')

# make a list of all IDterminals (GPS ID of Viasata data) each ID terminal (track) represent a distinct vehicle
all_ID_TRACKS = list(all_VIASAT_IDterminals.idterm.unique())


## pool = mp.Pool(processes=mp.cpu_count()) ## use all available processors
pool = mp.Pool(processes=10)     ## use 55 processors
print("++++++++++++++++ POOL +++++++++++++++++", pool)
## use the function "func" defined in "routecheck_viasat_ROMA_FK.py" to run multitocessing...
results = pool.map(func, [(last_track_idx, track_ID) for last_track_idx, track_ID in enumerate(all_ID_TRACKS)])
pool.close()
pool.join()

'''
### to terminate multiprocessing
pool.terminate()
'''


## add indices ######

cur_HAIG.execute("""
CREATE index routecheck_2019_id_idx on public.routecheck_2019("id");
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2019_idterm_idx on public.routecheck_2019("idterm");
""")
conn_HAIG.commit()



cur_HAIG.execute("""
CREATE index routecheck_2019_TRIP_ID_idx on public.routecheck_2019("TRIP_ID");
""")
conn_HAIG.commit()



cur_HAIG.execute("""
CREATE index routecheck_2019_timedate_idx on public.routecheck_2019("timedate");
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2019_grade_idx on public.routecheck_2019("grade");
""")
conn_HAIG.commit()



cur_HAIG.execute("""
CREATE index routecheck_2019_anomaly_idx on public.routecheck_2019("anomaly");
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2019_speed_idx on public.routecheck_2019("speed");
""")
conn_HAIG.commit()



cur_HAIG.execute("""
CREATE index routecheck_2019_lat_idx on public.routecheck_2019(latitude);
""")
conn_HAIG.commit()


cur_HAIG.execute("""
CREATE index routecheck_2019_lon_idx on public.routecheck_2019(longitude);
""")
conn_HAIG.commit()


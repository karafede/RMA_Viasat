
import os
from load_DB import obu
from load_DB import upload_DB
from load_DB import idterm_vehtype_portata
import glob
import db_connect

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


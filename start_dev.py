import glob
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
print(sys.path)
import sysvars
import datetime
from subprocess import *
from time import sleep
print(os.getcwd())
import argparse
from datetime import date,datetime
from AlertCypher import AlertCypher
from gard.methods import get_node_counts

def check_update(db_type):
    db = AlertCypher('system')
    today = datetime.now()

    config_selection = {'ct':['clinical_update', 'ct_interval'], 'pm':['pubmed_update', 'pm_interval'], 'gnt':['grant_update', 'gnt_interval']}
    selection = config_selection[db_type]

    last_update = db.getConf('DATABASE',selection[0])
    last_update = datetime.strptime(last_update,"%m/%d/%y")

    delta = today - last_update
    interval = db.getConf('DATABASE',selection[1])
    interval = int(interval)

    last_update = datetime.strftime(last_update,"%m/%d/%y")

    if delta.days > interval:
        return [True,last_update]
    else:
        return [False,last_update]

while True:
    current_updates = {k:False for k,v in sysvars.db_abbrevs.items()}
    
    print('Checking for Updates')
    for db_abbrev in sysvars.db_abbrevs:
        current_updates[db_abbrev] = check_update(db_abbrev)[0]

    print('Triggering Database Updates')
    for k,v in current_updates.items():
        if v == True:
            full_db_name = sysvars.db_abbrevs[k]
            print(f'{full_db_name} Update Initiated',k,v)
            
            p = Popen(['python3', 'driver_manual.py', '-db', f'{k}', '-m', 'update'], encoding='utf8')
            p.wait()
            
            print('Updating Node Counts on GARD db')
            get_node_counts()

            db = AlertCypher('system')
            db.setConf('DATABASE', f'{k}_update', datetime.strftime(datetime.now(),"%m/%d/%y"))

            print('Dumping GARD db')
            p = Popen(['python3', 'generate_dump.py', '-dir', 'gard', '-t'], encoding='utf8')
            p.wait()

            print(f'Dumping {full_db_name} db')
            p = Popen(['python3', 'generate_dump.py', '-dir', f'{full_db_name}', '-t'], encoding='utf8')
            p.wait()

            print(f'Transfering GARD dump to TEST server')
            p = Popen(['python3', 'file_transfer.py', f'-dir gard', '-s test'], encoding='utf8')
            p.wait()

            print(f'Transfering {full_db_name} dump to TEST server')
            p = Popen(['python3', 'file_transfer.py', f'-dir {full_db_name}', '-s test'], encoding='utf8')
            p.wait()

            print(f'Update of {full_db_name} Database Complete...')
            
    sleep(3600)



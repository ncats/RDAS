import glob
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append('/home/leadmandj/RDAS/')
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
from RDAS_GARD.methods import get_node_counts
import firebase_admin
from firebase_admin import auth
from firebase_admin import credentials
from firebase_admin import firestore

prefix=sysvars.db_prefix
config_selection = {'ct':[prefix+'rdas.ctkg_update', 'ct_interval'], 'pm':[prefix+'rdas.pakg_update', 'pm_interval'], 'gnt':[prefix+'rdas.gfkg_update', 'gnt_interval']}

# this line below no longer needed because the neo4j databases has change their names.
# config_selection = {'ct':['clinical_update', 'ct_interval'], 'pm':['pubmed_update', 'pm_interval'], 'gnt':['grant_update', 'gnt_interval']}


def check_update(db, db_type):
    """
    Checks if an update is needed for a specified database based on the configured update interval.

    Parameters:
    - database_abbreviation (str): Abbreviation for the database type (e.g., 'ct', 'pm', 'gnt').

    Returns:
    - bool: True if an update is needed, False otherwise.
    """

    # Get the current date and time
    today = datetime.now()

    selection = config_selection[db_type]
    print("selection::",selection)

    # Get the last update date from the configuration
    last_update = db.getConf('DATABASE',selection[0])
    last_update = datetime.strptime(last_update,"%m/%d/%y")
    print("last_update::",last_update)

    # Calculate the time difference between today and the last update
    delta = today - last_update

    interval = db.getConf('DATABASE',selection[1])
    interval = int(interval)
    print("interval::",interval)

    # Get the update interval from the configuration
    last_update = datetime.strftime(last_update,"%m/%d/%y")

    # Check if an update is needed based on the interval
    if delta.days > interval:
        return [True,last_update]
    else:
        return [False,last_update]

# Connect to the system database
db = AlertCypher('system')
cred = credentials.Certificate(sysvars.firebase_key_path)
firebase_admin.initialize_app(cred)
firestore_db = firestore.client()


while True:
    # Initialize a dictionary to track update status for each database
    current_updates = {k:False for k,v in sysvars.db_abbrevs.items()}
    # print("\n","current_updates::", current_updates)
    print('Checking for Updates')
    # Check update status for each database
    for db_abbrev in sysvars.db_abbrevs:
        current_updates[db_abbrev] = check_update(db, db_abbrev)[0]

    print('Triggering Database Updates')
    today = datetime.now()
    has_updates={}
    for k,v in current_updates.items():
        print("updates:::",k,v)
        if v[0] == True:
            last_update=v[1]
            print("check_last_updates::", last_update)
            full_db_name = sysvars.db_abbrevs[k]
            print(f'{full_db_name} Update Initiated')
            
            has_updates[full_db_name]=True
            
            p = Popen(['python3', 'driver_manual.py', '-db', f'{config_selection[k]}', '-m', 'update'], encoding='utf8')
            p.wait()
            
            # Update the node counts on the GARD Neo4j database (numbers used to display on the UI)
            print('Updating Node Counts on GARD db')
            get_node_counts()
            
            target_address = sysvars.rdas_urls['dev']
            db.run(f'STOP DATABASE gard')
            p = Popen(['ssh', '-i', f'~/.ssh/id_rsa', f'{sysvars.current_user}@{target_address}', 'python3', '~/RDAS/remote_dump_and_transfer.py' ' -dir', 'gard'], encoding='utf8')
            p.wait()
            db.run(f'START DATABASE gard')

            db.run(f'STOP DATABASE {full_db_name}')
            p = Popen(['ssh', '-i', f'~/.ssh/id_rsa', f'{sysvars.current_user}@{target_address}', 'python3', '~/RDAS/remote_dump_and_transfer.py' ' -dir', f'{full_db_name}'], encoding='utf8')
            p.wait()
            db.run(f'START DATABASE {full_db_name}')

            # Creates a backup file for the current state of the GARD database, puts that file in the transfer directory
            print('Dumping GARD db')
            p = Popen(['python3', 'generate_dump.py', '-dir', 'gard', '-t'], encoding='utf8')
            p.wait()
            
            # Creates a backup file for the current state of the database being updated in this iteration, puts that file in the transfer directory
            print(f'Dumping {full_db_name} db')
            p = Popen(['sudo', 'python3', 'generate_dump.py', f'-dir {full_db_name}', '-b', '-t', '-s dev'], encoding='utf8')
            p.wait()

            print(f'Transfering GARD dump to TEST server')
            p = Popen(['sudo', 'python3', 'file_transfer.py', f'-dir {full_db_name}', '-s test'], encoding='utf8')
            p.wait()

            # Transfers the current databases of this iteration's backup file to the Testing Server's transfer folder
            print(f'Transfering {full_db_name} dump to TEST server')
            p = Popen(['sudo', 'python3', 'file_transfer.py', f'-dir {full_db_name}', '-s test'], encoding='utf8')
            p.wait()
            
            print(f'Update of {full_db_name} Database Complete...')

    if True in has_updates.values():
        ses_firebase.trigger_email(firestore_db,has_updates, date_start=last_update,date_end=datetime.strftime(today,"%m/%d/%y")) 
    
    print('database update and email sending has finished')
    sleep(3600)



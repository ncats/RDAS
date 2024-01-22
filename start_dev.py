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
    """
    Checks if an update is needed for a specified database based on the configured update interval.

    Parameters:
    - database_abbreviation (str): Abbreviation for the database type (e.g., 'ct', 'pm', 'gnt').

    Returns:
    - bool: True if an update is needed, False otherwise.
    """

    # Connect to the system database
    db = AlertCypher('system')

    # Get the current date and time
    today = datetime.now()

    # Mapping of database abbreviations to configuration fields
    config_selection = {'ct':['clinical_update', 'ct_interval'], 'pm':['pubmed_update', 'pm_interval'], 'gnt':['grant_update', 'gnt_interval']}
    selection = config_selection[db_type]

    # Get the last update date from the configuration
    last_update = db.getConf('DATABASE',selection[0])
    last_update = datetime.strptime(last_update,"%m/%d/%y")

    # Calculate the time difference between today and the last update
    delta = today - last_update

    interval = db.getConf('DATABASE',selection[1])
    interval = int(interval)

    # Get the update interval from the configuration
    last_update = datetime.strftime(last_update,"%m/%d/%y")

    # Check if an update is needed based on the interval
    if delta.days > interval:
        return [True,last_update]
    else:
        return [False,last_update]




while True:
    # Initialize a dictionary to track update status for each database
    current_updates = {k:False for k,v in sysvars.db_abbrevs.items()}
    
    print('Checking for Updates')
    # Check update status for each database
    for db_abbrev in sysvars.db_abbrevs:
        current_updates[db_abbrev] = check_update(db_abbrev)[0]

    print('Triggering Database Updates')
    # Trigger updates for databases that require it
    for k,v in current_updates.items():
        if v == True:
            full_db_name = sysvars.db_abbrevs[k]
            print(f'{full_db_name} Update Initiated',k,v)
            
            # Execute manual update script for the database
            p = Popen(['python3', 'driver_manual.py', '-db', f'{k}', '-m', 'update'], encoding='utf8')
            p.wait()
            
            # Update the node counts on the GARD Neo4j database (numbers used to display on the UI)
            print('Updating Node Counts on GARD db')
            get_node_counts()

            # Update last update date in the system database configuration
            db = AlertCypher('system')
            db.setConf('DATABASE', f'{full_db_name}_update', datetime.strftime(datetime.now(),"%m/%d/%y"))

            # Creates a backup file for the current state of the GARD database, puts that file in the transfer directory
            print('Dumping GARD db')
            p = Popen(['python3', 'generate_dump.py', '-dir', 'gard', '-t'], encoding='utf8')
            p.wait()

            # Creates a backup file for the current state of the database being updated in this iteration, puts that file in the transfer directory
            print(f'Dumping {full_db_name} db')
            p = Popen(['python3', 'generate_dump.py', '-dir', f'{full_db_name}', '-t'], encoding='utf8')
            p.wait()

            # Transfers the GARD backup file to the Testing Server's transfer folder
            print(f'Transfering GARD dump to TEST server')
            p = Popen(['python3', 'file_transfer.py', '-dir', 'gard', '-s', 'test'], encoding='utf8')
            p.wait()

            # Transfers the current databases of this iteration's backup file to the Testing Server's transfer folder
            print(f'Transfering {full_db_name} dump to TEST server')
            p = Popen(['python3', 'file_transfer.py', '-dir', f'{full_db_name}', '-s', 'test'], encoding='utf8')
            p.wait()

            print(f'Update of {full_db_name} Database Complete...')

    # Sleep for an hour before checking for updates again          
    sleep(3600)



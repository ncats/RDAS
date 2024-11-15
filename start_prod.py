import glob
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
import sysvars
import datetime
from AlertCypher import AlertCypher
from subprocess import *
from time import sleep
import argparse
from RDAS_MEMGRAPH_APP.Alert import Alert
from RDAS_MEMGRAPH_APP.Transfer import Transfer
from RDAS_MEMGRAPH_APP.Update import Update
from datetime import datetime

# Initialize modules for production environment and database operations
email_client = Alert()
transfer_module = Transfer('prod')
update_module = Update(mode=sysvars.gard_db)
db = AlertCypher('system')
init = True # Flag to skip initial updates when the script starts

# Main loop to continuously monitor for updates
while True:
    try:
        print('checking for update...')
        # Detect new database dump files in the transfer path
        transfer_detection,last_updates = transfer_module.detect(sysvars.transfer_path)
        new_dumps = transfer_detection

        # Skip sending emails or loading dumps upon the initial script start
        if init:
            init = False
            continue

        # Process each new dump file detected
        for db_name in new_dumps:
            db_single = AlertCypher(db_name) # Create a database-specific Cypher interface

            print('update found::', db_name)
            # Fetch the last update timestamp for the database
            last_update_obj = datetime.fromtimestamp(float(last_updates[db_name]))
            print('starting database loading')
            # Load the dump file into the seed cluster
            transfer_module.seed(db_name, sysvars.transfer_path)

            # Perform operations if the database has been successfully seeded
            if transfer_module.get_isSeeded():
                # Remove all nodes labeled `UserTesting` from the database
                db_single.run('MATCH (x:UserTesting) DETACH DELETE x') # Removes UserTesting Node
                
                print('starting email service')
                 # Trigger email notifications with details about the update
                email_client.trigger_email([db_name], date_start=datetime.strftime(last_update_obj, "%m/%d/%y"))

                # Reset the seeded flag for future updates
                transfer_module.set_isSeeded(False)

                # Refresh node counts in the database after the update
                update_module.refresh_node_counts()

        # Wait for a minute before checking for updates again
        sleep(60)
    except Exception as e:
        print(e)



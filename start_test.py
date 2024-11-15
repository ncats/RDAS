import glob
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
import sysvars
from datetime import datetime
from AlertCypher import AlertCypher
from subprocess import *
from time import sleep
import argparse
from RDAS_MEMGRAPH_APP.Alert import Alert
from RDAS_MEMGRAPH_APP.Transfer import Transfer
from RDAS_MEMGRAPH_APP.Dump import Dump

"""
This script constantly checks the test server for two conditions:
1. Whether a dump file was sent to the test server's transfer folder.
2. Whether a dump file was sent to the approved folder.
"""

# Initialize email client, transfer module, and dump module for test environment
email_client = Alert('test')
transfer_module = Transfer('test')
dump_module = Dump('test')
# Fetch email recipients and current date
recip = sysvars.contacts['test']
today_str = datetime.today().strftime('%m/%d/%y')

# Detect initial state in transfer and approved folders
transfer_detection,lastupdates = transfer_module.detect(sysvars.transfer_path)
transfer_detection,lastupdates = transfer_module.detect(sysvars.approved_path)

# Main loop to continuously monitor and process files
while True:
    print('[RDAS] Checking for new database files in the transfer folder')
    # Detect new dump files in the test server's transfer folder
    transfer_detection,lastupdates = transfer_module.detect(sysvars.transfer_path)
    new_dumps = transfer_detection

    # Process each detected dump file
    for db_name in new_dumps:
        try:
            # Seed the dump file into the cluster
            transfer_module.seed(db_name,sysvars.transfer_path)

            # If seeding is successful, notify stakeholders via email
            if transfer_module.get_isSeeded():
                print('database seeded within cluster')
            
                # Email notification setup
                sub = '[RDAS] ACTION REQUIRED - New Dump Uploaded to Test Server'
                msg = f'New dump uploaded to test for database {db_name}'
                html = f'''<p>A new dump file has been uploaded to the test databases</p>
                    <p>database effected: {db_name}</p>
                    <p>To approve the database to be transfered to production, log in to the databases browser and select the effected database</p>
                    <p>Run the following Cypher Query:</p>
                    <p>MATCH (x:UserTesting) SET x.Approved = \"True\"</p>'''
                email_client.send_email(sub,html,recip)
                print(f'Notification emails sent to {recip}')

                # Reset seeded flag
                transfer_module.set_isSeeded(False)

        except Exception as e:
            print(e)

    # Wait before next check
    sleep(15)
    print('[RDAS] Checking Neo4j for recently approved databases')

    # Check for databases marked as approved in Neo4j
    for db_name in sysvars.dump_dirs:
        db = AlertCypher(db_name)
        try:
            try:
                # Query the approval status of the database
                update = db.run('MATCH (x:UserTesting) RETURN x.Approved as update').data()[0]['update']
            except Exception:
                # Handle case where UserTesting node doesn't exist
                print(f'{db_name}:: False [Non-existent UserTesting Node]')
                continue

            if update == True:
                # Process approved database
                print(f'Detected Approved Database for {db_name}... sending to approved folder')
                dump_module.dump_file(sysvars.approved_path, db_name)
                # Update approval flag and log the date in Neo4j
                db.run(f'MATCH (x:UserTesting) SET x.Approved = False, x.LastApprovedRDAS = \"{today_str}\"')
                # Adjust file permissions
                p = Popen(['sudo', 'chmod', '777', f'{sysvars.approved_path}{db_name}.dump'], encoding='utf8')
                p.wait()

        except Exception as e:
            print(e)

    sleep(15)
    print('[RDAS] Checking approved folder for recently approved dump files')

    # Detect newly approved dump files in the approved folder
    transfer_detection,lastupdates = transfer_module.detect(sysvars.approved_path)
    new_dumps = transfer_detection

    print(new_dumps)

    # Process each approved dump file
    for db_name in new_dumps:
        print(f'Update approved for {db_name}... Sending to PROD')
        db = AlertCypher(db_name)
        
        # Transfer the approved dump file to the production server
        send_url = sysvars.rdas_urls['prod']
        p = Popen(['scp', f'{sysvars.approved_path}{db_name}.dump', f'{sysvars.current_user}@{send_url}:{sysvars.transfer_path}{db_name}.dump'], encoding='utf8')
        p.wait()

        # Notify stakeholders of the transfer
        sub = '[RDAS] NOTICE - Test Database Approved'
        msg = f'{db_name} database was approved to move to production'
        html = f'''<p>A dump file on the test server has been approved to move to production</p>
                    <p>Database effected: {db_name}</p>
                    <p>There are no other actions required on your end</p>'''
        email_client.send_email(sub,html,recip)
        print(f'Notification emails sent to {recip}')

    # Waits one minute before restarting checks
    sleep(15)


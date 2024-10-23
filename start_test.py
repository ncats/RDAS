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
This script constantly checks the test server for 2 things: whether a dump file was sent to the test server,
or if a dump file was sent to the approved folder.
The file sent TO the test server will be sent manually while the approval file will be sent automatically via a script (start_approval.py)
"""

email_client = Alert('test')
transfer_module = Transfer('test')
dump_module = Dump('test')
recip = sysvars.contacts['test']
today_str = datetime.today().strftime('%m/%d/%y')

# INIT
transfer_detection,lastupdates = transfer_module.detect(sysvars.transfer_path)
transfer_detection,lastupdates = transfer_module.detect(sysvars.approved_path)

while True:
    print('[RDAS] Checking for new database files in the transfer folder')
    # Detects all new dump files in the transfer folder of the TEST server
    transfer_detection,lastupdates = transfer_module.detect(sysvars.transfer_path)
    new_dumps = transfer_detection

    for db_name in new_dumps:
        transfer_module.seed(db_name,sysvars.transfer_path)
        transfer_module.seed(db_name,sysvars.transfer_path)
        print('database seeded within cluster')
        
        sub = '[RDAS] ACTION REQUIRED - New Dump Uploaded to Test Server'
        msg = f'New dump uploaded to test for database {db_name}'
        html = f'''<p>A new dump file has been uploaded to the test databases</p>
                <p>database effected: {db_name}</p>
                <p>To approve the database to be transfered to production, log in to the databases browser and select the effected database</p>
                <p>Run the following Cypher Query:</p>
                <p>MATCH (x:UserTesting) SET x.Approved = \"True\"</p>'''
        email_client.send_email(sub,html,recip)
        print(f'Notification emails sent to {recip}')

    print('[RDAS] Waiting for 15 seconds before checking for approval...')
    sleep(15)
    print('[RDAS] Checking Neo4j for recently approved databases')

    for db_name in sysvars.dump_dirs:
        db = AlertCypher(db_name)
        try:
            try:
                update = db.run('MATCH (x:UserTesting) RETURN x.Approved as update').data()[0]['update']
            except Exception:
                print(f'{db_name}:: False [Non-existent UserTesting Node]')
                continue

            if update == True:
                print(f'Detected Approved Database for {db_name}... sending to approved folder')
                dump_module.dump_file(sysvars.approved_path, db_name)
                db.run(f'MATCH (x:UserTesting) SET x.Approved = False, x.LastApprovedRDAS = \"{today_str}\"')
                p = Popen(['sudo', 'chmod', '777', f'{sysvars.approved_path}{db_name}.dump'], encoding='utf8')
                p.wait()

        except Exception as e:
            print(e)

    print('[RDAS] Waiting for 15 seconds before checking if new database files are in the approved folder')
    sleep(15)
    print('[RDAS] Checking approved folder for recently approved dump files')

    # Detects if a new dump file was loaded into the approved folder
    transfer_detection,lastupdates = transfer_module.detect(sysvars.approved_path)
    new_dumps = transfer_detection

    print(new_dumps)

    for db_name in new_dumps:
        print(f'Update approved for {db_name}... Sending to PROD')
        db = AlertCypher(db_name)
        
        # Sends to PROD
        send_url = sysvars.rdas_urls['prod']
        p = Popen(['scp', f'{sysvars.approved_path}{db_name}.dump', f'{sysvars.current_user}@{send_url}:{sysvars.transfer_path}{db_name}.dump'], encoding='utf8')
        p.wait()

        sub = '[RDAS] NOTICE - Test Database Approved'
        msg = f'{db_name} database was approved to move to production'
        html = f'''<p>A dump file on the test server has been approved to move to production</p>
                    <p>Database effected: {db_name}</p>
                    <p>There are no other actions required on your end</p>'''
        email_client.send_email(sub,html,recip)
        print(f'Notification emails sent to {recip}')

    # Waits one minute before restarting checks
    print('[RDAS] Waiting 15 seconds before restarting all checks')
    sleep(15)


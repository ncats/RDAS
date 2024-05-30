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
import detect_transfer
from seed_cluster import seed
from emails.alert import send_email,setup_email_client
#import file_transfer

while True:
    # Detects all new dump files in the transfer folder of the TEST server
    transfer_detection = detect_transfer.detect('test', sysvars.transfer_path)
    new_dumps = [k for (k,v) in transfer_detection.items() if v]
    #new_dumps = ['gard']
    # Seeds all 3 clusters in the TEST server so that the databases will be visible
    
    for db_name in new_dumps:
        seed(db_name,sysvars.transfer_path,'test')
        print('database seeded within cluster')

        for recip in sysvars.contacts:
            sub = '[RDAS] ACTION REQUIRED - New Dump Uploaded to Test Server'
            msg = f'New dump uploaded to test for database {db_name}'
            html = f'''<p>A new dump file has been uploaded to the test databases</p>
                    <p>database effected: {db_name}</p>
                    <p>To approve the database to be transfered to production, log in to the databases browser and select the effected database</p>
                    <p>Run the following Cypher Query:</p>
                    <p>MATCH (x:UserTesting) SET x.Approved = \"True\"</p>'''
            send_email(sub,msg,recip,html=html,client=setup_email_client())
            print(f'Notification email sent to {recip}')

    print('Waiting for 1 minute before checking for approval...')
    sleep(60)

    transfer_detection = detect_transfer.detect('test', sysvars.approved_path)
    new_dumps = [k for (k,v) in transfer_detection.items() if v]
    print(new_dumps)

    for db_name in new_dumps:
        db = AlertCypher(db_name)
        
        print(f'Update approved for {db_name}')

        send_url = sysvars.rdas_urls['prod']
        p = Popen(['scp', f'{sysvars.approved_path}{db_name}.dump', f'{sysvars.current_user}@{send_url}:{sysvars.transfer_path}{db_name}.dump'], encoding='utf8')
        p.wait()

        for recip in sysvars.contacts:
            sub = '[RDAS] NOTICE - Test Database Approved'
            msg = f'{db_name} database was approved to move to production'
            html = f'''<p>A dump file on the test server has been approved to move to production</p>
                        <p>Database effected: {db_name}</p>
                        <p>There are no other actions required on your end</p>'''
            send_email(sub,msg,recip,html=html,client=setup_email_client())
            print(f'Notification email sent to {recip}')

    # Waits one hour before retrying process
    sleep(3600)


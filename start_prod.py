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
import seed_cluster
from RDAS_MEMGRAPH_APP.Alert import Alert
from RDAS_MEMGRAPH_APP.Transfer import Transfer
from datetime import datetime

#!!! Script has to be ran with SUDO !!!

email_client = Alert()
transfer_module = Transfer('prod')
db = AlertCypher('system')
init = True

email_client = Alert()
db = AlertCypher('system')
while True:
    try:
        print('checking for update...')
        # Detect new dumps in the production server
        transfer_detection,last_updates = transfer_module.detect(sysvars.transfer_path)
        approval_detection,last_updates_approval = transfer_module.detect(sysvars.approved_path)
        new_dumps = [k for (k,v) in transfer_detection.items() if v]
        new_dumps_approval = [k for (k,v) in approval_detection.items() if v]

        # Sets the current db files last transfer date to today so it doesnt load and send emails upon script starting
        if init:
            init = False
            continue

        # Seed the seed cluster with the new dumps
        for db_name in new_dumps:
            print('update found::', db_name)
            last_update_obj = datetime.fromtimestamp(float(last_updates[db_name]))
            print('starting database loading')
            transfer_module.seed(db_name, sysvars.transfer_path)
            print('starting email service')
            email_client.trigger_email([db_name], date_start=datetime.strftime(last_update_obj, "%m/%d/%y"))

        # Sleep for a minute before checking for new dumps again
        sleep(60)
    except Exception as e:
        print(e)



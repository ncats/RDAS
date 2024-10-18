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
from datetime import datetime


email_client = Alert()
db = AlertCypher('system')
while True:
    # Detect new dumps in the production server
    transfer_detection,last_updates = detect_transfer.detect('prod',sysvars.transfer_path)
    new_dumps = [k for (k,v) in transfer_detection.items() if v]

    # Seed the seed cluster with the new dumps
    for db_name in new_dumps:
        last_update_obj = datetime.fromtimestamp(last_updates[db_name])
        seed_cluster.seed(db_name,sysvars.transfer_path,'prod')
        email_client.trigger_email([db_name], date_start=datetime.strftime(last_update_obj, "%m/%d/%y"))

    # Sleep for a minute before checking for new dumps again
    sleep(60)


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
import file_transfer

while True:
    # Detects all new dump files in the transfer folder of the TEST server
    transfer_detection = detect_transfer.detect('test', sysvars.transfer_path)
    new_dumps = [k for (k,v) in transfer_detection.items() if v]

    # Seeds all 3 clusters in the TEST server so that the databases will be visible
    for db_name in new_dumps:
        print(f'{db_name} cluster seeded')
        #seed_cluster.seed(db_name,sysvars.transfer_path) # NEEDS TO BE FIXED

    # Detects all new dumps files in the approved folder (quality checked files) of the TEST server
    approved_detection = detect_transfer.detect('test', sysvars.approved_path)
    new_dumps = [k for (k,v) in approved_detection.items() if v]

    # Transfers all newly approved dump files to the PROD server's transfer folder
    for db_name in new_dumps:
        print(f'{db_name} file staged for transfer')
        #file_transfer.transfer(sysvars.transfer_path, db_name, sysvars.rdas_urls['prod']) # NEEDS TO BE FIXED

    # Waits one hour before retrying process
    sleep(5)
    print('Next check initiated')

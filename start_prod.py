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

while True:
    transfer_detection = detect_transfer.detect('prod')
    new_dumps = [k for (k,v) in transfer_detection.items() if v]

    for db_name in new_dumps:
        seed_cluster.seed(db_name,sysvars.transfer_path)
        # send email alerts here

    sleep(3600)


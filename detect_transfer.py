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

def detect(server_name, path):
    db = AlertCypher('system')
    server = None
    config_title = None
    transfer_detection = {k:False for k in sysvars.dump_dirs}

    if server_name == 'test':
        server = 'TEST'
    elif server_name == 'prod':
        server = 'PROD'
    else:
        raise Exception

    if path == sysvars.transfer_path:
        config_title = 'TRANSFER'
    elif path == sysvars.approved_path:
        config_title = 'APPROVED'
    else:
        raise Exception

    for db_name in sysvars.dump_dirs:
        try:
            last_mod_date = db.getConf(f'{server}_{config_title}_DETECTION',f'{db_name}')
            cur_mod_date = os.path.getmtime(f"{path}{db_name}.dump")
            cur_mod_date = str(cur_mod_date)

            if not cur_mod_date == last_mod_date:
                transfer_detection[db_name] = True
                db.setConf(f'{server}_{config_title}_DETECTION',f'{db_name}',cur_mod_date)
        except Exception as e:
            transfer_detection[db_name] = False

    return transfer_detection


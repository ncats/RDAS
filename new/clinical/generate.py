import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import configparser
import load_neo4j
import update_neo4j
import datetime
from AlertCypher import AlertCypher

import threading
lock = threading.Lock()

def check (empty=False, db=AlertCypher("clinical"), date=datetime.date.today()):
    workspace = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    init = os.path.join(workspace, 'config.ini')
    configuration = configparser.ConfigParser()
    configuration.read(init)

    if empty == True:
        create(db)

    elif empty == False:
        update(db)
        
    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update (db):
    # Updates database from last update date
    update_neo4j.main(db)
    
def create (db):
    # Creates database from scratch
    load_neo4j.main(db)
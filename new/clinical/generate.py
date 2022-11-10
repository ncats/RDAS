import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import configparser
import load_neo4j
import datetime
from AlertCypher import AlertCypher
import threading
lock = threading.Lock()


def check (empty=False, db=AlertCypher("clinical"), date=datetime.date.today()):
    workspace = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    init = os.path.join(workspace, 'config.ini')
    configuration = configparser.ConfigParser()
    configuration.read(init)
    #conf = open(init, "w")

    if empty == True:
        create(db)
        #configuration.set('DATABASE', 'clinical_update', date.strftime("%m/%d/%y"))
        #configuration.write(conf)

    elif empty == False:
        update(db)
        #configuration.set('DATABASE', 'clinical_update', date.strftime("%m/%d/%y"))
        #configuration.write(conf)

    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update (db):
    # Updates database from last update date
    print('CLINICAL TRIAL DB UPDATING...')
    pass

def create (db):
    # Creates database from scratch
    load_neo4j.main(db)
    #pass
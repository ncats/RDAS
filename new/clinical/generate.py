import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import configparser
import load_neo4j
import datetime
from AlertCypher import AlertCypher


def check (empty=False, db=AlertCypher("clinical"), date=datetime.date.today()):
    workspace = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    init = os.path.join(workspace, 'config.ini')
    configuration = configparser.ConfigParser()
    configuration.read(init)

    if empty == True:
        create(db)
        configuration.set('DATABASE', 'clinical_update', date.strftime("%m/%d/%y"))
        with open(init, "w") as f:
            configuration.write(f)
    elif empty == False:
        update(db)
        configuration.set('DATABASE', 'clinical_update', date.strftime("%m/%d/%y"))
        with open(init, "w") as f:
            configuration.write(f)
    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update ():
    # Updates database from last update date
    pass

def create (db):
    # Creates database from scratch
    load_neo4j.main(db)
    #pass
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import configparser
import load_clinical
import update_clinical
import datetime
from AlertCypher import AlertCypher
import threading
lock = threading.Lock()

def check (empty=False, db=AlertCypher("clinical")):
    if empty == True:
        create(db)

    elif empty == False:
        update(db)
        
    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update (db):
    # Updates database from last update date
    update_clinical.main(db, update=True)
    
def create (db):
    # Creates database from scratch
    update_clinical.main(db, update=False)
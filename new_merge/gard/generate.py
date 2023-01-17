import sys
import os
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import load_gard
import update_gard
import configparser
import datetime
from AlertCypher import AlertCypher
import threading
lock = threading.Lock()

def check (empty=False, db=AlertCypher("gard")):
    if empty == True:
        create(db)
   
    elif empty == False:
        update(db)
      
    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update (db):
    # Updates database from last update date
    # connect to imported update script
    update_gard.main(db)
    print('GARD GRANT DB UPDATING...')

def create (db):
    # Creates database from scratch
    # connect to imported create script
    # use threading lock functions to prevent same line prints
    load_gard.main(db)
    lock.acquire()
    print('Creating GARD Grant Database...')
    lock.release()
    lock.acquire()
    print('Finishing up GARD Grant Database Creation...')
    lock.release()
    lock.acquire()
    print('GARD GRANT DATABASE CREATED')
    lock.release()
import sys
import os
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
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
    lock.acquire()
    print('UPDATING GARD Grant Database...')
    lock.release()
    update_gard.main(db, update=True)
    print('GARD GRANT DB UPDATING...')

def create (db):
    lock.acquire()
    print('Creating GARD Grant Database...')
    lock.release()
    update_gard.main(db, update=False)
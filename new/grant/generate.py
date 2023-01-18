import sys
import os
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import update_grant
import configparser
import datetime
from AlertCypher import AlertCypher
import threading
lock = threading.Lock()

def check (empty=False, db=AlertCypher("grant")):
    if empty == True:
        create(db)
   
    elif empty == False:
        update(db)
      
    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update (db):
    # Updates database from last update date
    # connect to imported update script
    update_grant.main(db)
    print('NIH GRANT DB UPDATING...')

def create (db):
    # Creates database from scratch
    # connect to imported create script
    # use threading lock functions to prevent same line prints
    update_grant.main(db)
    lock.acquire()
    print('Creating NIH Grant Database...')
    lock.release()
    lock.acquire()
    print('Finishing up NIH Grant Database Creation...')
    lock.release()
    lock.acquire()
    print('NIH GRANT DATABASE CREATED')
    lock.release()
    

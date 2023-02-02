import sys
import os
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import update_pubmed
import configparser
import datetime
from AlertCypher import AlertCypher
import threading
lock = threading.Lock()


def check (empty=False, db=AlertCypher("pubmed")):
    if empty == True:
        create(db)

    elif empty == False:
        update(db)

    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update (db):
    # Updates database from last update date
    # connect to imported update script
    print('PUBMED DB UPDATING...')
    update_pubmed.main(db, update=True)

def create (db):
    # Creates database from scratch
    # connect to imported create script
    # use threading lock functions to prevent same line prints
    lock.acquire()
    print('Creating PubMed Database...')
    lock.release()
    update_pubmed.main(db, update=False)
    lock.acquire()
    print('Finishing up PubMed Database Creation...')
    lock.release()
    lock.acquire()
    print('PUBMED DATABASE CREATED')
    lock.release()
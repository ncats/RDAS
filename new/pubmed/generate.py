import configparser
import datetime
import os
from AlertCypher import AlertCypher
import threading
lock = threading.Lock()

def check (empty=False, db=AlertCypher("pubmed"), date=datetime.date.today()):
    workspace = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    init = os.path.join(workspace, 'config.ini')
    configuration = configparser.ConfigParser()
    configuration.read(init)
    #conf = open(init, "w")

    if empty == True:
        create(db)
        #configuration.set('DATABASE', 'pubmed_update', date.strftime("%m/%d/%y"))
        #configuration.write(conf)

    elif empty == False:
        update(db)
        #configuration.set('DATABASE', 'pubmed_update', date.strftime("%m/%d/%y"))
        #configuration.write(conf)

    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update (db):
    # Updates database from last update date
    # connect to imported update script
    print('PUBMED DB UPDATING...')
    pass

def create (db):
    # Creates database from scratch
    # connect to imported create script
    # use threading lock functions to prevent same line prints
    
    lock.acquire()
    print('Creating PubMed Database...')
    lock.release()
    lock.acquire()
    print('Finishing up PubMed Database Creation...')
    lock.release()
    lock.acquire()
    print('PUBMED DATABASE CREATED')
    lock.release()
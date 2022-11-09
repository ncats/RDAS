import configparser
import datetime
import os
from AlertCypher import AlertCypher

def check (empty=False, db=AlertCypher("grant"), date=datetime.date.today()):
    workspace = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    init = os.path.join(workspace, 'config.ini')
    configuration = configparser.ConfigParser()
    configuration.read(init)
    
    if empty == True:
        create(db)
        configuration.set('DATABASE', 'grant_update', date.strftime("%m/%d/%y"))
        with open(init, "w") as f:
            configuration.write(f)
    elif empty == False:
        update(db)
        configuration.set('DATABASE', 'grant_update', date.strftime("%m/%d/%y"))
        with open(init, "w") as f:
            configuration.write(f)

    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update (db):
    # Updates database from last update date
    # connect to imported update script
    pass

def create (db):
    # Creates database from scratch
    # connect to imported create script
    print('Creating NIH Grant Database...')
    print('Finishing up NIH Grant Database Creation...')
    print('NIH GRANT DATABASE CREATED\n')

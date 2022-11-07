import configparser
import datetime
import os

def check (empty=False, date=datetime.date.today()):
    workspace = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    init = os.path.join(workspace, 'config.ini')
    configuration = configparser.ConfigParser()
    configuration.read(init)
    
    if empty == True:
        create()
        configuration.set('DATABASE', 'grant_update', date.strftime("%m/%d/%y"))
        with open(init, "w") as f:
            configuration.write(f)
    elif empty == False:
        update()
        configuration.set('DATABASE', 'grant_update', date.strftime("%m/%d/%y"))
        with open(init, "w") as f:
            configuration.write(f)
    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update ():
    pass

def create ():
    pass
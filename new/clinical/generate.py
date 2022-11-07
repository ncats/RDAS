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
        configuration.set('DATABASE', 'database_last_run', date.strftime("%m/%d/%y"))
    elif empty == False:
        update()
        configuration.set('DATABASE', 'database_last_run', date.strftime("%m/%d/%y"))
    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update ():
    pass

def create ():
    pass
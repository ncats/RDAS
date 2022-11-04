import configparser
import datetime

def check (empty=False, date=datetime.date.today()):
    configuration = configparser.ConfigParser()
    configuration.read("config.ini")
    
    if empty == True:
        create()
        configuration.set('database', 'database_last_run', date.strftime("%m/%d/%y"))
    elif empty == False:
        update()
        configuration.set('database', 'database_last_run', date.strftime("%m/%d/%y"))
    else:
        print("[ERROR] generate.py \"empty\" parameter not boolean")

def update ():
    pass

def create ():
    pass
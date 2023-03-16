import sys
import os
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import clinical.generate, grant.generate, pubmed.generate, gard.generate
from neo4j import GraphDatabase
from AlertCypher import AlertCypher
from gard import update_gard
from csv import DictReader
import threading
import configparser
from datetime import datetime, date
from time import sleep

def populate(db):
    '''
    Checks if Neo4j database is empty. If the database is either empty or the [database name]_finished value in config.ini is False, it creates a thread that creates the database
    from scratch or from the logged progress point
    '''
    unpack = {"clinical":[clinical, 'clinical_finished'], "grant":[grant, 'grant_finished'], "pubmed":[pubmed, 'pubmed_finished'], "gard":[gard, 'gard_finished']}
    cur_module = unpack[db.DBtype()]
    try:
        response = db.run("MATCH (x) RETURN x LIMIT 1").single()
        
        if db.getConf('DATABASE', cur_module[1]) == 'False':
            response = None
            
        if response == None:
            print('Creating {dbtype} database'.format(dbtype=db.DBtype().upper()))
            db.setConf('DATABASE', cur_module[1], 'False')
            thread = threading.Thread(target=cur_module[0].generate.check, args=(True, db,), daemon=True)
            return thread
        
    except Exception as e:
        print(e)
        print("[{dbtype}] Error finding Neo4j database. Check to see if database exists and rerun script".format(dbtype=db.DBtype().upper()))

def updateCheck(db):
    '''
    Creates a thread that updates a database if a set interval of time has passed since the last update
    '''
    unpack = {"clinical":["clinical_update","clinical_interval", clinical], 
        "grant":["grant_update","grant_interval", grant],
        "pubmed":["pubmed_update","pubmed_interval", pubmed]}

    updateInfo = unpack[db.DBtype()]
    # Reload configuration information
    update = db.getConf("DATABASE", updateInfo[0])
    if update == '':
        update = datetime.now()
        update = update.strftime("%m/%d/%y")
        update = datetime.strptime(update,"%m/%d/%y")
    else:
        update = datetime.strptime(update,"%m/%d/%y")

    current_time = datetime.now()
    current_time = current_time.strftime("%m/%d/%y")
    current_time = datetime.strptime(current_time,"%m/%d/%y")

    delta = current_time - update

    interval = int(db.getConf("DATABASE", updateInfo[1]))
    if delta.days > interval:
        thread = threading.Thread(target=updateInfo[2].generate.check, args=(False, db,), daemon=True)
        return {db.DBtype():[thread, delta, update, db, db.DBtype()]}
    else:
        return {db.DBtype():[None, delta, update, db, db.DBtype()]}

def updateDate(info):
    '''
    Writes the current date in the config.ini file when an update on a database has finished
    '''
    date = datetime.now().strftime("%m/%d/%y")
    try:
        if info["clinical"][0]:
            info["clinical"][3].setConf("DATABASE", "clinical_update", date)
    except TypeError:
        pass
    try:
        if info["grant"][0]:
            info["grant"][3].setConf("DATABASE", "grant_update", date)
    except TypeError:
        pass
    try:
        if info["pubmed"][0]:
            info["pubmed"][3].setConf("DATABASE", "pubmed_update", date)
    except TypeError:
        pass

print("[RARE DISEASE ALERT SYSTEM]\n---------------------------")

# Load configuration information
workspace = os.path.dirname(os.path.abspath(__file__))
init = os.path.join(workspace, 'config.ini')
configuration = configparser.ConfigParser()
configuration.read(init)

# Setup individual database communication objects. AlertCypher object is used to send cypher queries to a specific database in the server
CTcypher = AlertCypher("clinical")
GNTcypher = AlertCypher("grant")
PMcypher = AlertCypher("pubmed")
GARDcypher = AlertCypher("gard")

# Checks if clinical trial database is empty. If it is, it creates it from scratch on a seperate thread
threads = list()
threads.append(populate(CTcypher))
threads.append(populate(GNTcypher))
threads.append(populate(PMcypher))
threads.append(populate(GARDcypher))

# Recreates the GARD disease database when an update for any other database is triggered
if threads:
    update_gard.main(GARDcypher, update=True)

for thread in threads:
    if thread:
        thread.start()

for thread in threads:
    if thread:
        thread.join()

updateDate(threads)

# Runs until program is exited: Checks to see if any databases needed updated occasionally. 
while True:
    threads = list()
    info = dict()

    CT_info = updateCheck(CTcypher)["clinical"]
    info["clinical"] = CT_info
    GNT_info = updateCheck(GNTcypher)["grant"]
    info["grant"] = GNT_info
    PM_info = updateCheck(PMcypher)["pubmed"]
    info["pubmed"] = PM_info

    if info["clinical"][0]:
        threads.append(info["clinical"][0])
    if info["grant"][0]:
        threads.append(info["grant"][0])
    if info["pubmed"][0]:
        threads.append(info["pubmed"][0])
    
        
    print("\n-----------------------\nDays Since Last Update:\n[CLINICAL] {CT_day} Days ({CT_update})\n[GRANT] {GNT_day} Days ({GNT_update})\n[PUBMED] {PM_day} Days ({PM_update})\n"
            .format(CT_day=str(info["clinical"][1].days), 
            GNT_day=str(info["grant"][1].days), 
            PM_day=str(info["pubmed"][1].days),
            CT_update=str(info["clinical"][2]),
            GNT_update=str(info["grant"][2]),
            PM_update=str(info["pubmed"][2])))

    if threads:
        update_gard.main(GARDcypher, update=True)

    for thread in threads:
        if thread:
            thread.start()
            
    for thread in threads:
        if thread:
            thread.join()

    updateDate(info)

    # Time in seconds to check for an update
    sleep(3600)

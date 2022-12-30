import sys
import os
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import clinical.generate, grant.generate, pubmed.generate, gard.generate
from neo4j import GraphDatabase
from AlertCypher import AlertCypher
from csv import DictReader
import threading
import configparser
from datetime import datetime, date
from time import sleep

def populate(db):
    unpack = {"clinical":clinical, "grant":grant, "pubmed":pubmed, "gard":gard}
    cur_module = unpack[db.DBtype()]
    try:
        response = db.run("MATCH (x) RETURN x LIMIT 1").single()
        if response == None:
            print('{dbtype} Database Empty'.format(dbtype=db.DBtype().upper()))
            thread = threading.Thread(target=cur_module.generate.check, args=(True, db,), daemon=True)
            return thread
        
    except:
        print("[{dbtype}] Error finding Neo4j database. Check to see if database exists and rerun script".format(dbtype=db.DBtype().upper()))

def updateCheck(db):
    unpack = {"clinical":["clinical_update","clinical_interval", clinical], 
        "grant":["grant_update","grant_interval", grant], 
        "pubmed":["pubmed_update","pubmed_interval", pubmed],
        "gard":["gard_update","gard_interval", gard]}

    updateInfo = unpack[db.DBtype()]
    # Reload configuration information

    update = db.getConf("DATABASE", updateInfo[0])
    if update == "":
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
    try:
        if info["gard"][0]:
            info["gard"][3].setConf("DATABASE", "gard_update", date)
    except TypeError:
        pass
    

def updateGard():
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

for thread in threads:
    if thread:
        thread.start()

for thread in threads:
    if thread:
        thread.join()

updateDate(threads)

# Gets last database update from configuration file
while True:
    updateGard()

    threads = list()
    info = dict()
    CT_info = updateCheck(CTcypher)["clinical"]
    info["clinical"] = CT_info
    GNT_info = updateCheck(GNTcypher)["grant"]
    info["grant"] = GNT_info
    PM_info = updateCheck(PMcypher)["pubmed"]
    info["pubmed"] = PM_info
    GARD_info = updateCheck(GARDcypher)["gard"]
    info["gard"] = GARD_info

    if info["clinical"][0]:
        threads.append(info["clinical"][0])
    if info["grant"][0]:
        threads.append(info["grant"][0])
    if info["pubmed"][0]:
        threads.append(info["pubmed"][0])
    if info["gard"][0]:
        threads.append(info["gard"][0])
    
    print("\n-----------------------\nDays Since Last Update:\n[CLINICAL] {CT_day} Days ({CT_update})\n[GRANT] {GNT_day} Days ({GNT_update})\n[PUBMED] {PM_day} Days ({PM_update})\n[GARD] {GARD_day} Days ({GARD_update})\n"
            .format(CT_day=str(info["clinical"][1].days), 
            GNT_day=str(info["grant"][1].days), 
            PM_day=str(info["pubmed"][1].days),
            GARD_day=str(info["gard"][1].days), 
            CT_update=str(info["clinical"][2]),
            GNT_update=str(info["grant"][2]),
            PM_update=str(info["pubmed"][2]),
            GARD_update=str(info["gard"][2])))

    for thread in threads:
        if thread:
            thread.start()
            
    for thread in threads:
        if thread:
            thread.join()

    updateDate(info)

    # Time in seconds to check for an update
    sleep(5)
import sys
import os
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import clinical.generate, grant.generate, pubmed.generate
from neo4j import GraphDatabase
from AlertCypher import AlertCypher
import threading
import configparser
import datetime
from time import sleep

def populate(db):
    unpack = {"clinical":clinical, "grant":grant, "pubmed":pubmed}
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
    unpack = {"clinical":["clinical_update","clinical_interval"], 
        "grant":["grant_update","grant_interval"], 
        "pubmed":["pubmed_update","pubmed_interval"]}

    updateInfo = unpack[db.DBtype()]
    # Reload configuration information
    configuration.read(init)
    
    update = configuration.get("DATABASE", updateInfo[0])
    if update == "":
        update = datetime.date.today()
        update = update.strftime("%m/%d/%y")
        update = datetime.datetime.strptime(update,"%m/%d/%y")
    else:
        update = datetime.datetime.strptime(update,"%m/%d/%y")

    current_time = datetime.date.today()
    current_time = current_time.strftime("%m/%d/%y")
    current_time = datetime.datetime.strptime(current_time,"%m/%d/%y")

    delta = current_time - update

    interval = int(configuration.get("DATABASE", updateInfo[1]))
    if delta.days > interval:
        thread = threading.Thread(target=clinical.generate.check, args=(False, db,), daemon=True)
        return {db.DBtype():[thread, delta, update]}
    else:
        return {db.DBtype():[None, delta, update]}

def updateDate(threads):
    date = datetime.date.today()
    conf = open(init, "w")
    for i in range(len(threads)):
        if threads[i]:
            if i == 0:
                configuration.set('DATABASE', 'clinical_update', date.strftime("%m/%d/%y"))
            elif i == 1:
                configuration.set('DATABASE', 'grant_update', date.strftime("%m/%d/%y"))
            elif i == 2:
                configuration.set('DATABASE', 'pubmed_update', date.strftime("%m/%d/%y"))
    configuration.write(conf)
    conf.close()

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

# Checks if clinical trial database is empty. If it is, it creates it from scratch on a seperate thread

threads = list()
threads.append(populate(CTcypher))
threads.append(populate(GNTcypher))
threads.append(populate(PMcypher))

for thread in threads:
    if thread:
        thread.start()

for thread in threads:
    if thread:
        thread.join()

updateDate(threads)

# Gets last database update from configuration file
while True:
    threads = list()
    CT_info = updateCheck(CTcypher)["clinical"]
    GNT_info = updateCheck(GNTcypher)["grant"]
    PM_info = updateCheck(PMcypher)["pubmed"]

    if CT_info[0]:
        threads.append(CT_info[0])
    if GNT_info[0]:
        threads.append(GNT_info[0])
    if PM_info[0]:
        threads.append(PM_info[0])
    
    print("\n-----------------------\nDays Since Last Update:\n[CLINICAL] {CT_day} Days ({CT_update})\n[GRANT] {GNT_day} Days ({GNT_update})\n[PUBMED] {PM_day} Days ({PM_update})\n"
            .format(CT_day=str(CT_info[1].days), 
            GNT_day=str(GNT_info[1].days), 
            PM_day=str(PM_info[1].days), 
            CT_update=str(CT_info[2]),
            GNT_update=str(GNT_info[2]),
            PM_update=str(PM_info[2])))

    for thread in threads:
        if thread:
            thread.start()
            
    for thread in threads:
        if thread:
            thread.join()

    updateDate(threads)

    # Time in seconds to check for an update
    sleep(5)
from neo4j import GraphDatabase
import clinical.generate, grant.generate, pubmed.generate
from AlertCypher import AlertCypher
import threading
import configparser
import datetime
from time import sleep
import sys
import os

print("[RARE DISEASE ALERT SYSTEM]\n---------------------------")

# Load configuration information
workspace = os.path.dirname(os.path.abspath(__file__))
init = os.path.join(workspace, 'config.ini')
configuration = configparser.ConfigParser()

# Setup individual database communication objects. AlertCypher object is used to send cypher queries to a specific database in the server
CTcypher = AlertCypher("clinical")
GNTcypher = AlertCypher("grant")
PMcypher = AlertCypher("pubmed")

# Checks if clinical trial database is empty. If it is, it creates it from scratch on a seperate thread
try:
    response = CTcypher.run("MATCH (x) RETURN x LIMIT 1").single()
    if response == None:
        CT_thread = threading.Thread(target=clinical.generate.check, args=(True,), daemon=True)
        CT_thread.start()
        CT_thread.join()
except:
    print("[CLINICAL TRIAL] Error finding Neo4j database. Check to see if database exists and rerun script")

# Checks if NIH grant database is empty. If it is, it creates it from scratch on a seperate thread
try:
    response = GNTcypher.run("MATCH (x) RETURN x LIMIT 1").single()
    if response == None:
        GNT_thread = threading.Thread(target=grant.generate.check, args=(True,), daemon=True)
        GNT_thread.start()
        GNT_thread.join()
except:
    print("[GRANT] Error finding Neo4j database. Check to see if database exists and rerun script")

# Checks if PubMed database is empty. If it is, it creates it from scratch on a seperate thread
try:
    response = PMcypher.run("MATCH (x) RETURN x LIMIT 1").single()
    if response == None:
        PM_thread = threading.Thread(target=pubmed.generate.check, args=(True,), daemon=True)
        PM_thread.start()
        PM_thread.join()
except:
    print("[PUBMED] Error finding Neo4j database. Check to see if database exists and rerun script")

CTcypher.close()
GNTcypher.close()
PMcypher.close()

# Gets last database update from configuration file
while True:
    # Reload configuration information
    configuration.read(init)
    interval = configuration.get("DATABASE","update_interval")

    CT_update = configuration.get("DATABASE","clinical_update")
    if CT_update == "":
        CT_update = datetime.date.today()
        CT_update = CT_update.strftime("%m/%d/%y")
        CT_update = datetime.datetime.strptime(CT_update,"%m/%d/%y")
    else:
        CT_update = datetime.datetime.strptime(CT_update,"%m/%d/%y")

    GNT_update = configuration.get("DATABASE","grant_update")
    if GNT_update == "":
        GNT_update = datetime.date.today()
        GNT_update = GNT_update.strftime("%m/%d/%y")
        GNT_update = datetime.datetime.strptime(GNT_update,"%m/%d/%y")
    else:
        GNT_update = datetime.datetime.strptime(GNT_update,"%m/%d/%y")

    PM_update = configuration.get("DATABASE","pubmed_update")
    if PM_update == "":
        PM_update = datetime.date.today()
        PM_update = PM_update.strftime("%m/%d/%y")
        PM_update = datetime.datetime.strptime(PM_update,"%m/%d/%y")
    else:
        PM_update = datetime.datetime.strptime(PM_update,"%m/%d/%y")

    # Starts a database update every interval of days
    current_time = datetime.date.today()
    current_time = current_time.strftime("%m/%d/%y")
    current_time = datetime.datetime.strptime(current_time,"%m/%d/%y")
    
    CT_delta = current_time - CT_update
    GNT_delta = current_time - GNT_update
    PM_delta = current_time - PM_update

    if CT_delta.days > interval:
        CT_thread = threading.Thread(target=clinical.generate.check, daemon=True)
        CT_thread.start()
        CT_thread.join()

    if GNT_delta.days > interval:
        GNT_thread = threading.Thread(target=grant.generate.check, daemon=True)
        GNT_thread.start()
        GNT_thread.join()

    if PM_delta.days > interval:
        PM_thread = threading.Thread(target=pubmed.generate.check, daemon=True)
        PM_thread.start()
        PM_thread.join()

    print("Days Since Last Update:\n[CLINICAL] {CT_day} Days ({CT_update})\n[GRANT] {GNT_day} Days ({GNT_update})\n[PUBMED] {PM_day} Days ({PM_update})\n"
        .format(CT_day=str(CT_delta.days), 
        GNT_day=str(GNT_delta.days), 
        PM_day=str(PM_delta.days), 
        CT_update=str(CT_update),
        GNT_update=str(GNT_update),
        PM_update=str(PM_update)))
    
    # Time in seconds to check for an update
    sleep(3600)
    

    

    
    
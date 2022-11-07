from neo4j import GraphDatabase
import clinical.generate, grant.generate, pubmed.generate
from AlertCypher import AlertCypher
import threading
import configparser
import datetime
from time import sleep
import sys
import os

print("[RARE DISEASE ALERT SYSTEM]")

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

# Gets last database update from configuration file
last_run = configuration.get("DATABASE","database_last_run")
if last_run == "":
    start_time = datetime.date.today()
    start_time = start_time.strftime("%m/%d/%y")
    start_time = datetime.datetime.strptime(start_time,"%m/%d/%y")
else:
    start_time = last_run.strftime("%m/%d/%y")
    start_time = datetime.datetime.strptime(start_time,"%m/%d/%y")

# Starts a database update every interval of days
while True:
    current_time = datetime.date.today()
    current_time = current_time.strftime("%m/%d/%y")
    current_time = datetime.datetime.strptime(current_time,"%m/%d/%y")
    
    delta = current_time - start_time
    if delta.days == 30:
        clinical.generate.check()
        grant.generate.check()
        pubmed.generate.check()

    print("Days Since Last Update:\n{day} Days\n".format(day=str(delta.days)))
    sleep(3600)
    
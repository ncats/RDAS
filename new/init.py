from neo4j import GraphDatabase
import clinical.generate, grant.generate, pubmed.generate
import configparser
import datetime
from time import sleep
import sys
import os

print("[RARE DISEASE ALERT SYSTEM]\n")

"""
In this section, check to see if all the databases exist on the server, if not, call each missing generate script with empty=true





"""

configuration = configparser.ConfigParser()
configuration.read("config.ini")
last_run = configuration.get('database','database_last_run')

if last_run == "":
    start_time = datetime.date.today()
    start_time = start_time.strftime("%m/%d/%y")
    start_time = datetime.datetime.strptime(start_time,"%m/%d/%y")
else:
    start_time = last_run.strftime("%m/%d/%y")
    start_time = datetime.datetime.strptime(start_time,"%m/%d/%y")

while True:
    current_time = datetime.date.today()
    current_time = current_time.strftime("%m/%d/%y")
    current_time = datetime.datetime.strptime(current_time,"%m/%d/%y")
    
    delta = current_time - start_time
    if delta.days == 30:
        clinical.generate.check()
        grant.generate.check()
        pubmed.generate.check()

    sleep(3600)
    print("Days Since Last Update:\n{day} Days\n".format(day=str(delta.days)))
    
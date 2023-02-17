import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import load_neo4j_functions, data_model
from datetime import date
from http import client
from neo4j import GraphDatabase
from csv import DictReader
from AlertCypher import AlertCypher
import configparser
import threading
import pandas as pd
lock = threading.Lock()
from firestore_base.ses_firebase import trigger_email

def remove_dupes(db):
    for idx in range(len(data_model.additional_class_fields)):     
        apoc_cypher = 'MATCH (x:{tag}) WITH '.format(tag=data_model.additional_class_names[idx])
        for idy in range(len(data_model.additional_class_fields[idx])):
            apoc_cypher += 'toLower(x.{name}) AS label{name}, '.format(name=data_model.additional_class_fields[idx][idy])
        apoc_cypher += 'COLLECT(x) AS nodes CALL apoc.refactor.mergeNodes(nodes, {properties:"overwrite",mergeRels:true}) YIELD node RETURN *'
        db.run(apoc_cypher)
    
    apoc_cypher = 'MATCH (x:ClinicalTrial) WITH '
    apoc_cypher += 'x.NCTId AS nct, '
    apoc_cypher += 'COLLECT(x) AS nodes CALL apoc.refactor.mergeNodes(nodes, {properties:"overwrite",mergeRels:true}) YIELD node RETURN *'
    db.run(apoc_cypher)

def add_trial(db, trials, GARDId):
    cypher_add_trial_base = 'MATCH (gard:GARD) WHERE gard.GARDId = \'' + GARDId + '\''
    for trial in trials:
        # DB data pull
        existing_trial_data = db.run('MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{trial}\" RETURN x'.format(trial=trial)).data()
        # clinicaltrial.gov data pull
        full_trial = load_neo4j_functions.extract_all_fields(trial)
        
        #if trial exists in db
        if len(existing_trial_data) > 0:
            existing_trial_data = existing_trial_data[0]['x']
            #if trial exists in db and hasnt been updated on clinicaltrial.gov then continue to next
            if full_trial['LastUpdatePostDate'] == [existing_trial_data['LastUpdatePostDate']]:
                continue
            #if trial exists in db and HAS been updated on clinicaltrial.gov
            else:
                update = True
                clinical_trial_data_string = load_neo4j_functions.data_string(full_trial, data_model.ClinicalTrial, update, CT = True)

                if not len(clinical_trial_data_string) > 0:
                    continue
        
                cypher_add_trial = 'MATCH (trial:ClinicalTrial) WHERE trial.NCTId = \"' + trial + '\" SET '
                cypher_add_trial += clinical_trial_data_string[0]
            
                db.run(cypher_add_trial)
        #if trial does NOT exist
        else:
            # create clinical trial node
            update = False
            clinical_trial_data_string = load_neo4j_functions.data_string(full_trial, data_model.ClinicalTrial, update, CT = True)

            if not len(clinical_trial_data_string) > 0:
                continue
        
            cypher_add_trial = cypher_add_trial_base + 'CREATE (gard)-[:gard_in]->(trial:ClinicalTrial{'
            cypher_add_trial += clinical_trial_data_string[0]
            cypher_add_trial += '})'
            
            db.run(cypher_add_trial)

            if len(clinical_trial_data_string) == 0:
                print('\n\ttrial:',trial,'has no data')
            
            # generate data for additional classes
            additional_class_data = list()
            for class_fields in data_model.additional_class_fields:
                additional_class_data.append(load_neo4j_functions.data_string(full_trial, class_fields, update = False))
                
            # cypher query to create and attach additional class nodes
            cypher = 'MATCH (trial:ClinicalTrial) WHERE trial.NCTId = \''+trial+'\''
            cypher2 = ''
            for j in range(len(additional_class_data)):
                field = additional_class_data[j]
                if len(field) > 0:
                    for i in range(len(field)):
                            cypher2 += 'CREATE (trial)' + data_model.data_direction[j][0] + '[:' + data_model.additional_class_connections[j]
                            cypher2 += ']' + data_model.data_direction[j][1] + '(:'
                            cypher2 += data_model.additional_class_names[j] + '{' + field[i] +'})'

            cypher_batch = ['CREATE' + i for i in cypher2.split('CREATE')]
            for b in range(1,len(cypher_batch),4):
                b = cypher + ' '.join(cypher_batch[b:b+4])
                db.run(b)


def main(db, update=False):
    print('CLINICAL TRIAL DB UPDATING...')
    GARDdb = AlertCypher("gard")
    gard_matches = GARDdb.run('MATCH (x:GARD) RETURN x')
    for gard_mapping in gard_matches.data():
        gard_mapping = gard_mapping['x']
        
        GARDId = gard_mapping['GardId']
        GARD_name = gard_mapping['GardName'].replace('\\','\\\\').replace('\'','\\\'').replace('\"','\\"')
        GARD_synonyms = gard_mapping['Synonyms']
        GARD_list = [GARD_name] + GARD_synonyms
        retrieved_trials = load_neo4j_functions.nctid_list(GARD_list)
        db.run('MERGE (gard:GARD{GARDName: \"' + GARD_name + '\", GARDId: \"' + GARDId + '\"})')
        
        if len(retrieved_trials) > 0:
            add_trial(db, retrieved_trials, GARDId)

    lock.acquire()
    print('Finishing up Clinical Trial Database Update...')
    lock.release()
    
    remove_dupes(db) 
    
    if update == True:
        trigger_email(db, "clinical")
    
    print('Clinical Trial Database Update Finished')
        
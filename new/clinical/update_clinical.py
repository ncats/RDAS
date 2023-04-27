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
    '''
    Removes duplicate nodes by merging copied nodes and relationships together
    '''
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
    '''
    Retrieves clinical trial data and adds nodes and relationships to other data nodes
    '''
    cypher_add_trial_base = 'MATCH (gard:GARD) WHERE gard.GARDId = \'' + GARDId + '\''
    for trial in trials:
        print(trial, end="")
        print('.', end="")
        # DB data pull
        existing_trial_data = db.run('MATCH (y:GARD)--(x:ClinicalTrial) WHERE y.GARDId = \"{GARD}\" AND x.NCTId = \"{trial}\" RETURN x'.format(trial=trial, GARD=GARDId)).data()
        # clinicaltrial.gov data pull
        full_trial = load_neo4j_functions.extract_all_fields(trial)
        
        #if trial exists in db
        if len(existing_trial_data) > 0:
            existing_trial_data = existing_trial_data[0]['x']

            #if trial exists in db and hasnt been updated on clinicaltrial.gov then continue to next
            try:
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

            except KeyError as e:
                print(e)
                continue
        #if trial does NOT exist under specific disease
        else:
            # create or merge clinical trial node
            exists_in_db = db.run('MATCH (x:ClinicalTrial)--(y:GARD) WHERE x.NCTId = \"{trial}\" RETURN x'.format(trial=trial)).data()
            if len(exists_in_db) > 0:
                db.run('MATCH (y:GARD),(x:ClinicalTrial) WHERE y.GARDId = \"{GARD}\" AND  x.NCTId = \"{trial}\" MERGE (y)-[r:gard_in]->(x)'.format(trial=trial,GARD=GARDId))
                print('Merged with existing trial in database')
                continue
 
            update = False
            clinical_trial_data_string = load_neo4j_functions.data_string(full_trial, data_model.ClinicalTrial, update, CT = True)

            if not len(clinical_trial_data_string) > 0:
                print('else: no length on clinical_trial_data_string')
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
            
            # Section sometimes gets a neo4j unknown error, below code will retry if there is an error
            attempt = True
            tries = 0
            while attempt:
                try:
                    for b in range(1,len(cypher_batch),4):
                        b = cypher + ' '.join(cypher_batch[b:b+4])
                        db.run(b)
                        attempt = False
                except Exception as e:
                    if tries > 10:
                        break
                    tries += 1
                    print(e)

def main(db, update=False):
    if update:
      print('CLINICAL TRIAL DB UPDATING...')
    else:
      print('CREATING CLINICAL TRIAL DB')

    GARDdb = AlertCypher("gard")
    gard_matches = GARDdb.run('MATCH (x:GARD) RETURN x')
    progress = db.getConf('DATABASE', 'clinical_progress')
    
    if progress == '':
        progress = 0
    else:
        progress = int(progress)
    
    for idx, gard_mapping in enumerate(gard_matches.data()):
        
        if idx < progress:
          continue
        
        gard_mapping = gard_mapping['x']
        GARDId = gard_mapping['GardId']
        print(str(idx) + ": " + GARDId)
        GARD_name = gard_mapping['GardName'].replace('\\','\\\\').replace('\'','\\\'').replace('\"','\\"')
        GARD_synonyms = [x for x in gard_mapping['Synonyms'] if " " in x]
        GARD_list = [GARD_name] + GARD_synonyms
        print(GARD_list)
        retrieved_trials = load_neo4j_functions.nctid_list(GARD_list)
        print(f'Retrieved Trials: {len(retrieved_trials)}')
        db.run('MERGE (gard:GARD{GARDName: \"' + GARD_name + '\", GARDId: \"' + GARDId + '\"})')
        
        if len(retrieved_trials) > 0:
            add_trial(db, retrieved_trials, GARDId)
            
        db.setConf('DATABASE', 'clinical_progress', str(idx))

    lock.acquire()
    print('Finishing up Clinical Trial Database Update...')
    lock.release()
    
    remove_dupes(db) 
    
    if update:
        trigger_email(db, "clinical")
    else:
        db.setConf('DATABASE', 'pubmed_finished', 'True')
        db.setConf('DATABASE', 'pubmed_progress', '')
    
    print('Clinical Trial Database Update Finished')
        

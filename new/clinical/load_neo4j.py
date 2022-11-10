import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import load_neo4j_functions, data_model
from datetime import date
from http import client
from neo4j import GraphDatabase
from csv import DictReader
import configparser
import threading
lock = threading.Lock()

def main(db):
    lock.acquire()
    print('Creating Clinical Trial Database...')
    lock.release()

    # iterate through list of diseases/conditions
    cnd = os.path.join(workspace, 'conditions_matched_short.csv')
    with open(cnd, 'r') as read_obj:
        gard_matches = DictReader(read_obj)
        for gard_mapping in gard_matches:
            try:
                # extract data from mapping
                GARDId = gard_mapping['gard_id']
                GARD_name = gard_mapping['gard_name'].replace('\\','\\\\').replace('\'','\\\'').replace('\"','\\"')
                CT_name = gard_mapping['disease_name']
                
                # create gard condition node
                cypher_create_gard = 'MERGE (gard:GARD{GardName: \"' + GARD_name + '\", GARDId: \"' + GARDId + '\"})'
                response_trial_exists = db.run(cypher_create_gard)
                
                # add each clinical trial to neo4j
                trials = load_neo4j_functions.nctid_list(CT_name)
            except:
                continue
        
            for trial in trials:
                running = True
                while running:
                    try:
                        # generate neo4j query to attach disease to clinical trial
                        cypher_add_trial_base = 'MATCH (gard:GARD) WHERE gard.GardName = \'' + GARD_name + '\''
                        
                        # neo4j query to check if clinical trial node already exists
                        cypher_trial_exists = 'MATCH (trial:ClinicalTrial) WHERE trial.NCTId = \''+trial+'\' RETURN COUNT(trial)'
                        response_trial_exists = db.run(cypher_trial_exists)

                        # node doesn't exist, create new clinical trial node and connect
                        if int([elm[0] for elm in response_trial_exists][0]) == 0:
                            
                            # extract data from clinical trial
                            full_trial = load_neo4j_functions.extract_all_fields(trial)
                            
                            # create clinical trial node
                            clinical_trial_data_string = load_neo4j_functions.data_string(full_trial, data_model.ClinicalTrial, CT=True)
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
                                additional_class_data.append(load_neo4j_functions.data_string(full_trial, class_fields))
                                
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
                                running = False
                            
                        # if node exists
                        else:
                            # attach to gard node
                            cypher_add_trial = cypher_add_trial_base + 'MATCH (trial:ClinicalTrial) WHERE trial.NCTId = \'' + trial + '\''
                            cypher_add_trial += 'MERGE (gard)-[:gard_in]->(trial)'
                            db.run(cypher_add_trial)
                            running = False

                    except:
                        pass
    
    lock.acquire()
    print('Finishing up Clinical Trial Database Creation...')
    lock.release()

    for idx in range(len(data_model.additional_class_fields)):     
        apoc_cypher = 'MATCH (x:{tag}) WITH '.format(tag=data_model.additional_class_names[idx])
        for idy in range(len(data_model.additional_class_fields[idx])):
            apoc_cypher += 'toLower(x.{name}) AS label{name}, '.format(name=data_model.additional_class_fields[idx][idy])
        apoc_cypher += 'COLLECT(x) AS nodes CALL apoc.refactor.mergeNodes(nodes, {properties:"overwrite",mergeRels:true}) YIELD node RETURN *'
        db.run(apoc_cypher)
    
    apoc_cypher = 'MATCH (x:GARD)'
    apoc_cypher += ' WITH COLLECT(x) AS nodes CALL apoc.refactor.rename.nodeProperty("GardName", "GARDName", nodes) YIELD total RETURN true'
    db.run(apoc_cypher)
    
    apoc_cypher = 'MATCH (x:ClinicalTrial) WITH '
    apoc_cypher += 'toLower(x.NCTId) AS nct, '
    apoc_cypher += 'COLLECT(x) AS nodes CALL apoc.refactor.mergeNodes(nodes, {properties:"overwrite",mergeRels:true}) YIELD node RETURN *'
    db.run(apoc_cypher)

    now = date.today()
    now = now.strftime("%m/%d/%y")
    now = "\"{now}\"".format(now=now)
    apoc_cypher = 'MATCH (x:ClinicalTrial) SET x.DateCreated = {now} RETURN x'.format(now=now)
    db.run(apoc_cypher)
    
    lock.acquire() 
    print('CLINICAL TRIAL DATABASE CREATED')
    lock.release()
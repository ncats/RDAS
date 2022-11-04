import configparser
import datetime
from http import client
from neo4j import GraphDatabase
from csv import DictReader
import load_neo4j_functions
import data_model
from datetime import date

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
    def main():
        connection = GraphDatabase.driver(uri='bolt://localhost:7687', auth=('neo4j', 'test'))
        session = connection.session()
        
        # iterate through list of diseases/conditions
        with open('conditions_matched.csv', 'r') as read_obj:
            gard_matches = DictReader(read_obj)
            for gard_mapping in gard_matches:
                try:
                    # extract data from mapping
                    GARDId = gard_mapping['gard_id']
                    GARD_name = gard_mapping['gard_name'].replace('\\','\\\\').replace('\'','\\\'').replace('\"','\\"')
                    CT_name = gard_mapping['disease_name']
                    
                    # create gard condition node
                    cypher_create_gard = 'MERGE (gard:GARD{GardName: \"' + GARD_name + '\", GARDId: \"' + GARDId + '\"})'
                    session.run(cypher_create_gard)
                    print(GARDId + ':\t' + GARD_name)
                    
                    # add each clinical trial to neo4j
                    trials = load_neo4j_functions.nctid_list(CT_name)
                    print('\tnum trials:', len(trials))
                    print('\t',end='')
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
                            response_trial_exists = session.run(cypher_trial_exists)

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

                                session.run(cypher_add_trial)
                                session.close()

                                if len(clinical_trial_data_string) == 0:
                                    print('\n\ttrial:',trial,'has no data')
                                
                                # generate data for additional classes
                                additional_class_data = list()
                                for class_fields in data_model.additional_class_fields:
                                    additional_class_data.append(load_neo4j_functions.data_string(full_trial, class_fields))
                                    
                                # cypher query to create and attach additional class nodes
                                session = connection.session()
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
                                    session.run(b)
                                    session.close()
                                    running = False
                                #data_model.additional_class_variable_names[j]
                                print('+',end='')
                                
                            # if node exists
                            else:
                                
                                # attach to gard node
                                session = connection.session()
                                cypher_add_trial = cypher_add_trial_base + 'MATCH (trial:ClinicalTrial) WHERE trial.NCTId = \'' + trial + '\''
                                cypher_add_trial += 'MERGE (gard)-[:gard_in]->(trial)'
                                session.run(cypher_add_trial)
                                session.close()
                                running = False
                                print('.',end='')

                        except:
                            pass

                        

                session.close()
        
        session = connection.session()
        for idx in range(len(data_model.additional_class_fields)):     
            apoc_cypher = 'MATCH (x:{tag}) WITH '.format(tag=data_model.additional_class_names[idx])
            for idy in range(len(data_model.additional_class_fields[idx])):
                apoc_cypher += 'toLower(x.{name}) AS label{name}, '.format(name=data_model.additional_class_fields[idx][idy])
            apoc_cypher += 'COLLECT(x) AS nodes CALL apoc.refactor.mergeNodes(nodes, {properties:"overwrite",mergeRels:true}) YIELD node RETURN *'
            session.run(apoc_cypher)
        
        session = connection.session()
        apoc_cypher = 'MATCH (x:GARD)'
        apoc_cypher += ' WITH COLLECT(x) AS nodes CALL apoc.refactor.rename.nodeProperty("GardName", "GARDName", nodes) YIELD total RETURN true'
        session.run(apoc_cypher)
        
        session = connection.session()
        apoc_cypher = 'MATCH (x:ClinicalTrial) WITH '
        apoc_cypher += 'toLower(x.NCTId) AS nct, '
        apoc_cypher += 'COLLECT(x) AS nodes CALL apoc.refactor.mergeNodes(nodes, {properties:"overwrite",mergeRels:true}) YIELD node RETURN *'
        session.run(apoc_cypher)

        session = connection.session()
        now = date.today()
        now = now.strftime("%m/%d/%y")
        now = "\"{now}\"".format(now=now)
        print(now)
        apoc_cypher = 'MATCH (x:ClinicalTrial) SET x.DateCreated = {now} RETURN x'.format(now=now)
        session.run(apoc_cypher)
    main()
import os
import re
import sys
import spacy
from spacy.matcher import Matcher
import requests
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

def condition_map(db):
    remove_words = ['and','or','of','the','and/or']

    nlp = spacy.load('en_core_web_lg')
    conditions = db.run('MATCH (x:Condition) RETURN x.Condition, ID(x)')
    diseases = db.run('MATCH (x:GARD) RETURN x.GARDId, x.GARDName')
    cond_docs = dict()
    disease_docs = dict()
    
    for cond in conditions:
        cond_id = cond['ID(x)']
        cond_name = cond['x.Condition']
        doc = nlp(cond_name)
        tokens = [i.text.lower() for i in doc if not i.is_punct and i.text.lower() not in remove_words]
        cond_docs[cond_id] = tokens
    
    for disease in diseases:
        gard_id = disease['x.GARDId']
        name = disease['x.GARDName']
        doc = nlp(name)
        tokens = [i.text.lower() for i in doc if not i.is_punct and i.text.lower() not in remove_words]
        disease_docs[gard_id] = tokens
    
    for k,v in disease_docs.items():
        for k2,v2 in cond_docs.items():
            check = all(item in v2 for item in v)
            if check:
                db.run('MATCH (x:GARD) WHERE x.GARDId = \"{gard_id}\" MATCH (z:Condition) WHERE ID(z) = {cond_id} MERGE (x)<-[:mapped_to_gard]-(z) RETURN TRUE'.format(cond_id=k2,gard_id=k))

def drug_normalize(drug):
    print(drug)
    new_val = drug.encode("ascii", "ignore")
    updated_str = new_val.decode()
    updated_str = re.sub('\W+',' ', updated_str)
    print(updated_str)
    return updated_str

def create_drug_connection(db,rxdata,drug_id):
    rxnormid = rxdata['RxNormID']
    db.run('MATCH (x:Intervention) WHERE ID(x)={drug_id} MERGE (y:Drug {{RxNormID:{rxnormid}}}) MERGE (y)<-[:has_participant]-(x)'.format(rxnormid=rxnormid, drug_id=drug_id))

    for k,v in rxdata.items():
        key = k.replace(' ','')
        db.run('MERGE (y:Drug {{RxNormID:{rxnormid}}}) WITH y MATCH (x:Intervention) WHERE ID(x)={drug_id} MERGE (y)<-[:has_participant]-(x) SET y.{key} = {value}'.format(rxnormid=rxdata['RxNormID'], drug_id=drug_id, key=key, value=v))


def get_rxnorm_data(drug):
    rq = 'https://rxnav.nlm.nih.gov/REST/rxcui.json?name={drug}&search=2'.format(drug=drug)
    response = requests.get(rq)
    try:
        rxdata = dict()
        response = response.json()['idGroup']['rxnormId'][0]
        rxdata['RxNormID'] = response

        rq2 = 'https://rxnav.nlm.nih.gov/REST/rxcui/{rxnormid}/allProperties.json?prop=codes+attributes+names+sources'.format(rxnormid=response)
        response = requests.get(rq2)
        response = response.json()['propConceptGroup']['propConcept']

        for r in response:
            if r['propName'] in rxdata:
                rxdata[r['propName']].append(r['propValue'])
            else:
                rxdata[r['propName']] = [r['propValue']]

        return rxdata

    except KeyError as e:
        return
    except ValueError as e:
        print('ERROR')
        print(drug)
        return


def nlp_to_drug(db,doc,matches,drug_name,drug_id):
    for match_id, start, end in matches:
        span = doc[start:end].text
        rxdata = get_rxnorm_data(span.replace(' ','+'))
            
        if rxdata:
            create_drug_connection(db,rxdata,drug_id)
        else:
            print('Map to RxNorm failed for intervention name: {drug_name}'.format(drug_name=drug_name))

def rxnorm_map(db):
    print('Starting RxNorm data mapping to Drug Interventions')
    nlp = spacy.load('en_ner_bc5cdr_md')
    pattern = [{'ENT_TYPE':'CHEMICAL'}]
    matcher = Matcher(nlp.vocab)
    matcher.add('DRUG',[pattern])

    results = db.run('MATCH (x:Intervention) WHERE x.InterventionType = "Drug" RETURN x.InterventionName, ID(x)')

    for idx,res in enumerate(results.data()):
        print(idx)
        drug_id = res['ID(x)']
        drug = res['x.InterventionName']
        drug = drug_normalize(drug)
        drug_url = drug.replace(' ','+')
        rxdata = get_rxnorm_data(drug_url)

        if rxdata:
            create_drug_connection(db, rxdata, drug_id)
        else:
            doc = nlp(drug)
            matches = matcher(doc)
            nlp_to_drug(db,doc,matches,drug,drug_id)

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
    condition_map(db)
    exit()
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
    rxnorm_map(db) 
    condition_map(db)
    
    if update:
        trigger_email(db, "clinical")
    else:
        db.setConf('DATABASE', 'pubmed_finished', 'True')
        db.setConf('DATABASE', 'pubmed_progress', '')
    
    print('Clinical Trial Database Update Finished')
        

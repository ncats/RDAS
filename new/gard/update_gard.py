import os
import requests
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from datetime import datetime, date
from http import client
from neo4j import GraphDatabase
from csv import DictReader
import configparser
import threading
import pandas as pd
from ast import literal_eval
lock = threading.Lock()

def create_relationship_query(db, current, node, has_parent=False):
    if has_parent:
        query = '''
        MATCH (x:GARD {GardId: $parent}), (y:GARD {GardId: $current})
        MERGE (x)<-[i:subClassOf]-(y)
        RETURN y
        '''
        params = {
        "current":current,
        "parent":node
        }
    else:
        query = '''
        MATCH (x:GARD {GardId: $current}), (y:GARD {GardId: $child})
        MERGE (x)<-[i:subClassOf]-(y)
        RETURN y
        '''
        params = {
        "current":current,
        "child":node,
        }

    return db.run(query, args=params).single().value()    

def create_relationship(db, data):
    '''if data['RootTerm'] == 'GARD to be classified':
       return
    '''
    current = data['GardID']
    parent = data['Parent']
    child = data['Child']

    try:
        if len(parent) > 0:
            create_relationship_query(db, current, parent, has_parent=True)
    except:
        pass
    try:
        if len(child) > 0:
            create_relationship_query(db, current, child, has_parent=False)
    except:
        pass

def create_disease_node(db, data):
    query = '''
    MERGE (d:GARD {GardId:$gard_id}) 
    ON CREATE SET
    d.GardName = $name,
    d.GardId = $gard_id,
    d.ClassificationLevel = $classlvl, 
    d.DisorderType = $disordertype, 
    d.Synonyms = $syns
    RETURN d
    '''

    if type(data[4]) == float:
        data[4] = []

    else:
        data[4] = data[4].replace('\'','\\\'')
        data[4] = data[4].replace(',','\',\'')
        data[4] = data[4].replace('[','[\'')
        data[4] = data[4].replace(']','\']')
        data[4] = literal_eval(data[4])
        data[4] = [ele.strip() for ele in data[4]]

    data[2] = data[2].replace('[','')
    data[2] = data[2].replace(']','')

    params = {
    "name":data[3],
    "gard_id":data[0],
    "classlvl":data[1], 
    "disordertype":data[2], 
    "syns":data[4]
    }

    return db.run(query, args=params).single().value()

def retrieve_gard_data(db, compare=None):
    print("Retrieving GARD data from Palantir")
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o new/gard/GARD.csv https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.ec51a84a-3b60-44d8-9625-3fc2a2b1d481/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=db.getConf("CREDENTIALS","palantir"))
    os.system(command)
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o new/gard/GARD_classification.csv https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.363c2a9e-3213-4e66-a5db-052af2309f02/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=db.getConf("CREDENTIALS","palantir"))
    os.system(command)
    
    #MAY HAVE TO FULLY REBUILD NEO4J ANYTIME THERE IS A CHANGE BETWEEN THE 2 FILES
    if compare:
        gard_new = pd.read_csv(os.path.dirname(workspace) + '\\gard\\GARD.csv', index_col=False)
        classification_new = pd.read_csv(os.path.dirname(workspace) + '\\gard\\GARD_classification.csv', index_col=False)
        gard_old = compare['gard']
        classification_old = compare['classification']
        gard_diff = pd.concat([gard_new, gard_old]).drop_duplicates(keep=False)
        classification_diff = pd.concat([classification_new, classification_old]).drop_duplicates(keep=False)
        
        return {'gard':gard_diff, 'classification':classification_diff}
    
def generate(db, data):
    print("Building new GARD connections")
    gard = data['gard']
    classification = data['classification']
    r,c = gard.shape
    for i in range(r):
        row = gard.iloc[i]
        data = row.to_list()
        create_disease_node(db, data)

    r,c = classification.shape
    for i in range(r):
        row = classification.iloc[i]
        data = row.to_dict()
        create_relationship(db, data)
    
def main(db, update=False):
    now = datetime.now().strftime("%m/%d/%y")
    if db.getConf('DATABASE','gard_update') == now:
            print('GARD DB already up to date')
            return
        
    if update:
        gard = pd.read_csv(os.path.dirname(workspace) + '\\gard\\GARD.csv', index_col=False)
        classification = pd.read_csv(os.path.dirname(workspace) + '\\gard\\GARD_classification.csv', index_col=False)
        data = retrieve_gard_data(db, compare={'gard':gard, 'classification':classification})
        generate(db, data)
    else:
        retrieve_gard_data(db)
        gard = pd.read_csv(os.path.dirname(workspace) + '\\gard\\GARD.csv', index_col=False)
        classification = pd.read_csv(os.path.dirname(workspace) + '\\gard\\GARD_classification.csv', index_col=False)
        data = {'gard':gard, 'classification':classification}
        generate(db, data)
        
    db.setConf('DATABASE','gard_update', now)
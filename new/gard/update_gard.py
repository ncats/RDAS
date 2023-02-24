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
    '''
    Creates GARD disease relationships with a specific direction
    '''
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
    '''
    Creates the relationships connecting to each GARD disease using the classification list
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
    '''
    Creates GARD Disease node in the Neo4j
    '''
    query = '''
    MERGE (d:GARD {GardId:$gard_id}) 
    ON CREATE SET
    d.GardName = $name,
    d.GardId = $gard_id,
    d.DataSource = $mapping_type,
    d.DataSourceId = $mapping_id,
    d.ClassificationLevel = $classlvl, 
    d.DisorderType = $disordertype, 
    d.Synonyms = $syns
    RETURN d
    '''

    if type(data[6]) == float:
        data[6] = []

    # Turn synonyms into a python list
    else:
        data[6] = data[6].replace('\'','\\\'')
        data[6] = data[6].replace(',','\',\'')
        data[6] = data[6].replace('[','[\'')
        data[6] = data[6].replace(']','\']')
        data[6] = literal_eval(data[6])
        data[6] = [ele.strip() for ele in data[6]]

    # Remove brackets from DisorderType
    data[4] = data[4].replace('[','')
    data[4] = data[4].replace(']','')

    params = {
    "name":data[5],
    "gard_id":data[0],
    "mapping_type":data[1],
    "mapping_id":data[2],
    "classlvl":data[3], 
    "disordertype":data[4], 
    "syns":data[6]
    }

    return db.run(query, args=params).single().value()

def retrieve_gard_data(db):
    '''
    Retrieves GARD disease files from Palantir workspace
    '''
    print("Retrieving GARD data from Palantir")
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o new/gard/GARD.csv https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.ec51a84a-3b60-44d8-9625-3fc2a2b1d481/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=os.environ['PALANTIR_KEY'])
    os.system(command)
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o new/gard/GARD_classification.csv https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.363c2a9e-3213-4e66-a5db-052af2309f02/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=os.environ['PALANTIR_KEY'])
    os.system(command)
    
def generate(db, data):
    '''
    Loops through all GARD diseases and creates all the nodes and relationships
    '''
    print("Building new GARD connections")
    
    # Erase database so it can be recreated
    db.run('MATCH ()-[r]-() DELETE r')
    db.run('MATCH (n) DELETE n')
    
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
        
    retrieve_gard_data(db)
    gard = pd.read_csv(os.path.dirname(workspace) + '\\gard\\GARD.csv', index_col=False)
    classification = pd.read_csv(os.path.dirname(workspace) + '\\gard\\GARD_classification.csv', index_col=False)
    data = {'gard':gard, 'classification':classification}
    generate(db, data)
    
    if not update:
        db.setConf('DATABASE', 'gard_finished', 'True')
        
    db.setConf('DATABASE','gard_update', now)

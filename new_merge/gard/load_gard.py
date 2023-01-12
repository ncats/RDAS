import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from datetime import date
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
        MERGE (x)-[i:subClassOf]->(y)
        RETURN y
        '''
        params = {
        "current":current,
        "parent":node
        }
    else:
        query = '''
        MATCH (x:GARD {GardId: $current}), (y:GARD {GardId: $child})
        MERGE (x)-[i:subClassOf]->(y)
        RETURN y
        '''
        params = {
        "current":current,
        "child":node,
        }

    return db.run(query, args=params).single().value()    

def create_relationship(db, data):
    if data['RootTerm'] == 'GARD to be classified':
       return

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

def main(db):
    gard = pd.read_csv(os.path.dirname(workspace) + '\\gard\\GARD.csv', index_col=False)
    r,c = gard.shape
    for i in range(r):
        row = gard.iloc[i]
        data = row.to_list()
        create_disease_node(db, data)

    classification = pd.read_csv(os.path.dirname(workspace) + '\\gard\\GARD_classification.csv', index_col=False)
    r,c = classification.shape
    for i in range(r):
        row = classification.iloc[i]
        data = row.to_dict()
        create_relationship(db, data)
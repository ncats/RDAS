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

def create_disease_node(db, data, xrefs): # Include xrefs into GARD node instead of seperate node
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
    d.Synonyms = $syns,
    d.Orphanet = $orpha,
    d.ICD10 = $icd10,
    d.UMLS = $umls,
    d.OMIM = $omim,
    d.SNOMEDCT = $snomed,
    d.DiseaseOntology = $diseaseontology,
    d.MeSH = $mesh,
    d.MedDRA = $meddra,
    d.GeneticAlliance = $genetic,
    d.ICD11 = $icd11,
    d.GeneticsHomeReference = $ghr,
    d.ICD10CM = $icd10cm
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

    results = xrefs.loc[xrefs['GardID'] == data[0]]
    results = results.groupby('XrefSource')['SourceID'].apply(list).to_dict()

    params = {
    "name":data[5],
    "gard_id":data[0],
    "mapping_type":data[1],
    "mapping_id":data[2],
    "classlvl":data[3], 
    "disordertype":data[4], 
    "syns":data[6],
    "orpha":results['Orphanet'] if 'Orphanet' in results else None,
    "icd10":results['ICD-10'] if 'ICD-10' in results else None,
    "umls":results['UMLS'] if 'UMLS' in results else None,
    "omim":results['OMIM'] if 'OMIM' in results else None,
    "snomed":results['SNOMED-CT'] if 'SNOMED-CT' in results else None,
    "diseaseontology":results['DiseaseOntology'] if 'DiseaseOntology' in results else None,
    "mesh":results['MeSH'] if 'MeSH' in results else None,
    "meddra":results['MedDRA'] if 'MedDRA' in results else None,
    "genetic":results['GeneticAlliance'] if 'GeneticAlliance' in results else None,
    "icd11":results['ICD-11'] if 'ICD-11' in results else None,
    "ghr":results['GeneticsHomeReference'] if 'GeneticsHomeReference' in results else None,
    "icd10cm":results['ICD-10-CM'] if 'ICD-10-CM' in results else None
    }

    db.run(query, args=params).single().value()

def retrieve_gard_data(db):
    '''
    Retrieves GARD disease files from Palantir workspace
    '''
    print("Retrieving GARD data from Palantir")
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o {PATH} https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.ec51a84a-3b60-44d8-9625-3fc2a2b1d481/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=os.environ['PALANTIR_KEY'], PATH=os.path.join(os.path.dirname(workspace), 'gard', 'GARD.csv'))
    os.system(command)
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o {PATH} https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.363c2a9e-3213-4e66-a5db-052af2309f02/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=os.environ['PALANTIR_KEY'], PATH=os.path.join(os.path.dirname(workspace), 'gard', 'GARD_classification.csv'))
    os.system(command)
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o {PATH} https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.95f22ad1-90fc-48a1-9c40-c4c632f9c310/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=os.environ['PALANTIR_KEY'], PATH=os.path.join(os.path.dirname(workspace), 'gard', 'GARD_xrefs.csv'))
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
        row = row.to_list()
        create_disease_node(db, row, data['xrefs'])

    r,c = classification.shape
    for i in range(r):
        row = classification.iloc[i]
        row = row.to_dict()
        create_relationship(db, row)
    
def main(db, update=False):
    now = datetime.now().strftime("%m/%d/%y")
    if db.getConf('DATABASE','gard_update') == now:
            print('GARD DB already up to date')
            return
        
    retrieve_gard_data(db)
    gard = pd.read_csv(os.path.join(os.path.dirname(workspace), 'gard', 'GARD.csv'), index_col=False)
    classification = pd.read_csv(os.path.join(os.path.dirname(workspace), 'gard', 'GARD_classification.csv'), index_col=False)
    xrefs = pd.read_csv(os.path.join(os.path.dirname(workspace), 'gard', 'GARD_xrefs.csv'), index_col=False)
    data = {'gard':gard, 'classification':classification, 'xrefs':xrefs}
    generate(db, data)
    
    if not update:
        db.setConf('DATABASE', 'gard_finished', 'True')
        
    db.setConf('DATABASE','gard_update', now)

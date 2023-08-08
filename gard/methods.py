import os
import sysvars
from AlertCypher import AlertCypher
import re
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
    else:
        data[6] = data[6].split('|')
        data[6] = [term.replace(',',';') for term in data[6]]

    if type(data[4]) == float:
        data[4] = []
    else:
        data[4] = data[4].split('|')

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


def retrieve_gard_data():
    '''
    Retrieves GARD disease files from Palantir workspace
    '''
    print("Retrieving GARD data from Palantir")
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o {PATH} https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.ec51a84a-3b60-44d8-9625-3fc2a2b1d481/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=os.environ['PALANTIR_KEY'], PATH=f'{sysvars.gard_files_path}GARD.csv')
    os.system(command)
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o {PATH} https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.363c2a9e-3213-4e66-a5db-052af2309f02/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=os.environ['PALANTIR_KEY'], PATH=f'{sysvars.gard_files_path}GARD_classification.csv')
    os.system(command)
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o {PATH} https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.95f22ad1-90fc-48a1-9c40-c4c632f9c310/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=os.environ['PALANTIR_KEY'], PATH=f'{sysvars.gard_files_path}GARD_xrefs.csv')
    os.system(command)
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o {PATH} https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.32bdc41d-a8eb-4101-a2e7-0701a58c354b/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=os.environ['PALANTIR_KEY'], PATH=f'{sysvars.gard_files_path}GARD_genes.csv')
    os.system(command)
    command = 'curl -X GET -H "Authorization: Bearer {PALANTIR_KEY}" -o {PATH} https://nidap.nih.gov/foundry-data-proxy/api/dataproxy/datasets/ri.foundry.main.dataset.2b15e594-f3e7-4abc-a9b1-e067eedcc51e/branches/master/csv?includeColumnNames=true'.format(PALANTIR_KEY=os.environ['PALANTIR_KEY'], PATH=f'{sysvars.gard_files_path}GARD_phenotypes.csv')
    os.system(command)

def add_genes(db, data):
    query = '''
    MATCH (g:GARD {GardId:$gardId})
    MERGE (d:Gene {GeneIdentifier:$geneId})
    ON CREATE SET
    d.GeneSymbol = $gene_symbol,
    d.GeneSynonyms = $gene_syns,
    d.GeneTitle = $gene_title,
    d.GeneType = $gene_type,
    d.Locus = $locus,
    d.OMIM = $omim,
    d.Ensembl = $ensembl,
    d.IUPHAR = $iuphar,
    d.Swissprot = $swissprot,
    d.Reactome = $reactome
    MERGE (g)-[r:associated_with_gene]->(d)
    ON CREATE SET
    r.AssociationType = $assoc_type,
    r.AssociationStatus = $assoc_status,
    r.Reference = $reference
    RETURN TRUE
    '''
    
    try:
        data['GeneSynonym'] = data['GeneSynonym'].strip('][').split(', ')
    except Exception as e:
        print(e)
        pass

    try:
        data['Reference'] = data['Reference'].strip('][').split(', ')
    
    except Exception as e:
        print(e)
        pass

    params = {
    'gardId':data['GardID'],
    'geneId':data['GeneIdentifier'],
    "gene_symbol":data['GeneSymbol'] if not type(data['GeneSymbol']) == float else None,
    "gene_syns":data['GeneSynonym'] if not type(data['GeneSynonym']) == float else None,
    "gene_title":data['GeneTitle'] if not type(data['GeneTitle']) == float else None,
    "gene_type":data['GeneType'] if not type(data['GeneTitle']) == float else None,
    "locus":data['Locus'] if not type(data['Locus']) == float else None,
    "assoc_type":data['AssociationType'] if not type(data['AssociationType']) == float else None,
    "assoc_status":data['AssociationStatus'] if not type(data['AssociationStatus']) == float  else None,
    "reference":data['Reference'] if not type(data['Reference']) == float else None,
    "omim":data['OMIM'] if not type(data['OMIM']) == float else None,
    "ensembl":data['Ensembl'] if not type(data['Ensembl']) == float else None,
    "iuphar":data['IUPHAR'] if not type(data['IUPHAR']) == float else None,
    "swissprot":data['SwissProt'] if not type(data['SwissProt']) == float else None,
    "reactome":data['Reactome'] if not type(data['Reactome']) == float else None
    }

    db.run(query, args=params).single().value()


def add_phenotypes(db, data):
    query = '''
    MATCH (g:GARD {GardId:$gardId})
    MERGE (d:Phenotype {HPOId:$HPOId})
    ON CREATE SET
    d.HPOTerm = $hpo_term,
    d.Sex = $sex,
    d.Onset = $onset,
    d.Modifier = $mod,
    d.Online = $online
    MERGE (g)-[r:has_phenotype]->(d)
    ON CREATE SET
    r.ValidationStatus = $validation_status,
    r.HPOFrequency = $hpo_freq,
    r.Evidence = $evidence,
    r.Reference = $reference
    RETURN TRUE
    '''

    try:
        data['Reference'] = data['Reference'].strip('][').split(', ')
    except Exception as e:
        print(e)
        pass

    try:
        data['Modifier'] = data['Modifier'].split(';')
    except Exception as e:
        pass

    try:
        temp = data['Online']
        if temp == 'y':
            temp = True
        else:
            temp = False
        data['Online'] = temp
    except Exception as e:
        pass

    try:
        temp = data['ValidationStatus']
        if temp == 'y':
            temp = True
        else:
            temp = False
        data['ValidationStatus'] = temp
    except Exception as e:
        pass


    params = {
    'gardId':data['GardID'],
    'HPOId':data['HPOId'],
    "hpo_term":data['HPOTerm'] if not type(data['HPOTerm']) == float else None,
    "hpo_freq":data['HPOFrequency'] if not type(data['HPOFrequency']) == float else None,
    "evidence":data['Evidence'] if not type(data['Evidence']) == float else None,
    "sex":data['Sex'] if not type(data['Sex']) == float else None,
    "onset":data['Onset'] if not type(data['Onset']) == float else None,
    "mod":data['Modifier'] if not type(data['Modifier']) == float else None,
    "reference":data['Reference'] if not type(data['Reference']) == float else None,
    "online":data['Online'] if not type(data['Online']) == float else None,
    "validation_status":data['ValidationStatus'] if not type(data['ValidationStatus']) == float else None,
    }

    db.run(query, args=params).single().value()

def generate(db, data):
    '''
    Loops through all GARD diseases and creates all the nodes and relationships
    '''
    
    print("Building new GARD nodes")
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

    print('Building GARD relationships')
    r,c = classification.shape
    for i in range(r):
        row = classification.iloc[i]
        row = row.to_dict()
        create_relationship(db, row)
    
    print('Building Gene data')
    genes = data['genes']
    r,c = genes.shape
    for i in range(r):
        row = genes.iloc[i]
        row = row.to_dict()
        add_genes(db, row)
    
    print('Building Phenotype data')
    phenotypes = data['phenotypes']
    r,c = phenotypes.shape
    for i in range(r):
        row = phenotypes.iloc[i]
        row = row.to_dict()
        add_phenotypes(db, row)
    

from neo4j import GraphDatabase
import pandas as pd
import load_neo4j_functions
from mapper.bin import AbstractMap
import os
import time

connection = GraphDatabase.driver(uri='bolt://localhost:7687', auth=('neo4j', 'test'))
session = connection.session()

if not os.path.exists('mapper/bin/data/output/abstract_matches.csv'):
    get_trials_query = 'MATCH (a:Article)--(d:Disease) RETURN a.abstractText,a.pubmed_id,d.gard_id'
    trials = session.run(get_trials_query)

    ll = list()
    for trial in trials:
        ll.append(trial.data())

    df = pd.DataFrame(ll)
    df = df.rename({'a.pubmed_id':'PMID','a.abstractText':'abstractText','d.gard_id':'GARDId'}, axis=1)


    df.to_csv('mapper/bin/data/input/neo4j_pubmed.csv')
    mapper = AbstractMap.AbstractMap()
    mapper._match('neo4j_pubmed.csv','neo4j_rare_disease_list.json',IDcol='PMID',TEXTcols=['abstractText'])
else:
    connection = GraphDatabase.driver(uri='bolt://localhost:7687', auth=('neo4j', 'test'))
    session = connection.session()
    update_0 = session.run('match (a:Article)-[r:MENTIONED_IN]-(d:Disease) set r.original_derived=true return a')
    
    df = pd.read_csv('mapper/bin/data/output/abstract_matches.csv', index_col=False)
    IDS = df.ID.unique()
    tot = len(IDS)
    cnt = 1
    
    for ID in IDS:
        print(str(cnt)+'/'+str(tot))
        rows = df.loc[df['ID'] == ID][['GARD_id','Matched_Word']]
        rows = rows.drop_duplicates()
        clean_ID = str(int(ID))
        
        for row in rows.iterrows():
            gard_id = row[1].values[0]
            update = session.run('MATCH (g:Disease{gard_id: $gard_id}) MERGE (n:Article {pubmed_id: $clean_ID}) MERGE (g)-[r:MENTIONED_IN]->(n) SET r.normmap_derived=true RETURN n,r,g',clean_ID=clean_ID,gard_id=gard_id)
             
        cnt += 1
    
    session.close()
    connection.close()

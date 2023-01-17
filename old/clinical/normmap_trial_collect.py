from neo4j import GraphDatabase
import pandas as pd
import os

connection = GraphDatabase.driver(uri='bolt://localhost:7687', auth=('neo4j', 'test'))
session = connection.session()

get_trials_query = 'MATCH (gard:GARD)-[r:clinical_trial]->(trial:ClinicalTrial)--(cond:Condition) WHERE r.normmap_derived = true RETURN trial.NCTId,trial.OfficialTitle,trial.BriefTitle,trial.BriefSummary,cond.Condition,gard.GARDId'
trials = session.run(get_trials_query)

ll = list()
for trial in trials:
    ll.append(trial.data())

df = pd.DataFrame(ll)
df = df.rename({'trial.NCTId':'NCTId','trial.OfficialTitle':'OfficialTitle','trial.BriefTitle':'BriefTitle','trial.BriefSummary':'BriefSummary','cond.Condition':'Condition','gard.GARDId':'GARDId'}, axis=1)
df = df[['NCTId','GARDId','OfficialTitle','BriefTitle','BriefSummary','Condition']]

df.to_csv('normmap_derived_clinical_trials.csv', index=False)


from neo4j import GraphDatabase
import pandas as pd
import load_neo4j_functions
from mapper.bin import AbstractMap
import os
from datetime import datetime

connection = GraphDatabase.driver(uri='bolt://localhost:7687', auth=('neo4j', 'test'))
session = connection.session()

# Change relationship name and direction of Clinical Trial to Condition
update = session.run('MATCH (c:ClinicalTrial)-[r:includes_conditions]->(g:Condition) MERGE (g)-[:investigates_condition]->(c) DELETE r')

#Change relationship name and direction of Clinical Trial to Reference
update = session.run('MATCH (c:ClinicalTrial)-[r:has_references]->(ref:Reference) MERGE (ref)-[:is_about]->(c) DELETE r')

#If intervention is a drug, create a new node with drug info with a different node/relationship. Delete old data
update = session.run('MATCH (c:ClinicalTrial)-[rel:uses_interventions]-(i:Intervention) WITH c,rel,i FOREACH ( CREATE (c)-[:investigates_patient_administrated_with]->(d:Drug {InterventionBrowseLeafId: i.InterventionBrowseLeafId, InterventionBrowseLeafName: i.InterventionBrowseLeafName, InterventionBrowseLeafRelevance: i.InterventionBrowseLeafRelevance, InterventionMeshId: i.InterventionMeshId, InterventionMeshTerm: i.InterventionMeshTerm, InterventionName: i.InterventionName, InterventionType: i.InterventionType} DELETE i)IN CASE WHEN i.InterventionType = \'[Drug]\') THEN [1] ELSE [] END')

#Add age of each node/relationship
update = session.run('MATCH')

from neo4j import GraphDatabase
import matplotlib.pyplot as plt
import numpy as np
import datetime

# connect to neo4j database
connection = GraphDatabase.driver(uri='bolt://localhost:7687', auth=('neo4j', 'tgcbf'))
session = connection.session()

# query def
query = 'MATCH (n:ClinicalTrial) RETURN n.Phase'
response = session.run(query)

# count by phase
count = dict()
count[1], count[2], count[3], count[4] = 0, 0, 0, 0
num_null = 0
for elm in response:
    print(elm)
    if elm == None or elm[0] == None:
        num_null += 1
    else:
        print(elm[0])
        num_phases = 0
        for e in elm[0]:
            for phase in range(1,5):
                if str(phase) in e:
                    count[phase] += 1
                    print('\t-->',phase)
                    num_phases += 1
        if num_phases == 0:
            num_null += 1
        elif num_phases > 1:
            print(elm[0])

# results
for phase in range(1,5):
    print('phase:', phase, 'has', count[phase], 'clinical trials')
print(num_null,'null')

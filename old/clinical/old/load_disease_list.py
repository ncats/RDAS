import requests
from neo4j import GraphDatabase

'''
downloads list of rare diseases from clinical trials website
'''

# task description
fields = ['NCTId','BriefTitle']
read_file = open('rare_diseases_list.txt', 'r', encoding="utf-8")

# connect to neo4j
connection = GraphDatabase.driver(uri = 'bolt://localhost:7687', auth = ('neo4j', 'tgcbf'))
session = connection.session()

# for each disease
diseases = read_file.readlines()[:3]
NCTids = set()
for disease in diseases:
    
    # add node for disease
    session.run('CREATE (:Disease{disease_name: \'' + disease[:-1] + '\'})')
    
    # generate query request
    query = 'https://clinicaltrials.gov/api/query/study_fields?expr=%22'
    query += disease.replace(' ', '+') + '%22&fields=' + "%2C".join(fields)
    query += '&min_rnk=1&max_rnk=1000&fmt=csv'

    # get Clinical Trials API response
    x = requests.get(query)

# close files
read_file.close()

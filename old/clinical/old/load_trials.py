import requests
from neo4j import GraphDatabase

'''
for each rare disease in rare_diseases_list.txt, extracts all fields from each 
clinical trial and loads data into neo4j
'''

# connect to neo4j database
connection = GraphDatabase.driver(uri='bolt://localhost:7687', auth=('neo4j', 'tgcbf'))
session = connection.session()

# file with names of all rare diseases on Clinical Trials website
read_file = open('rare_diseases_list.txt', 'r', encoding="utf-8")

# for each rare disease
diseases = read_file.readlines()
read_file.close()
for disease in diseases:
    
    # query clinical trials API to find all related clinical trials
    find_trials_query = 'https://clinicaltrials.gov/api/query/study_fields?expr=%22'
    find_trials_query += disease.replace(' ', '+') + '%22&fields=NCTId&min_rnk=1&max_rnk=1000&fmt=csv'

    # clinical trials API response
    find_trials_response = requests.get(find_trials_query)
    trials = [line.split(',')[1][1:-1] for line in find_trials_response.text.splitlines()[11:]]
    
    # query neo4j to create node for disease
    session.run('CREATE (:Disease{disease_name: \'' + disease[:-1] + '\'})')
    
    # attach disease node to all clinical trials in list
    for trial in trials:
        
        # generate neo4j query to attach disease to clinical trial
        connect_command = 'MATCH (d:Disease) WHERE d.disease_name = \'' + disease[:-1] + '\''
        
        # neo4j query to check if clinical trial node already exists
        query_trial_exists = 'MATCH (c:Clinical_Trial) WHERE c.NCTId = \'' + trial + '\' RETURN COUNT(c)'
        response_trial_exists = session.run(query_trial_exists)

        # node doesn't exist, create new clinical trial node and connect
        if int([elm[0] for elm in response_trial_exists][0]) == 0:
            
            # query clinical trials API to get 
            full_trial_query = 'https://clinicaltrials.gov/api/query/full_studies?expr='
            full_trial_query += trial.replace(' ', '+') + '&min_rnk=1&max_rnk=1&fmt=xml'
            full_trial_response = requests.get(full_trial_query)

            # clean trial data
            data = list()
            for line in full_trial_response.text.splitlines():
                if '<Field Name="' in line:
                    line = line.split('\"')
                    data.append(line[1] + ': \'' + line[2][1:-8].replace('\'','\\\'') + '\'')
                    
            # query neo4j to create and attach clinical trial node
            connect_command += 'CREATE (d)-[:clinical_trial]->(:Clinical_Trial{' + ', '.join(data) + '})'
            abc = session.run(connect_command)
            
        # node exists, attach with disease node
        else:
            connect_command += 'MATCH (c:Clinical_Trial) WHERE c.NCTId = \'' + trial
            connect_command += '\' CREATE (d)-[:clinical_trial]->(c)'
            session.run(connect_command)

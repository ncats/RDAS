import sys
import os
workspace = os.path.dirname(os.path.abspath(__file__))
print(workspace)
sys.path.append(workspace)
from clinical import methods as rdas
import requests
from AlertCypher import AlertCypher
import pandas as pd
import string
from time import sleep
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline

tokenizer = AutoTokenizer.from_pretrained("dslim/bert-base-NER")
model = AutoModelForTokenClassification.from_pretrained("dslim/bert-base-NER", config={'output_hidden_states': False})
nlp = pipeline("ner", model=model, tokenizer=tokenizer)

def mask_name(name):
    name = name.rstrip(string.punctuation)
    name = name.title()
    entities = nlp(name)
    person_entities = [entity['word'] for entity in entities if entity['entity'] == 'B-PER' or entity['entity'] == 'I-PER']
    if person_entities == []:
        reversed_name = ' '.join(name.split()[::-1])
        entities = nlp(reversed_name)
        person_entities = [entity['word'] for entity in entities if entity['entity'] == 'B-PER' or entity['entity'] == 'I-PER']
    masked_name = ' '.join(person_entities) if person_entities else None
    masked_name = masked_name.replace(' ##', '') if masked_name is not None else None
    if masked_name != None:
             name=name.replace('"', '').replace("'", '')
             masked_name=', '.join([i.strip() for i in name.split(',') if len(i.strip())>=4 ])
    return masked_name

db = AlertCypher('test.clinical')

response = db.run('MATCH (x:ClinicalTrial) WHERE NOT EXISTS((x)--(:Investigator)) RETURN ID(x) as id, x.NCTId as nctid').data()
print(len(response))

for idx,res in enumerate(response):
    print(idx)
    #if idx < 80000:
    #    continue

    contacts = dict()
    locations = dict()
    nctid = res['nctid']
    id = res['id']

    full_trial_query = 'https://clinicaltrials.gov/api/query/full_studies?expr=' + f'{nctid}' + '&min_rnk=1&max_rnk=1&fmt=json'
        
    try:
        # Make the API request and parse the JSON response
        full_trial_response = requests.get(full_trial_query).json()

        # Use the parse_trial_fields function to flatten the nested structure
        full_trial = rdas.parse_trial_fields(full_trial_response)
    except Exception:
        sleep(60)
        try:
            full_trial_response = requests.get(full_trial_query).json()
            full_trial = rdas.parse_trial_fields(full_trial_response)
        except Exception:
            # Return None if there is an issue with the JSON response
            full_trial = None

    #if 'OverallOfficial' in full_trial:
    #    overall_official_list = full_trial['OverallOfficial']
    #else:
    #    continue

    #print(full_trial.keys())

    if not full_trial:
        continue

    if 'Location' in full_trial:
        location_list = full_trial['Location']
        for info in location_list:
            if 'LocationContactList' in info:
                info = info['LocationContactList']['LocationContact']
                for list_loc_contact in info:
                    if 'LocationContactName' in list_loc_contact and ('LocationContactPhone' in list_loc_contact or 'LocationContactEMail' in list_loc_contact):
                        locations[list_loc_contact['LocationContactName']] = list_loc_contact

    if 'CentralContact' in full_trial:
        contact_list = full_trial['CentralContact']
        for info in contact_list:
            if 'CentralContactName' in contact_list and ('CentralContactPhone' in contact_list or 'CentralContactEMail' in contact_list):
                contacts[info['CentralContactName']] = info
    
    if 'OverallOfficial' in full_trial:
        overall_official_list = full_trial['OverallOfficial']

        for official in overall_official_list:
            neo4j_query = '{'

            if 'OverallOfficialName' in official:
                name = official['OverallOfficialName']
            else:
                name = None
            if 'OverallOfficialAffiliation' in official:
                location = official['OverallOfficialAffiliation']
            else:
                location = None
            if 'OverallOfficialRole' in official:
                role = official['OverallOfficialRole']
            else:
                role = None

            if name:
                name = name.replace('\'','')
                masked_name = mask_name(name)
                if masked_name:
                    neo4j_query += f'OfficialName:\'{masked_name}\'~'
                else:
                    neo4j_query += f'OfficialName:\'{name}\'~'
            if location:
                location = location.replace('\'','')
                neo4j_query += f'OfficialAffiliation:\'{location}\'~'
            if role:
                role = role.replace('\'','')
                neo4j_query += f'OfficialRole:\'{role}\'~'

            end_term = neo4j_query.rfind('~')
            neo4j_query = neo4j_query[:end_term] + neo4j_query[end_term+1:]
            neo4j_query = neo4j_query.replace('~',',')

            neo4j_query += '}'

            investigator_id = db.run(f'MATCH (x:ClinicalTrial) WHERE x.NCTId = \'{nctid}\' MERGE (y:Investigator {neo4j_query}) MERGE (x)<-[:investigates]-(y) RETURN ID(y) as id').data()[0]['id']

            if name in locations:
                contact_query = ''
                populate_info = locations[name]

                if 'LocationContactPhone' in populate_info:
                    contact_query += f"SET x.ContactPhone = \"{populate_info['LocationContactPhone']}\""
                if 'LocationContactEMail' in populate_info:
                    contact_query += f"SET x.ContactEmail = \"{populate_info['LocationContactEMail']}\""

                db.run(f'MATCH (x:Investigator) WHERE ID(x) = {investigator_id} {contact_query}')

            if name in contacts:
                contact_query = ''
                populate_info = contacts[name]

                if 'CentralContactPhone' in populate_info:
                    contact_query += f"SET x.ContactPhone = \"{populate_info['CentralContactPhone']}\""
                if 'CentralContactEMail' in populate_info:
                    contact_query += f"SET x.ContactEmail = \"{populate_info['CentralContactEMail']}\""

                db.run(f'MATCH (x:Investigator) WHERE ID(x) = {investigator_id} {contact_query}')
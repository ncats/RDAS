from skr_web_api import Submission, METAMAP_INTERACTIVE_URL
import json
import os
import sys
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
print(workspace)
sys.path.append(workspace)
sys.path.append('/home/leadmandj/RDAS/')
from src import data_model as dm
import requests
import html
import re
from unidecode import unidecode
from datetime import date
from AlertCypher import AlertCypher
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
# import spacy
import nltk
from nltk.stem import PorterStemmer
nltk.download("punkt")
from spacy.matcher import Matcher
import spacy
<<<<<<<< HEAD:RDAS.CTKG/methods.py
========
import pandas as pd
>>>>>>>> devon_dev:RDAS_CTKG/methods.py
from fuzzywuzzy import fuzz
import ijson
import string
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
from time import sleep




def webscrape_ctgov_diseases():
    """
    Scrapes disease names and corresponding trial numbers from the ClinicalTrials.gov browse page.

    Returns:
        list: A list containing two lists:
            - The first list contains parsed disease names.
            - The second list contains corresponding trial numbers (as integers).

    Dependencies:
        - Selenium: Python library for automating web browser interaction.
        - Chromedriver: ChromeDriver executable must be available and its path provided in sysvars.

    Example:
        parsed_data = webscrape_ctgov_diseases()
        print(parsed_data)
        # Output: [['Disease1', 'Disease2', ...], [123, 456, ...]]
    """

    # Define the URL for the ClinicalTrials.gov browse page (Rare Diseases Section)
    url = 'https://clinicaltrials.gov/ct2/search/browse?brwse=ord_alpha_all'

    # Set up a headless Chrome browser using Selenium
    service = Service(f'{sysvars.ct_files_path}chromedriver')
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    driver = webdriver.Chrome(service=service,options=options)

    # Navigate to the specified URL
    driver.get(url)

    # Set the number of items to display on one page to all
    select = Select(driver.find_element('name','theDataTable_length'))
    select.select_by_value('-1')

    # Find elements in the table containing disease names and corresponding trial numbers
    table = driver.find_elements(By.XPATH, '//*[@id="theDataTable"]/tbody/tr/td/a')
    listed_trials = driver.find_elements(By.XPATH, '//*[@id="theDataTable"]/tbody/tr/td[2]')

    # Initialize lists to store parsed data
    parsed_table = list()
    parsed_trial_nums = list()

    # Parse disease names from the table
    for ele in table:
        parsed_table.append(ele.text)

    # Parse trial numbers from the table, removing commas and converting to integers
    for ele in listed_trials:
        parsed_trial_nums.append(int(ele.text.replace(',','')))

    # Return a list containing parsed disease names and corresponding trial numbers
    return [parsed_table,parsed_trial_nums]




def get_nctids(name_list):
    """
    Retrieves ClinicalTrials.gov Identifiers (NCTIDs) for a list of rare disease names.

    Args:
        name_list (list): List of rare disease names.

    Returns:
        list: List of ClinicalTrials.gov Identifiers (NCTIDs) associated with the provided rare disease names.

    Example:
        disease_names = ["Disease1", "Disease2", ...]
        nct_ids = get_nctids(disease_names)
        print(nct_ids)
        # Output: ["NCT123", "NCT456", ...]
    """

    # Initialize a list to store all retrieved NCTIDs
    all_trials = list()

    # Iterate through each rare disease name
    for name in name_list:
        # Replace double quotes to prevent issues with the URL
        name = name.replace('"','\"')

        # Construct the initial API query to get the total number of trials
        initial_query = 'https://clinicaltrials.gov/api/query/study_fields?expr=AREA[ConditionBrowseBranchAbbrev] Rare AND \"' + name + '\"&fields=NCTId&'
        query_end1 = 'min_rnk=1&max_rnk=1000&fmt=csv'
        
        try:
            # Make the API request to get the total number of trials
            response = requests.get(initial_query + query_end1).text.splitlines()
            total_trials = int(response[4][16:-1])
        except Exception as e:
            # Retry in case of an error
            print('ERROR in retrieving NCTIDS, retrying...')
            print(response)
            response = requests.get(initial_query + query_end1).text.splitlines()
            total_trials = int(response[4][16:-1])

        try:
            # Add trials to a temporary list
            trials = list()
            for trial in response[11:]:
                trials.append(trial.split(',')[1][1:-1])

            # Break into extra queries of 1000 trials if necessary
            for rank in range(1, total_trials//1000 + 1):
                # Get next 1000 trials
                query_end2 = 'min_rnk=' + str(rank*1000+1) + '&max_rnk=' + str((rank+1)*1000) + '&fmt=csv'
                response = requests.get(initial_query + query_end2).text.splitlines()

                # Add trials to the temporary list
                for trial in response[11:]:
                    trials.append(trial.split(',')[1][1:-1])

            # Add the trials from the temporary list to the overall list
            all_trials += trials

        except Exception as e:
            print(e)
            print(initial_query + query_end2)
            print(trial)

    # Return the list of all retrived NCTIDs
    return all_trials




def parse_module(module, trial):
    """
    Recursively parses a nested dictionary structure (module) and adds its key-value pairs to the trial dictionary.

    Args:
        module (dict): Nested dictionary structure to be parsed.
        trial (dict): Dictionary to which the key-value pairs will be added.

    Returns:
        dict: Modified trial dictionary containing key-value pairs from the provided module.

    Example:
        module_data = {"field1": "value1", "field2": {"nested_field": "nested_value"}}
        trial_data = parse_module(module_data, {})
        print(trial_data)
        # Output: {"field1": "value1", "field2": {"nested_field": "nested_value"}}
    """

    # Iterate through each key in the dictionary
    for key in module.keys():
        field = module[key]

        # Check if the current field is a nested dictionary
        if type(field) == dict:
            # Recursively parse the nested dictionary
            parse_module(field,trial)
        else:
            # Add the key-value pair to the trial dictionary
            trial[key] = module[key]

    # Return the modified trial dictionary
    return trial




def parse_trial_fields(trial):
    """
    Parses trial fields from a nested dictionary structure and returns a flattened dictionary.

    Args:
        trial (dict): Nested dictionary structure representing trial data.

    Returns:
        dict: Flattened dictionary containing parsed trial fields.

    Example:
        trial_data = {"FullStudiesResponse": {"FullStudies": [{"Study": {"field1": "value1", "field2": {"nested_field": "nested_value"}}}]}}
        parsed_data = parse_trial_fields(trial_data)
        print(parsed_data)
        # Output: {"field1": "value1", "field2": {"nested_field": "nested_value"}}
    """

    try:
        # Extract the base dictionary containing trial information
        base = trial['FullStudiesResponse']['FullStudies'][0]['Study']

        # Use the parse_module function to flatten the nested structure
        parsed_trial = parse_module(base, dict())
    except KeyError:
        # Return None if the expected keys are not found
        return None
    
    # Return the flattened dictionary containing parsed trial fields
    return parsed_trial




def extract_fields(nctid):
    """
    Extracts trial fields for a given ClinicalTrials.gov Identifier (NCTID) and returns a flattened dictionary.

    Args:
        nctid (str): ClinicalTrials.gov Identifier (NCTID) for the trial.

    Returns:
        dict: Flattened dictionary containing parsed trial fields.

    Example:
        nct_id = "NCT12345678"
        trial_fields = extract_fields(nct_id)
        print(trial_fields)
        # Output: {"field1": "value1", "field2": {"nested_field": "nested_value"}}
    """

    # Contruct the API query to retrieve full study information
    full_trial_query = 'https://clinicaltrials.gov/api/query/full_studies?expr=' + nctid + '&min_rnk=1&max_rnk=1&fmt=json'
    sleep(0.5)
    
    try:
        # Make the API request and parse the JSON response
        full_trial_response = requests.get(full_trial_query).json()

        # Use the parse_trial_fields function to flatten the nested structure
        full_trial = parse_trial_fields(full_trial_response)
    except ValueError:
        # Return None if there is an issue with the JSON response
        return None
    
    # Return the flattened dictionary containing parsed trial fields
    return full_trial


def get_lastupdated_postdate (ID):
    postdate_query = f'https://clinicaltrials.gov/api/query/field_values?expr={ID}&field=LastUpdatePostDate&fmt=json'
    try:
        # Make the API request and parse the JSON response
        full_response = requests.get(postdate_query).json()
        postdate = full_response['FieldValuesResponse']['FieldValues'][0]['FieldValue']

        return postdate

    except ValueError:
        # Return None if there is an issue with the JSON response
        return None



def cypher_generate(db,now,NCTID,data,node_type,update=None,return_single=None):
    """
    Generates a Cypher query for creating or updating nodes and relationships in a Neo4j database.

    Args:
        db: Neo4j database connection.
        now (str): Current date in string format.
        NCTID (str): ClinicalTrials.gov Identifier (NCTID) for the trial.
        data (dict): Dictionary containing node properties.
        node_type (str): Type of node in the Neo4j database.
        update (bool): Flag indicating whether to update an existing node.
        return_single (bool): Flag indicating whether to return data for a single clinical trial.

    Returns:
        str: Cypher query for creating or updating nodes and relationships.

    Example:
        db_connection = get_neo4j_connection()
        now_date = datetime.now().strftime("%m/%d/%y")
        trial_data = {"field1": "value1", "field2": "value2"}
        cypher_query = cypher_generate(db_connection, now_date, "NCT12345678", trial_data, "ClinicalTrial", update=True)
        print(cypher_query)
    """
        
    ID = None
    existing_node = list()
    pattern = '\'\w+\':'
    query = str()
    prev_create = str()
    
    if node_type == 'ClinicalTrial':
        if not update:
            data['DateCreatedRDAS'] = now
        else:
            try:
                # Get the previous creation date for an existing ClinicalTrial node, When a new ClinicalTrial node is created for the update it will keep the previous nodes DateCreatedRDAS
                prev_create = db.run('MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{NCTID}\" RETURN x.DateCreatedRDAS as created'.format(NCTID=NCTID)).data()
                prev_create = prev_create[0]['created']
                data['DateCreatedRDAS'] = prev_create
            except Exception as e:
                print(f'ERROR. Not Committing Changes')
                print(NCTID)
                print(prev_create)
                return

        data['LastUpdatedRDAS'] = now

    # Removes the quotation marks from around the strings for preparation to convert the data to a Cypher query
    data_string = str(data)    
    matches = re.finditer(pattern,data_string)
    for match in matches:
        start = match.start()
        end = match.end() - 2
        data_string = data_string[:start] + ' ' + data_string[start+1:]
        data_string = data_string[:end] + ' ' + data_string[end+1:]

    # Returns back just the string of data (Not the Cypher query) if the flag is set to True
    if return_single:
        return data_string

    try:
        # Check if a node with the given properties already exists
        existing_node = 'MATCH (x:{node_type} {data_string}) RETURN ID(x) AS ID LIMIT 1'.format(node_type=node_type,data_string=data_string)
        existing_node = db.run(existing_node).data()
    except Exception as e:
        print('ERROR. Not Committing Changes')
        return    
    if len(existing_node) > 0:
        ID = existing_node[0]['ID']

    # Generates the Cypher query used to create the node on the Neo4j Database
    if not node_type == 'ClinicalTrial':
        query += 'MATCH (ct: ClinicalTrial {{NCTId:\'{NCTID}\'}}) '.format(NCTID=NCTID)
    else:
        if update:
            # Update an existing ClinicalTrial node
            query += 'MATCH ({node_abbr}:{node_type} {{NCTId:\"{NCTID}\"}}) SET {node_abbr} = {data_string} '.format(node_abbr=dm.abbreviations[node_type],node_type=node_type,data_string=data_string,NCTID=NCTID)
        else:
            # Create a new ClinicalTrial node
            query += 'MERGE ({node_abbr}:{node_type} {data_string}) '.format(node_abbr=dm.abbreviations[node_type],node_type=node_type,data_string=data_string)

    if not node_type == 'ClinicalTrial':
        if ID:
            # Create a relationship between the ClinicalTrial node and the existing node
            query += 'MATCH ({node_abbr}:{node_type}) WHERE ID({node_abbr}) = {ID} MERGE (ct){dir1}[:{rel_name}]{dir2}({node_abbr})'.format(ID=ID,node_type=node_type,dir1=dm.rel_directions[node_type][0],dir2=dm.rel_directions[node_type][1],rel_name=dm.relationships[node_type],node_abbr=dm.abbreviations[node_type])
        else:
            # Create a relationship between the ClinicalTrial node and a new node
            query += 'MERGE ({node_abbr}:{node_type} {data_string}) MERGE (ct){dir1}[:{rel_name}]{dir2}({node_abbr}) '.format(data_string=data_string,ID=ID,node_type=node_type,dir1=dm.rel_directions[node_type][0],dir2=dm.rel_directions[node_type][1],rel_name=dm.relationships[node_type],node_abbr=dm.abbreviations[node_type])
    query += 'RETURN ID({node_abbr}) as id'.format(node_abbr=dm.abbreviations[node_type])
    return query




def format_node_data(db,now,trial,node_type,NCTID,update=None,return_single=None):
    data_collection = None
    node_data = dict()
    node_data_list = list()
    queries = list()
    query = str()
    fields = dm.fields[node_type]
    #NCTID = trial.get('NCTId')

    if node_type in dm.lists_of_nodes:
        list_of_nodes = dm.lists_of_nodes[node_type]
        if list_of_nodes in trial:
            data_collection = trial[list_of_nodes]
        
    if data_collection:
        if node_type == 'Condition':
            node_data_list = [{node_type:i} for i in data_collection]
        
        else:
            for node in data_collection:
                node_data = dict()
                for field in fields:
                    if field in node:
                        value = node[field]
                        node_data[field] = value
                node_data_list.append(node_data)
        
    elif trial:
        for field in fields:
            if field in trial:
                if field == 'Phase':
                    if trial[field]:
                        value = "; ".join(trial[field])
                    else:
                        value = "No Phase Specified"
                else:
                    value = trial[field]
                node_data[field] = value
        
        node_data_list.append(node_data)

    else:
        return None


    if return_single:
        # Returns only the Cypher query and does not run it
        return cypher_generate(db,now,NCTID,node_data_list[0],node_type,return_single=return_single)
    
    for ele in node_data_list:
        if not ele == {}:
            query = cypher_generate(db, now, NCTID, ele, node_type,update=update)
            if query and node_type == 'ClinicalTrial':
                db.run(query) #TEST response = db.run(query)

            elif query and not node_type == 'ClinicalTrial':
                # Steps to be ran to create additional nodes after a base node is created (ex. A Condition Leaf Node that connects to a Condition Node)
                if node_type in dm.process_nodes:
                    queries = unpack_nested_data(db, now, NCTID, trial, node_type)
                else:
                    queries = [query]

                if queries:
                    for q in queries:
                        db.run(q)
            else:
                print('ERROR: Query Returned Empty')
            
            

    return query



def unpack_nested_data (db, now, nctid, trial, node_type):
    """
    #POSTPONED FOR NOW
    if node_type == 'Condition':
        create_ancestor_nodes(db, trial, node_id, node_type)
        create_leaf_nodes(db, trial, node_id, node_type)

    if node_type == 'Intervention':
        #create_other_properties(db, trial, node_id, node_type)
        #ALSO POSTPONED
        #create_leaf_nodes(db, trial, node_id, node_type)
    """
    queries = None

    if node_type == 'ClinicalTrial':
        tokenizer = AutoTokenizer.from_pretrained("dslim/bert-base-NER")
        model = AutoModelForTokenClassification.from_pretrained("dslim/bert-base-NER", config={'output_hidden_states': False})
        nlp = pipeline("ner", model=model, tokenizer=tokenizer)

        queries = create_investigator_nodes(db, nlp, nctid, trial)

    elif node_type == 'Organization':
        queries = create_organization_node(db, now, nctid, trial)

    elif node_type == 'PrimaryOutcome':
        queries = create_primary_outcome_nodes(db, nctid, trial)

    return queries



def create_primary_outcome_nodes (db, nctid, trial):
    queries = list()

    pout_list = trial.get('PrimaryOutcome')
    if pout_list:
        for pout in pout_list:
            desc = pout.get('PrimaryOutcomeDescription')
            if desc:
                desc = desc.replace('\"','').replace('\'','')

            timeframe = pout.get('PrimaryOutcomeTimeFrame')
            if timeframe:
                timeframe = timeframe.replace('\"','').replace('\'','')

            measure = pout.get('PrimaryOutcomeMeasure')
            if measure:
                measure = measure.replace('\"','').replace('\'','')

            queries.append(f'MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\" MERGE (y:PrimaryOutcome {{PrimaryOutcomeDescription: \"{desc}\", PrimaryOutcomeTimeFrame: \"{timeframe}\", PrimaryOutcomeMeasure: \"{measure}\"}}) MERGE (x)-[:has_outcome]->(y)')

    return queries



def create_organization_node (db, now, nctid, trial):
    full_org_list = list()
    query_list = list()

    sname = trial.get('LeadSponsorName')
    sclass = trial.get('LeadSponsorClass')
    oname = trial.get('OrgFullName')
    oclass = trial.get('OrgClass')

    if sname and sclass:
        full_org_list.append({'OrgName':sname, 'OrgClass':sclass, 'OrgType': 'Sponsor'})
    elif sname:
        full_org_list.append({'OrgName':sname, 'OrgType': 'Sponsor'})
    elif sclass:
        full_org_list.append({'OrgClass':sclass, 'OrgType': 'Sponsor'})

    if oname and oclass:
        full_org_list.append({'OrgName':oname, 'OrgClass':oclass, 'OrgType': 'Organization'})
    elif oname:
        full_org_list.append({'OrgName':oname, 'OrgType': 'Organization'})
    elif oclass:
        full_org_list.append({'OrgClass':oclass, 'OrgType': 'Organization'})


    collaborator = trial.get('Collaborator')

    if collaborator:
        for collab in collaborator:
            full_org_list.append({'OrgName':collab['CollaboratorName'], 'OrgClass':collab['CollaboratorClass'], 'OrgType': 'Collaborator'})

    for org in full_org_list:
        query = cypher_generate(db, now, nctid, org, 'Organization')
        query_list.append(query)

    return query_list


def create_ancestor_nodes (db, trial, node_id, node_type):
    if node_type == 'Condition':
        pass


def create_leaf_nodes (db, trial, node_id, node_type):
    if node_type == 'Condition':
        pass
    elif node_type == 'Intervention':
        pass



"""
# MAY NOT NEED FUNCTION
def create_other_properties (db, nctid, trial, node_type):
    if node_type == 'Intervention':
        isDrug = trial.get('IsFDARegulatedDrug')
        isDevice = trial.get('IsFDARegulatedDevice')

        if not isDrug:
            isDrug = 'NULL'
        if not isDevice:
            isDevice = 'NULL'

        db.run(f'MATCH (x:ClinicalTrial)--(y:Intervention) WHERE x.NCTId = \"{nctid}\" SET y.IsFDARegulatedDrug = {isDrug} SET y.IsFDARegulatedDevice = {isDevice} RETURN TRUE')
"""



def create_investigator_nodes (db, nlp, nctid, full_trial):
    queries = list()
    contacts = dict()
    locations = dict()

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
                masked_name = mask_name(nlp, name)
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
            print(neo4j_query)

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



def mask_name(nlp, name):
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



def is_acronym(word):
    """
    Checks if a word is an acronym.

    Args:
        word (str): The word to be checked.

    Returns:
        bool: True if the word is an acronym, False otherwise.

    Example:
        result = is_acronym("NASA")
        print(result)  # Output: True
    """

    # Check if the word contains spaces
    if len(word.split(' ')) > 1:
        return False
    # Check if the word follows the pattern of an acronym
    elif bool(re.match(r'\w*[A-Z]\w*', word[:len(word)-1])) and (word[len(word)-1].isupper() or word[len(word)-1].isnumeric()):
        return True
    else:
        return False




def get_unmapped_conditions(db):
    """
    Retrieves conditions that are not mapped to GARD in the database.

    Args:
        db: The database connection.

    Returns:
        list: List of dictionaries containing condition names and their corresponding IDs.

    Example:
        conditions = get_unmapped_conditions(my_database)
        print(conditions)
        # Output: [{'Condition': 'Unmapped Condition 1', 'ID': 1}, {'Condition': 'Unmapped Condition 2', 'ID': 2}, ...]
    """

    # Query to find conditions that are not mapped to GARD
    conditions = db.run('MATCH (x:Condition) where not (x)-[:mapped_to_gard]-(:GARD) RETURN x.Condition, ID(x)').data()
    return conditions




def filter_mappings(mappings,cond_name,cond_id):
    """
    Filters and extracts relevant details from condition mappings.

    Args:
        mappings (list): List of dictionaries containing condition mappings.
        cond_name (str): Name of the condition.
        cond_id: ID of the condition.

    Returns:
        dict: Dictionary containing filtered details (CUI, SEM, PREF, FUZZ, META).

    Example:
        mappings = get_condition_mappings(my_database, 'Some Condition', 1)
        filtered_details = filter_mappings(mappings, 'Some Condition', 1)
        print(filtered_details)
        # Output: {'CUI': ['C12345', 'C67890'], 'SEM': [['T047', 'T191'], ['T123', 'T456']],
        # 'PREF': ['PreferredTerm1', 'PreferredTerm2'], 'FUZZ': [90, 75], 'META': [7, 8]}
    """

    cui_details = list()
    pref_details = list()
    fuzz_details = list()
    meta_details = list()
    sem_details = list()

    for idx,mapping in enumerate(mappings):
        # Extracting MetaMap score
        meta_score = int(mapping['MappingScore'].replace('-','')) // 10
        
        # Extracting details from the candidate
        candidates = mapping['MappingCandidates'][0]
        CUI = candidates['CandidateCUI']
        sem_types = candidates['SemTypes']
        candidate_pref = candidates['CandidatePreferred']

        # Calculating fuzziness score between condition name and preferred term
        fuzz_score_cond_pref = int(fuzz.token_sort_ratio(cond_name, candidate_pref))

        # Appending details to respective lists
        cui_details.append(CUI)
        sem_details.append(sem_types)
        pref_details.append(candidate_pref)
        fuzz_details.append(fuzz_score_cond_pref)
        meta_details.append(meta_score)

    # Creating a dictionary with filtered details
    if len(cui_details) > 0:
        return {'CUI':cui_details, 'SEM':sem_details, 'PREF':pref_details, 'FUZZ':fuzz_details, 'META':meta_details}




def normalize(phrase):
    """
    Normalizes a given phrase by removing diacritics, replacing single quotes,
    and removing non-alphanumeric characters.

    Args:
        phrase (str): The input phrase.

    Returns:
        str: The normalized phrase.

    Example:
        input_phrase = "Café au lait!"
        normalized_result = normalize(input_phrase)
        print(normalized_result)
        # Output: 'Cafe au lait'
    """
    phrase = unidecode(phrase) # Remove diacritics
    phrase = phrase.replace("\'","") # Replace single quotes
    phrase = re.sub('\W+', ' ', phrase) # Remove non-alphanumeric characters
    return phrase




def umls_to_gard(db,CUI):
    """
    Maps a UMLS CUI to GARD entries in the database.

    Args:
        db: The database connection.
        CUI (str): The UMLS CUI.

    Returns:
        dict: A dictionary containing GARD IDs and names corresponding to the given UMLS CUI.

    Example:
        db_connection = get_database_connection()
        umls_cui = "C0002736"
        result_mapping = umls_to_gard(db_connection, umls_cui)
        print(result_mapping)
        # Output: {'gard_id': ['12345', '67890'], 'gard_name': ['Example GARD 1', 'Example GARD 2']}
    """

    # Query the database to find GARD entries that have the given UMLS CUI
    res = db.run('MATCH (x:GARD) WHERE \"{CUI}\" IN x.UMLS RETURN x.GardId as gard_id, x.GardName as name'.format(CUI=CUI)).data()
    
    if res:
        data = list()
        names = list()
        # Extract GARD IDs and names from the query result
        for i in res:
            gard_id = i['gard_id']
            gard_name = i['name']
            data.extend([gard_id])
            names.extend([gard_name])
        return {'gard_id':data, 'gard_name':names}

def convert_semantic_types(type_list):
    names = pd.read_csv(f'{sysvars.ct_files_path}SemanticTypes_2018AB.txt', delimiter='|', usecols=[0,2], names=['ABBR', 'FULLSEM'])
    names = dict(zip(names['ABBR'], names['FULLSEM']))
    
    temp = list()
    for entry in type_list:
        temp.append(names[entry])
    return temp

def add_metamap_annotation(db, trial_info):
    for k,v in trial_info.items():
        concept = v['term']
        score = v['score']
        types = v['types']
        nctid = v['nctid']
        db.run(f'MATCH (y:ClinicalTrial) WHERE y.NCTId = \'{nctid}\' MERGE (x:Trial_Annotation {{umls_cui:\'{k}\', umls_concept:\'{concept}\', umls_types:{types}}}) MERGE (y)-[:has_metamap_annotation {{umls_score:{score}}}]->(x)')

def metamap_trial_annotation(db, trial_info, update_metamap=True):
    INSTANCE = Submission(os.environ['METAMAP_EMAIL'],os.environ['METAMAP_KEY'])
    INSTANCE.init_generic_batch('metamap','-J acab,amas,aapp,anab,antb,bact,bacs,bodm,comd,chem,clnd,cgab,diap,dsyn,elii,enzy,emod,fngs,gngm,hops,horm,imft,irda,inpo,inch,inpr,mobd,mosq,neop,nnon,nusq,orch,podg,phsu,rcpt,sosy,topp,virs,vita --JSONn') #--sldiID
    INSTANCE.form['SingLinePMID'] = True

    trial_strs = [f"{k}|{normalize(v)}\n" for k,v in trial_info.items()]
    with open(f'{sysvars.ct_files_path}metamap_trials.txt','w') as f:
        f.writelines(trial_strs)

    # Update MetaMap results if required
    if update_metamap:
        if os.path.exists(f'{sysvars.ct_files_path}metamap_trials_out.json'):
            os.remove(f'{sysvars.ct_files_path}metamap_trials_out.json')
            print('INITIATING UPDATE... METAMAP_TRIALS_OUT.JSON REMOVED')

    # Run MetaMap and store results
    if not os.path.exists(f'{sysvars.ct_files_path}metamap_trials_out.json'):
        INSTANCE.set_batch_file(f'{sysvars.ct_files_path}metamap_trials.txt') #metamap_cond.txt
        print('METAMAP JOB SUBMITTED')
        response = INSTANCE.submit()

        try:
            data = response.content.decode().replace("\n"," ")
            data = re.search(r"({.+})", data).group(0)
    
        except Exception as e:
            print(e)
            data = None
    
        try:
            data = json.loads(data)
            with open(f'{sysvars.ct_files_path}metamap_trials_out.json','w') as f:
                json.dump(data,f)
                data = data['AllDocuments']
    
        except Exception as e:
            print(e)
    
    else:
        print('USING PREVIOUSLY CREATED METAMAP_TRIALS_OUT.JSON')
        with open(f'{sysvars.ct_files_path}metamap_trials_out.json','r') as f:
            data = ijson.items(f,'AllDocuments.item')

            # Process MetaMap results and update database
            for idx, entry in enumerate(data):
                print(f'{str(idx)}')
                utterances = entry['Document']['Utterances'][0]
                utt_text = utterances['UttText']
                print(utt_text)
                phrases = utterances['Phrases']
                
                nctid = utterances['PMID']

                meta_single_trial = dict()
                cleaned_meta_single_trial = dict()
                for phrase in phrases:
                    if len(phrase['Mappings']) > 0:
                        for phr in phrase['Mappings']:
                            meta_term = phr['MappingCandidates'][0]['CandidatePreferred']
                            meta_cui = phr['MappingCandidates'][0]['CandidateCUI']
                            meta_score = int(phr['MappingScore'][1:])
                            meta_types = convert_semantic_types(phr['MappingCandidates'][0]['SemTypes'])
                            meta_single_trial[meta_cui] = {'term':meta_term.replace('\'',''), 'score':meta_score, 'types':meta_types, 'nctid':nctid}

                        for k,v in meta_single_trial.items():
                            if not k in cleaned_meta_single_trial:
                                cleaned_meta_single_trial[k] = v

                add_metamap_annotation(db, cleaned_meta_single_trial)
                print('------------------------')



def condition_map(db, update_metamap=True):
    """
    Maps conditions to UMLS concepts using MetaMap annotations.

    Args:
        db: The database connection.
        update_metamap (bool): Flag to update MetaMap results. Defaults to True.

    Returns:
        None

    Example:
        db_connection = get_database_connection()  # Replace with actual function to get the database connection
        condition_map(db_connection)
    """

    print('RUNNING SETUP')
    gard_db = AlertCypher('gard')
    
    # # Initialize MetaMap instance
    INSTANCE = Submission(os.environ['METAMAP_EMAIL'],os.environ['METAMAP_KEY'])
    INSTANCE.init_generic_batch('metamap','-J acab,anab,comd,cgab,dsyn,fndg,emod,inpo,mobd,neop,patf,sosy --JSONn') #--sldiID
    INSTANCE.form['SingLinePMID'] = True

    print('RUNNING GARD POPULATION')
    # Fetch GARD entries from the database
    gard_res = gard_db.run('MATCH (x:GARD) RETURN x.GardId as GardId, x.UMLS as gUMLS, x.GardName as GardName, x.Synonyms as Synonyms, x.UMLS_Source as usource')
    for gres in gard_res.data():
        gUMLS = gres['gUMLS']
        name = gres['GardName']
        gard_id = gres['GardId']
        syns = gres['Synonyms']
        usource = gres['usource']

        # Check if UMLS data is present and create GARD node accordingly
        if gUMLS:
            db.run('MERGE (x:GARD {{GardId:\"{gard_id}\",GardName:\"{name}\",Synonyms:{syns},UMLS:{gUMLS},UMLS_Source:\"{usource}\"}})'.format(name=name,gard_id=gard_id,syns=syns,gUMLS=gUMLS,usource=usource))
        else:
            db.run('MERGE (x:GARD {{GardId:\"{gard_id}\",GardName:\"{name}\",Synonyms:{syns},UMLS_Source:\"{usource}\"}})'.format(name=name,gard_id=gard_id,syns=syns,usource=usource))

    print('RUNNING METAMAP')
    # Fetch conditions from the database that havent already been annotated and are not acronyms
<<<<<<<< HEAD:RDAS.CTKG/methods.py
    res = db.run('MATCH (c:Condition) WHERE NOT EXISTS((c)--(:Annotation)) RETURN c.Condition as condition, ID(c) as cond_id')
========
    res = db.run('MATCH (c:Condition) WHERE NOT EXISTS((c)--(:Condition_Annotation)) RETURN c.Condition as condition, ID(c) as cond_id')
>>>>>>>> devon_dev:RDAS_CTKG/methods.py
    cond_strs = [f"{i['cond_id']}|{normalize(i['condition'])}\n" for i in res if not is_acronym(i['condition'])]
    
    # Write condition strings to a file for MetaMap processing
    with open(f'{sysvars.ct_files_path}metamap_cond.txt','w') as f:
        f.writelines(cond_strs)
    
    # Update MetaMap results if required
    if update_metamap:
        if os.path.exists(f'{sysvars.ct_files_path}metamap_cond_out.json'):
            os.remove(f'{sysvars.ct_files_path}metamap_cond_out.json')
            print('INITIATING UPDATE... METAMAP_COND_OUT.JSON REMOVED')

    # Run MetaMap and store results
    if not os.path.exists(f'{sysvars.ct_files_path}metamap_cond_out.json'):
        INSTANCE.set_batch_file(f'{sysvars.ct_files_path}metamap_cond.txt') #metamap_cond.txt
        print('METAMAP JOB SUBMITTED')
        response = INSTANCE.submit()

        try:
            data = response.content.decode().replace("\n"," ")
            data = re.search(r"({.+})", data).group(0)
    
        except Exception as e:
            print(e)
            data = None
    
        try:
            data = json.loads(data)
            with open(f'{sysvars.ct_files_path}metamap_cond_out.json','w') as f:
                json.dump(data,f)
                data = data['AllDocuments']
    
        except Exception as e:
            print(e)
    
    else:
        print('USING PREVIOUSLY CREATED METAMAP_COND_OUT.JSON')
        with open(f'{sysvars.ct_files_path}metamap_cond_out.json','r') as f:
            data = json.load(f)['AllDocuments']

    print('PARSING OUT METAMAP FILTERS')
    ALL_SEM = dict()

    # Process MetaMap results and update database
    for entry in data:
        utterances = entry['Document']['Utterances'][0]
        utt_text = utterances['UttText']
        phrases = utterances['Phrases'][0]
        mappings = phrases['Mappings']
        cond_id = utterances['PMID']
        retrieved_mappings = filter_mappings(mappings,utt_text,cond_id)
       
        if retrieved_mappings:
            CUI = retrieved_mappings['CUI']
            PREF = retrieved_mappings['PREF']
            META = retrieved_mappings['META']
            FUZZ = retrieved_mappings['FUZZ']
            ALL_SEM[cond_id] = retrieved_mappings['SEM']

            # Update MetaMap-related properties in the Condition node
            query = 'MATCH (x:Condition) WHERE ID(x) = {cond_id} SET x.METAMAP_OUTPUT = {CUI} SET x.METAMAP_PREFERRED_TERM = {PREF} SET x.METAMAP_SCORE = {META} SET x.FUZZY_SCORE = {FUZZ}'.format(CUI=CUI,PREF=PREF,META=META,FUZZ=FUZZ,cond_id=cond_id)
            db.run(query)
            
    print('CREATING AND CONNECTING METAMAP ANNOTATIONS')
<<<<<<<< HEAD:RDAS.CTKG/methods.py
    # Delete existing annotations DONT NEED, REMOVE STEP
    #db.run('MATCH (x:Annotation) DETACH DELETE x')
========
>>>>>>>> devon_dev:RDAS_CTKG/methods.py
    # Fetch relevant data from Condition nodes
    res = db.run('MATCH (x:Condition) WHERE x.METAMAP_OUTPUT IS NOT NULL RETURN ID(x) AS cond_id, x.METAMAP_OUTPUT AS cumls, x.METAMAP_PREFERRED_TERM AS prefs, x.FUZZY_SCORE as fuzz, x.METAMAP_SCORE as meta').data()

    exclude_umls = sysvars.umls_blacklist

    # Process and create annotations based on MetaMap results
    for entry in res:
        cond_id = entry['cond_id']
        CUMLS = entry['cumls']
        prefs = entry['prefs']
        sems = ALL_SEM[str(cond_id)]
        fuzzy_scores = entry['fuzz']
        meta_scores = entry['meta']

        for idx,umls in enumerate(CUMLS):
            # Skip blacklisted or None UMLS entries
            if umls in exclude_umls or umls == None:
                continue            

            gard_ids = umls_to_gard(gard_db,umls)
            if gard_ids:
                gard_ids = gard_ids['gard_id']
                for gard_id in gard_ids:
                    # Create Annotation nodes and connect to Condition and GARD nodes
<<<<<<<< HEAD:RDAS.CTKG/methods.py
                    db.run('MATCH (z:GARD) WHERE z.GardId = \"{gard_id}\" MATCH (y:Condition) WHERE ID(y) = {cond_id} MERGE (x:Annotation {{UMLS_CUI: \"{umls}\", UMLSPreferredName: \"{pref}\", SEMANTIC_TYPE: {sems}, MATCH_TYPE: \"METAMAP\"}}) MERGE (x)<-[:has_annotation {{FUZZY_SCORE: {fuzz}, METAMAP_SCORE: {meta}}}]-(y) MERGE (z)<-[:mapped_to_gard]-(x)'.format(gard_id=gard_id,cond_id=cond_id,umls=umls,pref=prefs[idx],sems=sems[idx],fuzz=fuzzy_scores[idx],meta=meta_scores[idx]))
            else:
                # Create Annotation nodes and connect to Condition nodes
                db.run('MATCH (y:Condition) WHERE ID(y) = {cond_id} MERGE (x:Annotation {{UMLS_CUI: \"{umls}\", UMLSPreferredName: \"{pref}\", SEMANTIC_TYPE: {sems}, MATCH_TYPE: \"METAMAP\"}}) MERGE (x)<-[:has_annotation {{FUZZY_SCORE: {fuzz}, METAMAP_SCORE: {meta}}}]-(y)'.format(cond_id=cond_id,umls=umls,pref=prefs[idx],sems=sems[idx],fuzz=fuzzy_scores[idx],meta=meta_scores[idx]))
========
                    db.run('MATCH (z:GARD) WHERE z.GardId = \"{gard_id}\" MATCH (y:Condition) WHERE ID(y) = {cond_id} MERGE (x:Condition_Annotation {{UMLS_CUI: \"{umls}\", UMLSPreferredName: \"{pref}\", SEMANTIC_TYPE: {sems}, MATCH_TYPE: \"METAMAP\"}}) MERGE (x)<-[:has_annotation {{FUZZY_SCORE: {fuzz}, METAMAP_SCORE: {meta}}}]-(y) MERGE (z)<-[:mapped_to_gard]-(x)'.format(gard_id=gard_id,cond_id=cond_id,umls=umls,pref=prefs[idx],sems=sems[idx],fuzz=fuzzy_scores[idx],meta=meta_scores[idx]))
            else:
                # Create Annotation nodes and connect to Condition nodes
                db.run('MATCH (y:Condition) WHERE ID(y) = {cond_id} MERGE (x:Condition_Annotation {{UMLS_CUI: \"{umls}\", UMLSPreferredName: \"{pref}\", SEMANTIC_TYPE: {sems}, MATCH_TYPE: \"METAMAP\"}}) MERGE (x)<-[:has_annotation {{FUZZY_SCORE: {fuzz}, METAMAP_SCORE: {meta}}}]-(y)'.format(cond_id=cond_id,umls=umls,pref=prefs[idx],sems=sems[idx],fuzz=fuzzy_scores[idx],meta=meta_scores[idx]))
>>>>>>>> devon_dev:RDAS_CTKG/methods.py

    print('REMOVING UNNEEDED PROPERTIES')
    # Remove unnecessary properties from Condition nodes that were used during processing
    db.run('MATCH (x:Condition) SET x.METAMAP_PREFERRED_TERM = NULL SET x.METAMAP_OUTPUT = NULL SET x.FUZZY_SCORE = NULL SET x.METAMAP_SCORE = NULL')
    
    print('ADDING GARD-CONDITION MAPPINGS BASED ON EXACT STRING MATCH')
    # Fetch Condition nodes without existing annotations
    res = db.run('MATCH (x:Condition) WHERE NOT (x)-[:has_annotation]-() RETURN ID(x) as cond_id, x.Condition as cond').data()
    
    # Create annotations based on exact string match and connect to GARD nodes
    for entry in res:
        cond_id = entry['cond_id']
        cond = entry['cond']
<<<<<<<< HEAD:RDAS.CTKG/methods.py
        db.run('MATCH (x:GARD) WHERE toLower(x.GardName) = toLower(\"{cond}\") MATCH (y:Condition) WHERE ID(y) = {cond_id} MERGE (z:Annotation {{UMLSPreferredName: \"{cond}\", MATCH_TYPE: \"STRING\"}}) MERGE (z)<-[:has_annotation]-(y) MERGE (x)<-[:mapped_to_gard]-(z)'.format(cond=cond,cond_id=cond_id))
========
        db.run('MATCH (x:GARD) WHERE toLower(x.GardName) = toLower(\"{cond}\") MATCH (y:Condition) WHERE ID(y) = {cond_id} MERGE (z:Condition_Annotation {{UMLSPreferredName: \"{cond}\", MATCH_TYPE: \"STRING\"}}) MERGE (z)<-[:has_annotation]-(y) MERGE (x)<-[:mapped_to_gard]-(z)'.format(cond=cond,cond_id=cond_id))
>>>>>>>> devon_dev:RDAS_CTKG/methods.py




def drug_normalize(drug):
    """
    Normalize a drug name by removing non-ASCII characters and replacing non-word characters with spaces.

    Parameters:
    - drug (str): The input drug name to be normalized.

    Returns:
    - str: The normalized drug name.
    """

    # Remove non-ASCII characters
    new_val = drug.encode("ascii", "ignore")

    # Decode the bytes to string
    updated_str = new_val.decode()

    # Replace non-word characters with spaces
    updated_str = re.sub('\W+',' ', updated_str)

    return updated_str




def create_drug_connection(db,rxdata,drug_id,wspacy=False):
    """
    Create a connection between an Intervention node and a Drug node based on RxNormID.

    Parameters:
    - db: Neo4j database connection.
    - rxdata (dict): Dictionary containing RxNorm data for the drug.
    - drug_id (int): ID of the Intervention (ClinicalTrial node) to connect with the Drug node.
    - wspacy (bool): Flag indicating whether the connection involves SpaCy processing.

    Returns:
    - None
    """

    rxnormid = rxdata['RxNormID']

    # Create or merge Drug node with RxNormID
    db.run('MATCH (x:Intervention) WHERE ID(x)={drug_id} MERGE (y:Drug {{RxNormID:{rxnormid}}}) MERGE (y)<-[:mapped_to_rxnorm {{WITH_SPACY: {wspacy}}}]-(x)'.format(rxnormid=rxnormid, drug_id=drug_id, wspacy=wspacy))
    print(f'MAPPED {rxnormid}')

    # Set additional properties on the Drug node
    for k,v in rxdata.items():
        key = k.replace(' ','')
        db.run('MERGE (y:Drug {{RxNormID:{rxnormid}}}) WITH y MATCH (x:Intervention) WHERE ID(x)={drug_id} MERGE (y)<-[:mapped_to_rxnorm]-(x) SET y.{key} = {value}'.format(rxnormid=rxdata['RxNormID'], drug_id=drug_id, key=key, value=v))




def get_rxnorm_data(drug):
    """
    Retrieve RxNorm data for a given drug name from the RxNav API.

    Parameters:
    - drug (str): Name of the drug for which RxNorm data is to be retrieved.

    Returns:
    - dict or None: Dictionary containing RxNorm data for the drug, or None if data retrieval fails.
    """

    # Form RxNav API request to get RxNormID based on drug name
    rq = 'https://rxnav.nlm.nih.gov/REST/rxcui.json?name={drug}&search=2'.format(drug=drug)
    response = requests.get(rq)
    try:
        rxdata = dict()

        # Extract RxNormID from the response
        response = response.json()['idGroup']['rxnormId'][0]
        rxdata['RxNormID'] = response

        # Form RxNav API request to get all properties of the drug using RxNormID
        rq2 = 'https://rxnav.nlm.nih.gov/REST/rxcui/{rxnormid}/allProperties.json?prop=codes+attributes+names+sources'.format(rxnormid=response)
        response = requests.get(rq2)
        response = response.json()['propConceptGroup']['propConcept']

        # Extract and organize properties of the drug
        for r in response:
            if r['propName'] in rxdata:
                rxdata[r['propName']].append(r['propValue'])
            else:
                rxdata[r['propName']] = [r['propValue']]

        return rxdata

    except KeyError as e:
        return
    except ValueError as e:
        print('ERROR')
        print(drug)
        return




def nlp_to_drug(db,doc,matches,drug_name,drug_id):
    """
    Map drug names detected in NLP to RxNorm data and create connections in the database.

    Parameters:
    - db: Neo4j database connection.
    - doc: SpaCy NLP document containing the text.
    - matches: List of matches detected in the document.
    - drug_name (str): Name of the drug for which connections are to be created.
    - drug_id (int): ID of the drug node in the database.

    Returns:
    - None
    """

    for match_id, start, end in matches:
        span = doc[start:end].text

        # Retrieve RxNorm data for the drug name
        rxdata = get_rxnorm_data(span.replace(' ','+'))

        if rxdata:
            # Create connections in the database using RxNorm data
            create_drug_connection(db,rxdata,drug_id,wspacy=True)
        else:
            print('Map to RxNorm failed for intervention name: {drug_name}'.format(drug_name=drug_name))




def rxnorm_map(db, rxnorm_progress):
    """
    Map RxNorm data to Drug Interventions in the Neo4j database.

    Parameters:
    - db: Neo4j database connection.

    Returns:
    - None
    """

    print('Starting RxNorm data mapping to Drug Interventions')

    # Load SpaCy NLP model and set up matcher
    nlp = spacy.load('en_ner_bc5cdr_md')
    pattern = [{'ENT_TYPE':'CHEMICAL'}]
    matcher = Matcher(nlp.vocab)
    matcher.add('DRUG',[pattern])

    # Retrieve drug interventions from the database that do NOT already have a Drug node attached
    results = db.run('MATCH (x:Intervention) WHERE x.InterventionType = "Drug" AND NOT EXISTS((x)--(:Drug)) RETURN x.InterventionName, ID(x)').data()
    length = len(results)

    # Iterate over drug interventions and map RxNorm data
    for idx,res in enumerate(results):
        if idx < rxnorm_progress:
            continue
        
        print(f'{str(idx)}/{length}')
        db.setConf('UPDATE_PROGRESS', 'clinical_rxnorm_progress', str(idx))

        drug_id = res['ID(x)']
        drug = res['x.InterventionName']

        # Normalize drug name and prepare for RxNorm mapping
        drug = drug_normalize(drug)
        drug_url = drug.replace(' ','+')

        # Retrieve RxNorm data for the drug name
        rxdata = get_rxnorm_data(drug_url)

        if rxdata:
            # Create connections in the database using RxNorm data
            create_drug_connection(db, rxdata, drug_id)
            
        else:
            # If RxNorm data not found, use SpaCy NLP to detect drug names and map to RxNorm
            doc = nlp(drug)
            matches = matcher(doc)
            nlp_to_drug(db,doc,matches,drug,drug_id)

from src import data_model as dm
import requests
import html
import re
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

def webscrape_ctgov_diseases():
    url = 'https://clinicaltrials.gov/ct2/search/browse?brwse=ord_alpha_all'

    service = Service('/home/leadmandj/alert_remake/clinical/src/chromedriver')
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    driver = webdriver.Chrome(service=service,options=options)
    driver.get(url)

    select = Select(driver.find_element('name','theDataTable_length'))
    select.select_by_value('-1')
    table = driver.find_elements(By.XPATH, '//*[@id="theDataTable"]/tbody/tr/td/a')
    listed_trials = driver.find_elements(By.XPATH, '//*[@id="theDataTable"]/tbody/tr/td[2]')
    parsed_table = list()
    parsed_trial_nums = list()
    for ele in table:
        parsed_table.append(ele.text)
    for ele in listed_trials:
        parsed_trial_nums.append(int(ele.text.replace(',','')))

    return [parsed_table,parsed_trial_nums]

def get_nctids(name_list):
    # check total number of trials
    all_trials = list()
    for name in name_list:
        name = name.replace('"','\"')

        initial_query = 'https://clinicaltrials.gov/api/query/study_fields?expr=AREA[ConditionBrowseBranchAbbrev] Rare AND \"' + name + '\"&fields=NCTId&'
        query_end1 = 'min_rnk=1&max_rnk=1000&fmt=csv'
        response = requests.get(initial_query + query_end1).text.splitlines()
        try:
            total_trials = int(response[4][16:-1])
        except (IndexError,ValueError) as e:
            print(response)
            continue
        # add trials to list
        trials = list()
        for trial in response[11:]:
            trials.append(trial.split(',')[1][1:-1])

        # break into extra queries of 1000 trials if necessary
        for rank in range(1, total_trials//1000 + 1):
            # get next 1000 trials
            query_end2 = 'min_rnk=' + str(rank*1000+1) + '&max_rnk=' + str((rank+1)*1000) + '&fmt=csv'
            response = requests.get(initial_query + query_end2).text.splitlines()

            # add trials to list
            for trial in response[11:]:
                trials.append(trial.split(',')[1][1:-1])

        all_trials += trials

    # return list of trials
    return all_trials

def parse_module(module, trial):
    for key in module.keys():
        field = module[key]
        if type(field) == dict:
            parse_module(field,trial)
        else:
            trial[key] = module[key]

    return trial

def parse_trial_fields(trial):
    base = trial['FullStudiesResponse']['FullStudies'][0]['Study']
    parsed_trial = parse_module(base, dict())
    
    return parsed_trial

def extract_fields(nctid):
    full_trial_response = None
    full_trial_query = 'https://clinicaltrials.gov/api/query/full_studies?expr=' + nctid + '&min_rnk=1&max_rnk=1&fmt=json'
    
    try:
        full_trial_response = requests.get(full_trial_query).json()
        full_trial = parse_trial_fields(full_trial_response)
    except ValueError as e:
        print(e)
        print(full_trial_response)
        print(full_trial_response)
        full_trial = None
    
    return full_trial

def cypher_generate(NCTID,data,node_type):
    pattern = '\'\w+\':'
    query = str()
    data_string = str(data)
    
    matches = re.finditer(pattern,data_string)
    for match in matches:
        start = match.start()
        end = match.end() - 2
        data_string = data_string[:start] + ' ' + data_string[start+1:]
        data_string = data_string[:end] + ' ' + data_string[end+1:]
        
    if not node_type == 'ClinicalTrial':
        query += 'MATCH (ct: ClinicalTrial {{NCTId:\'{NCTID}\'}}) '.format(NCTID=NCTID)

    query += 'MERGE ({node_abbr}: {node_type} {data_string}) '.format(node_abbr=dm.abbreviations[node_type],node_type=node_type,data_string=data_string)

    if not node_type == 'ClinicalTrial':
        query += 'MERGE (ct){dir1}[:{rel_name}]{dir2}({node_abbr}) '.format(dir1=dm.rel_directions[node_type][0],dir2=dm.rel_directions[node_type][1],rel_name=dm.relationships[node_type],node_abbr=dm.abbreviations[node_type])

    query += 'RETURN TRUE'

    #print(query)

    return query

def format_node_data(trial,node_type):
    data_collection = None
    node_data = dict()
    node_data_list = list()
    queries = list()
    query = str()
    fields = dm.fields[node_type]
    NCTID = trial['NCTId']

    if node_type in dm.lists_of_nodes:
        list_of_nodes = dm.lists_of_nodes[node_type]
        if list_of_nodes in trial:
            data_collection = trial[list_of_nodes]
        
    if data_collection:
        if node_type == 'Condition':
            for cond in data_collection:
                node_data[node_type] = cond
                node_data_list.append(node_data)
        
        else:
            for node in data_collection:
                node_data = dict()
                for field in fields:
                    if field in node:
                        value = node[field]
                        node_data[field] = value
                node_data_list.append(node_data)
        
    else:
        for field in fields:
            if field in trial:
                value = trial[field]
                node_data[field] = value
        node_data_list.append(node_data)

    for ele in node_data_list:
        if not ele == {}: 
            query = cypher_generate(NCTID, ele, node_type)
            queries.append(query)
    return queries
'''
def data_string(full_data, node_fields, update=False, CT=False):
    if update:
        setType = '= '
        varType = 'trial.'
    else:
        setType = ': '
        varType = ''

    node_data = list()
    nodes_data = list()
    node_group = list()
    maximum = 0
    for field_name in node_fields:
        if field_name in full_data:
            size = len(full_data[field_name])
            if size > maximum:
                maximum = size

    for i in range(maximum):
        node_data.clear()
        for field_name in node_fields:
            if field_name in full_data:
                try:
                    if field_name in dm.ArrayValues:
                        val = full_data[field_name]

                        field_value = [elem for elem in val]
                        node_data.append(varType + field_name + setType + str(field_value))
                        continue
                    else:
                        val = full_data[field_name][i]
                except IndexError as e:
                    continue

                field_value = val.replace('\\','\\\\').replace('\'','\\\'').replace('\"','\\"')
                field_value = '\"' + field_value + '\"'
                node_data.append(varType + field_name + setType + field_value)
            nodes_data = ', '.join(node_data)
        node_group.append(nodes_data)
        if CT:
            temp = node_group[0]
            now = date.today()
            now = now.strftime("%m/%d/%y")

            if update == False:
                temp = temp + ', ' + varType + 'DateCreatedRDAS' + setType + '\"{now}\"'.format(now=now)

            temp = temp + ', ' + varType + 'LastUpdatedRDAS' + setType + '\"{now}\"'.format(now=now)
            node_group[0] = temp
            return (list(set(node_group)))

    # return list of strings for each individual node
    return (list(set(node_group)))

def create_trial_node (db, string_data):
    cypher_add_trial = 'MERGE (trial:ClinicalTrial{' + string_data + '})'
    db.run(cypher_add_trial)

def create_additional_nodes (db, string_data, trial):
    print(trial)
    cypher = 'MATCH (trial:ClinicalTrial {{NCTId:\"{trial}\"}}) '.format(trial=trial)
    for j in range(len(string_data)):
        field = string_data[j]
        if len(field) > 0:
            for i in range(len(field)):
                cypher2 = 'MERGE (data_node:' + dm.additional_class_names[j] + '{' + field[i]+ '}) MERGE (trial)' + dm.data_direction[j][0] + '[:' + dm.additional_class_connections[j] + ']' + dm.data_direction[j][1] + '(data_node)'
                db.run(cypher + cypher2)
'''

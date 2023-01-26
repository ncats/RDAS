from dataclasses import field
import requests
import html
import data_model
from datetime import date

def extract_all_fields(nctid):
    '''
    generates dictionary: field names -> field values for a clinical trial defined by its nctid
    arguments:
        nctid (string): clinical trial to extract data from
    returns:
        full_trial (dictionary): all field data
    '''
    # query clinical trials API to get 
    full_trial_query = 'https://clinicaltrials.gov/api/query/full_studies?expr='
    full_trial_query += nctid + '&min_rnk=1&max_rnk=1&fmt=xml'
    full_trial_response = requests.get(full_trial_query)

    # extract data: field name and field value
    full_trial_response = full_trial_response.text.split('</Field>')
    full_trial = dict()
    for block in full_trial_response:
        if '<Field Name="' in block:
            block = block.split('<Field Name="')[1]
            start = block.find('">')
            field_name = block[:start].strip()
            field_value = html.unescape(block[(start+2):])

            try:
                if type(full_trial[field_name]) == list:
                    
                    if field_value:
                        full_trial[field_name] = full_trial[field_name] + [field_value]
                        
                    else:
                        full_trial[field_name] = full_trial[field_name] + ['None']

            except KeyError:
                full_trial[field_name] = [field_value]

    return full_trial

def list_type(text_raw):
    '''
    generates string containing all data to create a neo4j node
    arguments:
        text (string): raw data from clinical trials api
    returns:
        (string): string representing array for neo4j
    '''
    text_raw = list(text_raw)
    if type(text_raw) == list:
        if len(text_raw) > 1:
            #print(text_raw)
            pass

        clean_list = list()
        for text in text_raw:
            text = text.replace('||','\n')  # '||' represents sub-element
            text_list = text.split('|')
            [clean_list.append(ele) for ele in text_list]
    else:
        text_raw = text_raw.replace('||','\n')  # '||' represents sub-element
        text_list = text_raw.split('|')
        clean_list = [elm.replace('\"','\\"') for elm in text_list]
    return str(clean_list)          # '|' represents new element

def data_string(full_data, node_fields, special_types = data_model.special_types, CT = False):
    '''
    generates string containing all data to create a neo4j node
    arguments:
        full_data (dictionary): all fields for a clinical trial
        node_fields (string list): fields for this node type
        special_types (string set): set of fields with | || data format
    returns:
        (string): data_string
    '''
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
                    if field_name in data_model.ArrayValues:
                        val = full_data[field_name]
                        
                        field_value = [elem for elem in val]
                        node_data.append(field_name + ': ' + str(field_value))
                        continue
                    else:
                        val = full_data[field_name][i]
                except IndexError:
                    continue
                field_value = val.replace('\\','\\\\').replace('\'','\\\'').replace('\"','\\"')
                field_value = '\"' + field_value + '\"'
                node_data.append(field_name + ': ' + field_value)
            nodes_data = ', '.join(node_data)
        node_group.append(nodes_data)
        if CT:
            temp = node_group[0]
            now = date.today()
            now = now.strftime("%m/%d/%y")
            temp = temp + ', DateCreated: \"{now}\"'.format(now=now)
            node_group[0] = temp
            return (list(set(node_group)))

    # return list of strings for each individual node
    return (list(set(node_group)))

def nctid_list(condition_name):
    '''
    generates list of all clinical trials (nctid) for a given condition
    arguments:
        condition_name (string): condition to search for in clinical trials api
    returns:
        (string list): clinical trials (nctid list)
    '''
    # check total number of trials
    initial_query = 'https://clinicaltrials.gov/api/query/study_fields?expr=%22'
    initial_query += condition_name.replace(' ', '+') + '%22&fields=NCTId&min_rnk=1&max_rnk=1000&fmt=csv'
    initial_response = requests.get(initial_query).text.splitlines()
    total_trials = int(initial_response[4][16:-1])
    
    # add trials to list
    trials = list()
    for trial in initial_response[11:]:
        trials.append(trial.split(',')[1][1:-1])

    # break into extra queries of 1000 trials if necessary
    for rank in range(1, total_trials//1000 + 1): 
        
        # get next 1000 trials
        extra_query = 'https://clinicaltrials.gov/api/query/study_fields?expr=%22' + condition_name.replace(' ', '+')
        extra_query += '%22&fields=NCTId&min_rnk=' + str(rank*1000+1) + '&max_rnk=' + str((rank+1)*1000) + '&fmt=csv'
        extra_response = requests.get(extra_query).text.splitlines()
        
        # add trials to list
        for trial in extra_response[11:]:
            trials.append(trial.split(',')[1][1:-1])
        
    # return list of trials
    return trials




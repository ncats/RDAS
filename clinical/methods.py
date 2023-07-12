from src import data_model as dm
import requests
import html
import re
from datetime import date
from AlertCypher import AlertCypher
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import spacy
from spacy.matcher import Matcher

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
                print(trial)
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
    try:
        base = trial['FullStudiesResponse']['FullStudies'][0]['Study']
        parsed_trial = parse_module(base, dict())
    except KeyError:
        return None
    
    return parsed_trial

def extract_fields(nctid):
    full_trial_query = 'https://clinicaltrials.gov/api/query/full_studies?expr=' + nctid + '&min_rnk=1&max_rnk=1&fmt=json'
    
    try:
        full_trial_response = requests.get(full_trial_query).json()
        full_trial = parse_trial_fields(full_trial_response)
    except ValueError:
        return None
    
    return full_trial

def cypher_generate(db,now,NCTID,data,node_type,return_single=None):
    ID = None
    existing_node = list()
    pattern = '\'\w+\':'
    query = str()
    
    if node_type == 'ClinicalTrial':
        data['DateCreatedRDAS'] = now
        data['LastUpdatedRDAS'] = now

    data_string = str(data)    
    matches = re.finditer(pattern,data_string)
    for match in matches:
        start = match.start()
        end = match.end() - 2
        data_string = data_string[:start] + ' ' + data_string[start+1:]
        data_string = data_string[:end] + ' ' + data_string[end+1:]
 
    if return_single:
        return data_string

    try: 
        existing_node = 'MATCH (x:{node_type} {data_string}) RETURN ID(x) AS ID LIMIT 1'.format(node_type=node_type,data_string=data_string)
        existing_node = db.run(existing_node).data()
    except (KeyError, IndexError) as e:
        print(f'ERROR: {e}')
    
    if len(existing_node) > 0:
        ID = existing_node[0]['ID']

    if not node_type == 'ClinicalTrial':
        query += 'MATCH (ct: ClinicalTrial {{NCTId:\'{NCTID}\'}}) '.format(NCTID=NCTID)

    else:
        query += 'MERGE ({node_abbr}:{node_type} {data_string}) '.format(node_abbr=dm.abbreviations[node_type],node_type=node_type,data_string=data_string)

    if not node_type == 'ClinicalTrial':
        if ID:
            query += 'MATCH ({node_abbr}:{node_type}) WHERE ID({node_abbr}) = {ID} MERGE (ct){dir1}[:{rel_name}]{dir2}({node_abbr})'.format(ID=ID,node_type=node_type,dir1=dm.rel_directions[node_type][0],dir2=dm.rel_directions[node_type][1],rel_name=dm.relationships[node_type],node_abbr=dm.abbreviations[node_type])
        else:
            query += 'MERGE ({node_abbr}:{node_type} {data_string}) MERGE (ct){dir1}[:{rel_name}]{dir2}({node_abbr}) '.format(data_string=data_string,ID=ID,node_type=node_type,dir1=dm.rel_directions[node_type][0],dir2=dm.rel_directions[node_type][1],rel_name=dm.relationships[node_type],node_abbr=dm.abbreviations[node_type])

    query += 'RETURN TRUE'

    return query

def format_node_data(db,now,trial,node_type,return_single=None):
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

    if return_single:
        return cypher_generate(db,now,NCTID,node_data_list[0],node_type,return_single=return_single)

    for ele in node_data_list:
        if not ele == {}: 
            query = cypher_generate(db, now, NCTID, ele, node_type)
            db.run(query)
            queries.append(query)
    return queries

def condition_map(db):
    remove_words = ['and','or','of','the','and/or','with','to','for','in','this','is','due']
    pos_remove = ['CCONJ','PUNCT','DET','CONJ','PART','ADP']    

    gard_db = AlertCypher('gard')
    nlp = spacy.load('en_core_web_lg')
    conditions = db.run('MATCH (x:Condition) RETURN x.Condition, ID(x)').data()
    diseases = gard_db.run('MATCH (x:GARD) RETURN x.GardId, x.GardName, x.Synonyms').data()
    cond_docs = dict()
    disease_docs = dict()

    for disease in diseases:
        gard_id = disease['x.GardId']
        name = disease['x.GardName']
        syns = disease['x.Synonyms']
        db.run('MERGE (x:GARD {{GardId:\"{gard_id}\",GardName:\"{name}\",Synonyms:{syns}}})'.format(gard_id=gard_id,name=name,syns=syns))

    for cond in conditions:
        cond_id = cond['ID(x)']
        cond_name = cond['x.Condition']
        doc = nlp(cond_name)
        #print(doc)
        tokens = [i.text.lower() for i in doc if not i.is_punct and i.text not in remove_words] #if not i.is_punct and i.text not in remove_words
        cond_docs[cond_id] = tokens
        #print(cond_docs[cond_id])

    for disease in diseases:
        all_tokens = list()
        gard_id = disease['x.GardId']
        name = disease['x.GardName']
        syns = disease['x.Synonyms']
        syns.insert(0,name)
        
        for phrase in syns:
            doc = nlp(name)
            tokens = [i.text.lower() for i in doc if not i.is_punct and i.text not in remove_words] # if not i.is_punct not in remove_words
            all_tokens.append(tokens)

        disease_docs[gard_id] = all_tokens
        #print(disease_docs[gard_id])
        

    for idx, (k,v) in enumerate(disease_docs.items()):
        for tokens in v:
            for k2,v2 in cond_docs.items():
                check = all(item in v2 for item in tokens)
                if check:
                    query = 'MATCH (x:GARD) WHERE x.GardId = \"{gard_id}\" MATCH (z:Condition) WHERE ID(z) = {cond_id} MERGE (x)<-[:mapped_to_gard]-(z) RETURN TRUE'.format(cond_id=k2,gard_id=k)
                    db.run(query)
                    print(query)
                    print(tokens)
                    print(v2)
                    print(f'{k} mapped to {k2}')
        print(f'{idx} GARD Processed')

def drug_normalize(drug):
    print(drug)
    new_val = drug.encode("ascii", "ignore")
    updated_str = new_val.decode()
    updated_str = re.sub('\W+',' ', updated_str)
    print(updated_str)
    return updated_str

def create_drug_connection(db,rxdata,drug_id):
    rxnormid = rxdata['RxNormID']
    db.run('MATCH (x:Intervention) WHERE ID(x)={drug_id} MERGE (y:Drug {{RxNormID:{rxnormid}}}) MERGE (y)<-[:has_participant]-(x)'.format(rxnormid=rxnormid, drug_id=drug_id))

    for k,v in rxdata.items():
        key = k.replace(' ','')
        db.run('MERGE (y:Drug {{RxNormID:{rxnormid}}}) WITH y MATCH (x:Intervention) WHERE ID(x)={drug_id} MERGE (y)<-[:has_participant]-(x) SET y.{key} = {value}'.format(rxnormid=rxdata['RxNormID'], drug_id=drug_id, key=key, value=v))

def get_rxnorm_data(drug):
    rq = 'https://rxnav.nlm.nih.gov/REST/rxcui.json?name={drug}&search=2'.format(drug=drug)
    response = requests.get(rq)
    try:
        rxdata = dict()
        response = response.json()['idGroup']['rxnormId'][0]
        rxdata['RxNormID'] = response

        rq2 = 'https://rxnav.nlm.nih.gov/REST/rxcui/{rxnormid}/allProperties.json?prop=codes+attributes+names+sources'.format(rxnormid=response)
        response = requests.get(rq2)
        response = response.json()['propConceptGroup']['propConcept']

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
    for match_id, start, end in matches:
        span = doc[start:end].text
        rxdata = get_rxnorm_data(span.replace(' ','+'))

        if rxdata:
            create_drug_connection(db,rxdata,drug_id)
        else:
            print('Map to RxNorm failed for intervention name: {drug_name}'.format(drug_name=drug_name))

def rxnorm_map(db):
    print('Starting RxNorm data mapping to Drug Interventions')
    nlp = spacy.load('en_ner_bc5cdr_md')
    pattern = [{'ENT_TYPE':'CHEMICAL'}]
    matcher = Matcher(nlp.vocab)
    matcher.add('DRUG',[pattern])

    results = db.run('MATCH (x:Intervention) WHERE x.InterventionType = "Drug" RETURN x.InterventionName, ID(x)')

    for idx,res in enumerate(results.data()):
        print(idx)
        drug_id = res['ID(x)']
        drug = res['x.InterventionName']
        drug = drug_normalize(drug)
        drug_url = drug.replace(' ','+')
        rxdata = get_rxnorm_data(drug_url)

        if rxdata:
            create_drug_connection(db, rxdata, drug_id)
        else:
            doc = nlp(drug)
            matches = matcher(doc)
            nlp_to_drug(db,doc,matches,drug,drug_id)


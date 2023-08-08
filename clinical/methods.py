from skr_web_api import Submission, METAMAP_INTERACTIVE_URL
from src import data_model as dm
import json
import os
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
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
import spacy
import nltk
from nltk.stem import PorterStemmer
nltk.download("punkt")
from spacy.matcher import Matcher
from fuzzywuzzy import fuzz

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

def is_acronym(word):
    if len(word.split(' ')) > 1:
        return False
    elif bool(re.match(r'\w*[A-Z]\w*', word[:len(word)-1])) and (word[len(word)-1].isupper() or word[len(word)-1].isnumeric()):
        return True
    else:
        return False

def get_unmapped_conditions(db):
    conditions = db.run('MATCH (x:Condition) where not (x)-[:mapped_to_gard]-(:GARD) RETURN x.Condition, ID(x)').data()
    return conditions

#GATHER DATA FROM A LIST OF METAMAP MAPPINGS FOR A DISEASE AND FILTER MAPPING TO SINGLE RESULT USING TEXT SIMILARITY ALGORITHMS
def filter_mappings(mappings,cond_name):
    map_details = dict()
    for idx,mapping in enumerate(mappings):
        meta_score = int(mapping['MappingScore'].replace('-',''))
        candidates = mapping['MappingCandidates'][0]
        CUI = candidates['CandidateCUI']
        candidate_pref = candidates['CandidatePreferred']
        candidate_match = candidates['CandidateMatched']
        fuzz_score_cond_pref = fuzz.token_sort_ratio(cond_name, candidate_pref)

        map_details[idx] = {'meta_score':meta_score,'fuzz_score_cond_pref':fuzz_score_cond_pref,'CUI':CUI,'candidate_pref':candidate_pref,'candidate_match':candidate_match}

    map_details = {k:v for (k,v) in map_details.items() if v['meta_score'] > 750}
    if len(map_details) > 0:
        max_cond_pref = max(map_details, key= lambda x: map_details[x]['fuzz_score_cond_pref'])
        return map_details[max_cond_pref]['CUI']

#CONVERT ACCENTED CHARACTERS TO THEIR ENGLISH EQUIVILANTS AND REMOVE TRAILING WHITESPACE
def normalize(phrase):
    phrase = unidecode(phrase)
    phrase = re.sub(r'\W+', ' ', phrase)
    return phrase

def condition_map(db):
    #SETUP DATABASE OBJECTS
    gard_db = AlertCypher('gard')
    
    #SETUP METAMAP INSTANCE
    INSTANCE = Submission(os.environ['METAMAP_EMAIL'],os.environ['METAMAP_KEY'])
    INSTANCE.init_generic_batch('metamap','-J acab,anab,comd,cgab,dsyn,emod,fndg,inpo,mobd,neop,patf,sosy --JSONn') #--sldiID
    INSTANCE.form['SingLinePMID'] = True
    
    #POPULATE CLINICAL DB WITH GARD DATA WITH UMLS MAPPINGS FROM GARD NEO4J DB
    gard_res = gard_db.run('MATCH (x:GARD) RETURN x.GardId as GardId, x.GardName as GardName, x.Synonyms as Synonyms, x.UMLS as gUMLS')
    for gres in gard_res.data():
        gUMLS = gres['gUMLS']
        name = gres['GardName']
        gard_id = gres['GardId']
        syns = gres['Synonyms']
    
    if gUMLS:
        db.run('MERGE (x:GARD {{GardId:\"{gard_id}\",GardName:\"{name}\",Synonyms:{syns},UMLS:{gUMLS}}})'.format(name=gres['GardName'],gard_id=gres['GardId'],syns=gres['Synonyms'],gUMLS=gres['gUMLS']))
    else:
        db.run('MERGE (x:GARD {{GardId:\"{gard_id}\",GardName:\"{name}\",Synonyms:{syns}}})'.format(name=gres['GardName'],gard_id=gres['GardId'],syns=gres['Synonyms']))
    
    #RUN BATCH METAMAP ON ALL CONDITIONS IN CLINICAL DB
    res = db.run('MATCH (c:Condition) RETURN c.Condition as condition, ID(c) as cond_id')
    cond_strs = [f"{i['cond_id']}|{normalize(i['condition'])}\n" for i in res]
    with open(f'{sysvars.ct_files_path}metamap_cond.txt','w') as f:
        f.writelines(cond_strs)
    
    if not os.path.exists(f'{sysvars.ct_files_path}metamap_cond_out.json'):
        INSTANCE.set_batch_file(f'{sysvars.ct_files_path}metamap_cond.txt') #metamap_cond.txt
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
        with open(f'{sysvars.ct_files_path}metamap_cond_out.json','r') as f:
            data = json.load(f)['AllDocuments']
            print(data)

    #PARSE OUT DATA FROM BATCH METAMAP AND FILTER MAPPINGS CANDIDATES TO ONE RESULT
    for entry in data:
        utterances = entry['Document']['Utterances'][0]
        utt_text = utterances['UttText']
        phrases = utterances['Phrases'][0]
        mappings = phrases['Mappings']
        cond_id = utterances['PMID']
        CUI = filter_mappings(mappings,utt_text)
        if CUI:
            db.run('MATCH (x:Condition) WHERE ID(x) = {cond_id} SET x.UMLS = \"{CUI}\"'.format(CUI=CUI,cond_id=cond_id))

    #CREATE RELATIONSHIPS BETWEEN CONDITION AND GARD BASED ON UMLS CODES MAPPED
    res = db.run('MATCH (x:Condition) RETURN x.UMLS AS UMLS,ID(x) as cond_id')
    cond_dict = {i['UMLS']:i['cond_id'] for i in res}
    print('APPLYING MAPPINGS TO DATABASE')
    for idx,(umls,cond_id) in enumerate(cond_dict.items()):
        print(idx,cond_id,umls)
        db.run('MATCH (x:GARD) WHERE \"{umls}\" IN x.UMLS MATCH (y:Condition) WHERE ID(y) = {cond_id} MERGE (y)-[:mapped_to_gard]->(x)'.format(umls=umls,cond_id=cond_id))

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


from skr_web_api import Submission, METAMAP_INTERACTIVE_URL
import os
from AlertCypher import AlertCypher
import json
from fuzzywuzzy import fuzz
import re
from unidecode import unidecode

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

#SETUP DATABASE OBJECTS
gard_db = AlertCypher('gard')
db = AlertCypher('clinicaltest2')

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
with open('metamap_cond.txt','w') as f:
    f.writelines(cond_strs)

if not os.path.exists('metamap_cond_out.json'):
    INSTANCE.set_batch_file('metamap_cond.txt') #metamap_cond.txt
    response = INSTANCE.submit()
    try:
        data = response.content.decode().replace("\n"," ")
        data = re.search(r"({.+})", data).group(0)
    
    except Exception as e:
        print(e)
        data = None

    try:
        data = json.loads(data)
        with open('metamap_cond_out.json','w') as f:
            json.dump(data,f)
        data = data['AllDocuments']

    except Exception as e:
        print(e)

else:
    with open('metamap_cond_out.json','r') as f:
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


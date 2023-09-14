from skr_web_api import Submission, METAMAP_INTERACTIVE_URL
import os
from AlertCypher import AlertCypher
import json
from fuzzywuzzy import fuzz
import re
from unidecode import unidecode
import pandas as pd

#GATHER DATA FROM A LIST OF METAMAP MAPPINGS FOR A DISEASE AND FILTER MAPPING TO SINGLE RESULT USING TEXT SIMILARITY ALGORITHMS
def filter_mappings(mappings,cond_name):
    map_details = list()
    for idx,mapping in enumerate(mappings):
        meta_score = int(mapping['MappingScore'].replace('-',''))
        candidates = mapping['MappingCandidates'][0]
        CUI = candidates['CandidateCUI']
        candidate_pref = candidates['CandidatePreferred']
        candidate_match = candidates['CandidateMatched']

        map_details.append(CUI)
    
    if len(map_details) > 0:    
        return map_details

def normalize_fda(phrase):
    phrase = phrase.replace('Treatment of','')
    phrase = phrase.replace('1. Treatment of','')
    phrase = phrase.replace('For induction of','')
    phrase = phrase.replace('Relief of symptoms of','')
    phrase = phrase.replace('Treatment and prevention of','')
    phrase = phrase.replace('For induction of','')

    return phrase

#CONVERT ACCENTED CHARACTERS TO THEIR ENGLISH EQUIVILANTS AND REMOVE TRAILING WHITESPACE
def normalize(phrase):
    phrase = unidecode(phrase)
    phrase = re.sub(r'\W+', ' ', phrase)
    return phrase

def umls_to_gard(db,CUI):
    print('UMLS TO GARD: ',CUI)
    res = db.run('MATCH (x:GARD) WHERE \"{CUI}\" IN x.UMLS RETURN x.GardId as gard_id, x.GardName as name'.format(CUI=CUI)).data()
    print('RES: ',res)
    if res:
        data = list()
        names = list()
        for i in res:
            gard_id = i['gard_id']
            gard_name = i['name']
            data.extend([gard_id])
            names.extend([gard_name])
        return {'gard_id':data, 'gard_name':names}

#SETUP DATABASE OBJECTS
gard_db = AlertCypher('gard')
#db = AlertCypher('clinicaltest2')

#SETUP METAMAP INSTANCE
INSTANCE = Submission(os.environ['METAMAP_EMAIL'],os.environ['METAMAP_KEY'])
INSTANCE.init_generic_batch('metamap','-J acab,anab,comd,cgab,dsyn,emod,fndg,inpo,mobd,neop,patf,sosy --JSONn') #--sldiID
INSTANCE.form['SingLinePMID'] = True

'''
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
'''
#RUN BATCH METAMAP ON ALL CONDITIONS IN CLINICAL DB
df = pd.read_csv('orphan_substance_public_records_6_14_2023.csv',index_col=False,encoding='iso-8859-1')
r,c = df.shape
fda_dict = dict()

for idx in range(r):
    row = df.iloc[idx]
    fda_dict[str(idx)] = normalize(row['Orphan Designation'])

cond_strs = [f"{k}|{v}\n" for (k,v) in fda_dict.items()]
with open('metamap_fda.txt','w') as f:
    f.writelines(cond_strs)
 
INSTANCE.set_batch_file('metamap_fda.txt') #metamap_cond.txt
response = INSTANCE.submit()
try:
    data = response.content.decode().replace("\n"," ")
    data = re.search(r"({.+})", data).group(0)

except Exception as e:
    print(e)
    data = None

try:
    data = json.loads(data)
    with open('metamap_fda_out.json','w') as f:
        json.dump(data,f)
    data = data['AllDocuments']

except Exception as e:
    print(e)

df['GARD_IDS'] = ''
df['GARD_NAMES'] = ''
df['UMLS_CODES'] = ''

for entry in data:
    utterances = entry['Document']['Utterances'][0]
    utt_text = utterances['UttText']
    phrases = utterances['Phrases'][0]
    mappings = phrases['Mappings']
    idx = utterances['PMID']
    CUI = filter_mappings(mappings,utt_text)
    print(utt_text)
    if CUI:
        all_gard = list()
        all_names = list()

        for umls in CUI:
            data = umls_to_gard(gard_db,umls)
            print('DATA: ',data)
            if data:
                all_gard.extend(data['gard_id'])
                all_names.extend(data['gard_name'])

        if data:
            print(all_gard)
            print(all_names)
        if len(all_gard) > 0:
            df.at[int(idx),'GARD_IDS'] = all_gard
        if len(all_names) > 0:
            df.at[int(idx),'GARD_NAMES'] = all_names
        df.at[int(idx),'UMLS_CODES'] = CUI

df.to_csv('fda_gard_mapping_6_14_2023.csv',index=False)

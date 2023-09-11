import sys
import os
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from clinical import methods as rdas
from skr_web_api import Submission, METAMAP_INTERACTIVE_URL
from AlertCypher import AlertCypher
import json
import re
from unidecode import unidecode
import pandas as pd
import numpy as np

#SETUP DATABASE OBJECTS
gard_db = AlertCypher('gard')
#SETUP METAMAP INSTANCE
INSTANCE = Submission(os.environ['METAMAP_EMAIL'],os.environ['METAMAP_KEY'])
INSTANCE.init_generic_batch('metamap','-J acab,anab,comd,cgab,dsyn,emod,fndg,inpo,mobd,neop,patf,sosy --JSONn') #--sldiID
INSTANCE.form['SingLinePMID'] = True

df = pd.read_csv('/home/leadmandj/github/alert/scripts/orphan_substance_public_records_6_14_2023.csv',index_col=False,encoding='iso-8859-1')
r,c = df.shape
fda_dict = dict()

print('PARSING ORIGINAL FILE')
for idx in range(r):
    row = df.iloc[idx]
    orphan = re.findall(r"([\d]+\. )(.*?)(?=([\d]+\.)|($))",row['Orphan Designation'])
    if not orphan == []:
        fda_dict[str(idx)] = [rdas.normalize(i[1]) for i in orphan]
    else:
        fda_dict[str(idx)] = [rdas.normalize(row['Orphan Designation'])]

print('CREATING METAMAP INPUT FILE')
cond_strs = list()
for (k,v) in fda_dict.items():
    if len(v) == 1:
        cond_strs.append(f"{k}|{v[0]}\n")
    else:
        for match in v:
            cond_strs.append(f"{k}|{match}\n")

with open('metamap_fda.txt','w') as f:
    f.writelines(cond_strs)

print('RUNNING METAMAP')
if not os.path.exists(f'/home/leadmandj/github/alert/scripts/metamap_fda_out.json'):
    INSTANCE.set_batch_file('metamap_fda.txt') #metamap_cond.txt
    #response = INSTANCE.submit()
    try:
        data = response.content.decode().replace("\n"," ")
        data = re.search(r"({.+})", data).group(0)

    except Exception as e:
        print(e)
        data = None

    try:
        print('CREATING METAMAP OUTPUT')
        data = json.loads(data)
        with open('metamap_fda_out.json','w') as f:
            json.dump(data,f)
        data = data['AllDocuments']

    except Exception as e:
        print(e)

else:
    with open('/home/leadmandj/github/alert/scripts/metamap_fda_out.json','r') as f:
        data = json.load(f)['AllDocuments']


gard_dict = dict()
new_df = pd.DataFrame()

for entry in data:
    utterances = entry['Document']['Utterances'][0]
    utt_text = utterances['UttText']
    phrases = utterances['Phrases'][0]
    mappings = phrases['Mappings']
    idx = utterances['PMID']
    CUI = rdas.filter_mappings(mappings,utt_text)
    try:
        cur = gard_dict[idx]
    except KeyError as e:
        gard_dict[idx] = {'gard_id':list(),'gard_name':list()}
        cur = gard_dict[idx]

        
    if CUI:
        CUI = CUI['CUI']
        all_gard = list()
        all_names = list()

        for umls in CUI:
            data = rdas.umls_to_gard(gard_db,umls)
            if data:
                cur['gard_id'].extend(list(set(data['gard_id'])))
                cur['gard_name'].extend(list(set(data['gard_name'])))
    else:
        gard_dict[idx] = cur

    print(utt_text)
    print(gard_dict[idx])

for k,v in gard_dict.items():
    row = df.iloc[int(k)]
    if not v['gard_id'] == []:
        for idx,gard_id in enumerate(v['gard_id']):
            gard_name = v['gard_name'][idx]
            row['GARD_ID'] = gard_id
            row['GARD_NAME'] = gard_name
            row['ENTRY_ID'] = k
            row['MATCH_TYPE'] = 'METAMAP'
            new_df = new_df.append(row,ignore_index=True)
    else:
        row['GARD_ID'] = np.nan
        row['GARD_NAME'] = np.nan
        row['ENTRY_ID'] = k
        new_df = new_df.append(row,ignore_index=True)

new_df = new_df.drop_duplicates()

res = gard_db.run('MATCH (x:GARD) RETURN x.GardName as gname, x.GardId as gid').data()
unmapped_df = new_df[new_df['GARD_ID'].isna()]
new_df = new_df[~new_df['GARD_ID'].isna()]

r,c = unmapped_df.shape
for idx in range(r):
    row = unmapped_df.iloc[idx]
    phrase = rdas.normalize(row['Orphan Designation']).lower()
    for entry in res:
        gard_id = entry['gid']
        gard_name = rdas.normalize(entry['gname']).lower()
        if gard_name in phrase:
            row['GARD_ID'] = gard_id
            row['GARD_NAME'] = gard_name
            row['MATCH_TYPE'] = 'STRING'
            new_df = new_df.append(row,ignore_index=True)
        else:
            new_df = new_df.append(row,ignore_index=True)

new_df = new_df.drop_duplicates()
'''
for i in range(r):
    row = df.iloc[i]
    if not str(i) in gard_dict:
        row['GARD_ID'] = None
        row['GARD_NAME'] = None
        new_df = new_df.append(row,ignore_index=True)
'''
new_df.to_csv('/home/leadmandj/github/alert/scripts/fda_gard_mapping_6_14_2023.csv',index=False)



from AlertCypher import AlertCypher
import methods as rdas
import spacy
from spacy.matcher import Matcher
import pandas as pd

db = AlertCypher('clinicaltest')
results = db.run('MATCH p = (c:ClinicalTrial)-[]-(d:Condition)-[]-(g:GARD) return distinct g.GardName').data()
neo = [i['g.GardName'] for i in results]

ctgov = rdas.webscrape_ctgov_diseases()[0]
nlp = spacy.load('en_core_web_lg')

neo_list = {i:rdas.normalize_mapping(nlp,i) for i in neo}
ctgov_list = {i:rdas.normalize_mapping(nlp,i) for i in ctgov}
ctgov_map = 0
list_frame = list()

for k,tokens in ctgov_list.items():
    for k2,v2 in neo_list.items():
        if True:
            if True:
                match_cnt = 0

                disease_length = len(tokens)
                cond_length = len(v2)

                if cond_length > disease_length:
                    big_tokens = v2
                    small_tokens = tokens
                else:
                    big_tokens = tokens
                    small_tokens = v2

                for item1 in big_tokens:
                    for item2 in small_tokens:
                        if item2 == item1:
                            match_cnt += 1

                big_length = len(big_tokens)
                small_length = len(small_tokens)
                accur = (match_cnt/big_length)*100
                if accur > 67:
                    print('MAPPED')
                    print(f'big length: {big_length}')
                    print(f'match count: {match_cnt}')
                    print('match accuracy: ' + str((match_cnt/big_length)*100))
                    print(big_tokens)
                    print(small_tokens)
                    ctgov_map +=1
                    list_frame.append([k,k2,tokens,v2,accur,'MAPPED'])
                elif match_cnt > 1:
                    list_frame.append([k,k2,tokens,v2,accur,'NOT MAPPED'])
                    
df = pd.DataFrame(list_frame,columns=['CTGOV_NAME','NEO4J_NAME','CTGOV','NEO4J','ACCURACY','IS_MAPPED'])
print(df)
print(ctgov_map)
df.to_csv('mapping_accuracies.csv')


from AlertCypher import AlertCypher
gard_db = AlertCypher('gard')
ct_db = AlertCypher('clinicaltest2')
import csv
import re
import spacy
import scispacy
from scispacy.linking import EntityLinker
from spacy.attrs import ENT_TYPE

def norm(word):
    string = re.sub("\(.*?\)","",word)
    string = string.rstrip()
    return string

def filter_matches(umls_doc):
    matches = [(k,v) for k,v in umls_doc if float(v) == 1.0]

    if len(matches) == 0 and len(umls_doc) > 0:
        matches.append(max(umls_doc, key=lambda x: x[1]))

    return matches

def filter_disease_terms(doc):
    def filter_token(token):
        return not token.ent_type_ == 'CHEMICAL'

    filtered_tokens = list(filter(filter_token,doc))
    filtered_tokens = [i.text for i in filtered_tokens]
    return " ".join(filtered_tokens)

def nlp_filter(nlp,phrase,filter_only=None):
    norm_phrase = norm(phrase)
    try:
        umls_doc = nlp(norm_phrase)
        print(umls_doc)
        umls_doc = filter_disease_terms(umls_doc)
        umls_doc = nlp(umls_doc)
        print(umls_doc)
        umls_doc = umls_doc.ents[0]._.kb_ents
    except Exception as e:
        return None

    matches = filter_matches(umls_doc)

    return matches

def map_umls(dumls, cumls):
    mappings = list()
    for idx,(cond_id,cmap) in enumerate(cumls.items()): #[('C324934',1.0),('C394392',1.0)]
        print(idx)
        if not cmap == [] or not cmap == None:
            for (gard_id,dmap) in dumls.items(): #{'GARD:0000233':[('C324934',1.0),('C394392',1.0)]}
                if not dmap == [] or not dmap == None:
                    try:
                        for (dcode,score1) in dmap:
                            for (ccode,score2) in cmap:
                                if ccode == dcode:
                                    #with open('sim_match.csv', 'a', newline='') as file:
                                        #writer = csv.writer(file)
                                        #writer.writerow([cond_id,gard_id,ccode])
                                    query = 'MATCH (x:GARD) WHERE x.GardId = \"{gard_id}\" MATCH (z:Condition) WHERE ID(z) = {cond_id} MERGE (x)<-[r:mapped_to_gard]-(z) SET r.GardProbability = {score1} SET r.ConditionProbability = {score2} SET r.MatchedUMLS = \"{umls}\" RETURN TRUE'.format(cond_id=cond_id,gard_id=gard_id,score1=score1,score2=score2,umls=ccode)
                                    ct_db.run(query)
 
                    except Exception:
                        pass
            
nlp = spacy.load('en_ner_bc5cdr_md')
nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "linker_name": "umls"})

diseases = gard_db.run('MATCH (x:GARD) RETURN x.GardId, x.GardName, x.Synonyms').data()
conditions = ct_db.run('MATCH (x:Condition) RETURN x.Condition, ID(x)').data()
df = list()
cond_docs = dict()
disease_docs = dict()
disease_umls = dict()

diseases = gard_db.run('MATCH (x:GARD) RETURN x.GardId, x.GardName, x.Synonyms').data()
'''
for disease in diseases:
    gard_id = disease['x.GardId']
    name = disease['x.GardName']
    syns = disease['x.Synonyms']
    ct_db.run('MERGE (x:GARD {{GardId:\"{gard_id}\",GardName:\"{name}\",Synonyms:{syns}}})'.format(gard_id=gard_id,name=name,syns=syns))
'''
for cond in conditions:
    cond_id = cond['ID(x)']
    cond_name = cond['x.Condition']
    try:
        cond_umls = nlp_filter(nlp,cond_name) #nlp_filter(nlp,norm(cond_name).lower())
        cond_umls = [i for i in cond_umls if i is not None]
        temp = list()
        for i in cond_umls:
            if type(i) == list:
                temp.extend(i)
            else:
                temp.append(i)
        temp = list(set([i for i in temp]))
        temp = filter_matches(temp)
        if len(temp) > 0:
            cond_umls = temp
    except Exception as e:
        pass
    cond_docs[cond_id] = cond_umls

for disease in diseases:
    gard_id = disease['x.GardId']
    name = disease['x.GardName']
    syns = disease['x.Synonyms']
    syns.insert(0,name)
    disease_docs[gard_id] = syns # norm(i).lower() for i in syns if len(i.split(" ")) > 1
    try:
        disease_umls[gard_id] = [nlp_filter(nlp,i) for i in syns]
        disease_umls[gard_id] = [i for i in disease_umls[gard_id] if i is not None]
        temp = list()
        for i in disease_umls[gard_id]:
            if type(i) == list:
                temp.extend(i)
            else:
                temp.append(i)
        temp = list(set([i for i in temp]))
        temp = filter_matches(temp)
        if len(temp) > 0:
            disease_umls[gard_id] = temp

    except Exception as e:
        print(e)
        pass

final_maps = map_umls(disease_umls,cond_docs)
            


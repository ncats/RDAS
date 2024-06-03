import pandas as pd
import numpy as np
import spacy
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker
from remove_duplicate_entities import remove_duplicate_entities


ENCODING = 'latin1'


def load_model(model_name):
    while True:
        try:
            if model_name == 'en_ner_craft_md':
                nlp = spacy.load('en_ner_craft_md')
            elif model_name == 'en_ner_jnlpba_md':
                nlp = spacy.load('en_ner_jnlpba_md')
            elif model_name == 'en_ner_bc5cdr_md':
                nlp = spacy.load('en_ner_bc5cdr_md')
            elif model_name == 'en_ner_bionlp13cg_md':
                nlp = spacy.load('en_ner_bionlp13cg_md')
            else:
                print("Wrong model name")
                return

            nlp.add_pipe('remove_duplicate_entities')
            nlp.add_pipe('abbreviation_detector')
            nlp.add_pipe('scispacy_linker', config={'linker_name':'umls',
                                                    'resolve_abbreviations':True,
                                                    'threshold':0.8})
            break
        except:
            pass
    return nlp
    

def get_umls_concepts(nlp, text):
    text.sort_values(by=['APPLICATION_ID'], inplace=True)
    text.reset_index(drop=True, inplace=True)
    
    docs = list(nlp.pipe(text['ABSTRACT_TEXT']))
    linker = nlp.get_pipe('scispacy_linker')

    meta_df_lst = []
    
    for idx, doc in enumerate(docs):
        if len(doc.ents) > 0:
            concept_entity = []
            all_umls_data = []
        
            for ent in doc.ents:
                if len(ent._.umls_ents) > 0:
                    highest_umls_ent = ent._.umls_ents[0]
                    concept_entity.append((highest_umls_ent[0], str(ent)))
                    umls_data = linker.kb.cui_to_entity[highest_umls_ent[0]]
                    all_umls_data.append(umls_data)

            if len(concept_entity) > 0:
#                concept_entity_df = pd.DataFrame(concept_entity)
#                concept_entity_df.columns = ['concept_id', 'entity']
            
#                all_umls_data_df = pd.DataFrame(all_umls_data)
#                entity_umls_data_df = concept_entity_df.merge(all_umls_data_df, on='concept_id', how='left')
#                entity_umls_data_df['application_id'] = text.loc[idx, 'APPLICATION_ID']
#            
#                entity_umls_data_df = entity_umls_data_df[['application_id', 'entity', 'concept_id', 'canonical_name', 'types']]
#                entity_umls_data_df.drop_duplicates(subset=['concept_id'], keep='first', inplace=True)

                all_umls_data_df = pd.DataFrame(all_umls_data)
                all_umls_data_df['APPLICATION_ID'] = text.loc[idx, 'APPLICATION_ID']
                all_umls_data_df = all_umls_data_df[['APPLICATION_ID', 'concept_id', 'canonical_name', 'types']]
                all_umls_data_df.columns = ['APPLICATION_ID', 'UMLS_CUI', 'UMLS_CONCEPT', 'SEMANTIC_TYPES']
                all_umls_data_df.drop_duplicates(subset=['UMLS_CUI'], keep='first', inplace=True)

                meta_df_lst.append(all_umls_data_df)
                
    return pd.concat(meta_df_lst)

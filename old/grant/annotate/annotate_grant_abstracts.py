import glob
import pandas as pd
from annotate_text import *


ENCODING = 'latin1'
MODELS = ['en_ner_craft_md', 'en_ner_jnlpba_md', 'en_ner_bc5cdr_md', 'en_ner_bionlp13cg_md']


# Get CSV files lists from projects and abstracts folders
input_file_path = "../../data_neo4j/annotation_files"
input_files = glob.glob(input_file_path + "/*.csv")

output_file_path = "../../data_neo4j/grants_umls/grants_umls_"


# Annotate text with four scispaCy models
for model in MODELS:
    print(f'*** Annotate with {model} model ***')
    
    for file in input_files:
        yr_idx = len(input_file_path) + 17
        year = file[yr_idx : yr_idx+4]
        
        nlp = load_model(model)
        text = pd.read_csv(file, encoding=ENCODING, dtype={'APPLICATION_ID':int, 'ABSTRACT_TEXT':str})
        umls = get_umls_concepts(nlp, text)
        
        output_file = output_file_path + year + ".csv" 
        
        if model == 'en_ner_craft_md':
            umls.to_csv(output_file, index=False)
        else:
            umls.to_csv(output_file, index=False, mode='a', header=False)
            
        print("Added annotations to", output_file)


print("***** ALL DONE *****")

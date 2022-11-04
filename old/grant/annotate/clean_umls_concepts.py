import glob
import pandas as pd
from remove_general_umls_concepts import clean_annotation_output

ENCODING = 'latin1'

# Get CSV files lists from a folder
input_path = '../../data_neo4j/grants_umls/'
files = glob.glob(input_path + '*.csv')

# Clean all files
output_path = '../../data_neo4j/grants_umls/'

keep_semantic_types = pd.read_csv('semantic_type_keep.csv', usecols=['TUI'])
keep_semantic_types = keep_semantic_types['TUI'].to_list()

remove_umls_concepts = pd.read_csv('umls_concepts_remove.csv', usecols=['UMLS_CUI'])
remove_umls_concepts = remove_umls_concepts['UMLS_CUI'].to_list()

for file in files:
    umls = clean_annotation_output(file, keep_semantic_types, remove_umls_concepts)
    output_file = output_path + "RD_UMLS_CONCEPTS_" + file[41:45] + '.csv'
    umls.to_csv(output_file, index=False)

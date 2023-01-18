import pandas as pd
import re


def clean_annotation_output(file, keep_semantic_types, remove_umls_concepts):
    umls = pd.read_csv(file)
    umls.drop_duplicates(subset=['APPLICATION_ID', 'UMLS_CUI'], keep='first', inplace=True)
    umls.sort_values(by='APPLICATION_ID', inplace=True)
 
    # Keep only selected SEMANTIC TYPES
    umls['SEMANTIC_TYPES'] = umls['SEMANTIC_TYPES'].apply(extract_types)
    keep = umls['SEMANTIC_TYPES'].apply(lambda types: any(t in keep_semantic_types for t in types))
    umls = umls[keep]

    # Remove CUIs of general concepts
    umls = umls[~umls['UMLS_CUI'].isin(remove_umls_concepts)]
    return umls


def extract_types(types_str):
    types = types_str.split(", ")
    types = [re.sub('[^A-Za-z0-9]+', '', t) for t in types]
    return types

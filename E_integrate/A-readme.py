
# These are the steps after GARD, ClinicalTrail, Publication, Grant RAW data are processed.

''' 1. Before generating the Publication nodes: '''

#1.1 The following process is for "2_clinical_trial"
'''
init_5_clinical_trial_retrieve_pmids_umlti.py 
init_6_clinical_trial_pmids_not_in_Article_umlti.py
init_7_clinical_trial_pmids_not_in_Article_table_pubtator_multi.py
init_8_update-EPI-NHS-of-Article-multi.py
'''


#1.2 The following process is for "4_grant"
'''
init_12_grant_publications_not_in_Article_table_multi.py 
init_13_grant_publications_not_in_Article_table_pubtator_multi.py 
init_14_grant_update-EPI-NHS-of-Article-multi.py 
'''

      
''' 2. Generate person from ClinicalTrail, Grant, Publication'''
# 3_generate_person_.py                             
# 3_generate_person_1_clinical_trial.py             
# 3_generate_person_2_grant.py                      
# 3_generate_person_3_publication.py 

# 3_generate_person_4_rdas_person_id.py
# 3_generate_person_5_rdas_person_id_to_int.py
import os

current_version = 1.4

current_user = 'leadmandj'
base_directory_name = 'RDAS'
base_path = '/home/{current_user}/{base_directory_name}/'.format(current_user=current_user, base_directory_name=base_directory_name)

backup_path = '{base_path}backup/'.format(base_path=base_path)
transfer_path = '{base_path}transfer/'.format(base_path=base_path)
migrated_path = '{base_path}migrated/'.format(base_path=base_path)
approved_path = '{base_path}approved/'.format(base_path=base_path)

dump_dirs = ['clinical','pubmed','grant','gard']
db_abbrevs = {'ct':'clinical', 'pm':'pubmed', 'gnt':'grant'}

ct_files_path = '{base_path}/clinical/src/'.format(base_path=base_path)
pm_files_path = '{base_path}/pubmed/src/'.format(base_path=base_path)
gnt_files_path = '{base_path}/grant/src/'.format(base_path=base_path)
gard_files_path = '{base_path}/gard/src/'.format(base_path=base_path)

ct_db = 'clinical'
pm_db = 'pubmed'
gnt_db = 'grant'
gard_db = 'gard'

epiapi_url = "http://ncats-rdas-lnx-dev.ncats.nih.gov:80/api/"
rdas_urls = {'dev':'rdas-dev.ncats.nih.gov','test':"ncats-neo4j-lnx-test1.ncats.nih.gov",'prod':"ncats-neo4j-lnx-prod1.ncats.nih.gov"}


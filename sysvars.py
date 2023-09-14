current_version = 1.1

current_user = 'leadmandj'
base_directory_name = 'github/alert'
base_path = '/home/{current_user}/{base_directory_name}/'.format(current_user=current_user, base_directory_name=base_directory_name)

dump_path = '/home/{current_user}/dumps/'.format(current_user=current_user)
dump_dirs_test = {'clinicaltrials':'clinical','publication':'pubmed','grant':'grant','gard':'gard'} #FROM:TO
dump_dirs_prod = {}

ct_files_path = '{base_path}/clinical/src/'.format(base_path=base_path)
pm_files_path = '{base_path}/pubmed/src/'.format(base_path=base_path)
gnt_files_path = '{base_path}/grant/src/'.format(base_path=base_path)
gard_files_path = '{base_path}/gard/src/'.format(base_path=base_path)

ct_db = 'clinicaltest'
pm_db = 'pubmedtest'
gnt_db = 'granttest'
gard_db = 'gardtest'

epiapi_url = "http://ncats-rdas-lnx-dev.ncats.nih.gov:80/api/"
rdas_urls = {'test':"ncats-neo4j-lnx-test1.ncats.nih.gov",'prod':"ncats-neo4j-lnx-prod.ncats.nih.gov"}


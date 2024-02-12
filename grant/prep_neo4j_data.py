"""
This module defines a series of stages for preprocessing raw CSV grant
data downloaded from ExPORTER, and defines a function prep_data
for running those stages sequentially.
To greatly optimize the speed of this process, we use pygit2 to track
which files have changed and only annotate those files.
update_grant.py calls prep_data() from this module to figure out
which files need to be added to the database.
"""

import os
import sys
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import json
import re
import ast
import shutil
#import pygit2
import glob
from typing import TypedDict
import pandas as pd
from prepare_annotation_text import prepare_phr_aim
from annotate_text import *
from remove_general_umls_concepts import clean_annotation_output
from AlertCypher import AlertCypher
import grant.methods as rdas
import threading

ENCODING = "latin1"
raw_path = ""
neo4j_path = ""
years_to_annotate = set()

class FilesToAdd(TypedDict):
	"""
	FilesToAdd is just a dict with these particular keys (corresponding to the
	names of subdirectories of CSV files) and values that are of type list[str].
	Each element of that list[str] is a path (i.e. "subdir/file.csv")
	"""
	abstracts: list
	annotation_source: list
	annotation_umls: list
	clinical_studies: list
	disease: list
	publications: list
	patents: list
	projects: list
	link_tables: list


def data_raw(subpath: str):
	global raw_path
	return os.path.join(raw_path, subpath)


def data_neo4j(subpath: str):
	global neo4j_path
	print(neo4j_path)
	print(os.path.join(neo4j_path, subpath))
	return os.path.join(neo4j_path, subpath)


def years_to_files(subdir: str):
	all_files = glob.glob(data_neo4j(subdir) + "/*.csv")
	#return [f for f in all_files if f[-8:-4] in years_to_annotate]
	return [f for f in all_files]


def aggregate_disease_data():
	# Rename GARD-Project mapping results columns to match the names listed in the GARD data
	normmap_df = pd.read_csv(data_raw('normmap_results.csv'),index_col=False,usecols=['ID','GARD_id','CONF_SCORE','SEM_SIM'])
	# Rename GARD-Project mapping results columns to match the names listed in the GARD data
	normmap_df = pd.read_csv(data_raw('normmap_results.csv'),index_col=False,usecols=['ID','GARD_id','CONF_SCORE','SEM_SIM'])
	normmap_df = normmap_df.rename(columns={'ID':'APPLICATION_ID', 'GARD_id': 'GARD_ID'})

	disease_df = pd.read_json(data_raw('all_gards.json'))
	disease_df = disease_df.rename(columns={'GARD id':'GARD_ID', 'Name':'NAME', 'Synonyms':'SYNONYMS'})

	# Merge the GARD-Project mapping results with the GARD data
	merged_df = pd.merge(normmap_df, disease_df, on=['GARD_ID'])
	merged_df.to_csv(data_neo4j('disease/disease_to_application.csv'),index=False)

def combine_projects():
	combine_df = pd.DataFrame()

        # Appends each abstract CSV to the last CSV for all abstracts CSVs in folder
	for filename in os.listdir(data_raw('abstracts')):
		tmp = pd.read_csv(data_raw('abstracts/{filename}'.format(filename=filename)),index_col=False, encoding = "ISO-8859-1")
		combine_df = pd.concat([combine_df,tmp], axis=0)

	combine_df.to_csv(raw_path + '/RePORTER_PRJABS_C_FY_ALL.csv')

def run_normmap():
	print('Running NormMap')

	# Combine all project files into a single file
	combine_projects() # TEST remove #

	# Create CSV files headers
	with open(data_raw('normmap_results.csv'), "w") as f:
		f.writelines(['ID,GARD_id,CONF_SCORE,SEM_SIM\n'])

	chunk_size = 100
	thread_list = list()

	df = pd.read_csv(raw_path + '/RePORTER_PRJABS_C_FY_ALL.csv', index_col=False)
	df = df[0:1000]
	list_df = [df[i:i+chunk_size] for i in range(0,len(df),chunk_size)]

	# Create threads to process results
	for lst in list_df:
		thread = threading.Thread(target=batch_normmap, args=(lst,), daemon=True)
		thread_list.append(thread)

	for thr in thread_list:
		thr.start()

	for thr in thread_list:
		thr.join()

	print('GARD to Project connections made')

def get_RD_project_ids():
        # Get GARD to Project mappings
	run_normmap()
	aggregate_disease_data()
	apps = pd.read_csv(data_raw("normmap_results.csv"), usecols=["ID"])

	# Drop duplicate results and sort by Application ID
	apps = apps.drop_duplicates()
	apps = apps.sort_values(by=["ID"])

	apps.to_csv(data_neo4j("NormMap_mapped_app_ids.csv"), index=None)


def merge_project_funding():
	input_file_path = data_raw("projects/")
	output_file_path = data_neo4j("projects_with_funds/")

	# Processes for pre-2000 files
	for year in range(1985, 2000, 1):
		# Loads and sorts each Project Funding file by Application ID
		funding_file_name = input_file_path + "RePORTER_PRJFUNDING_C_FY" + str(year) + ".csv"
		funding = pd.read_csv(funding_file_name, encoding=ENCODING)
		funding.columns = ['APPLICATION_ID','FULL_PROJECT_NUM','FUNDING_ICs','FY','ORG_DUNS','SUBPROJECT_ID','TOTAL_COST','TOTAL_COST_SUB_PROJECT']
		funding.sort_values('APPLICATION_ID', inplace=True)

		# Loads and sorts each Project file by Application ID
		project_file_name = input_file_path + "RePORTER_PRJ_C_FY" + str(year) + ".csv"
		project = pd.read_csv(project_file_name, encoding=ENCODING, low_memory=False)
		project.sort_values('APPLICATION_ID', inplace=True)

		match_col = 'APPLICATION_ID'
		replace_cols = ['FULL_PROJECT_NUM','FUNDING_ICs','FY','ORG_DUNS','SUBPROJECT_ID','TOTAL_COST','TOTAL_COST_SUB_PROJECT']

		# Searches each application ID in Project file for the same ID in the Project Funding file and merges the respective result from Funding file into the Project file
		# Before 2000 the Project funding data was not included in the Project CSV file, so this is retrieving the up to date funding info
		project.loc[project[match_col].isin(funding[match_col]), replace_cols] = funding.loc[funding[match_col].isin(project[match_col]), replace_cols].values

		output_file_name = output_file_path + "RePORTER_PRJ_C_FY" + str(year) + "_cleaned.csv"
		project.to_csv(output_file_name, index=False, encoding=ENCODING)

		print("Finished", year)

	print("Copying over post-1999 project files")
	all_files = glob.glob(input_file_path + "*.csv")
	p = re.compile("([0-9]{4})[^0-9]*$")

	# Nothing is changed in the post-2000 files, therefore are just copied over into the "processed" folder
	for f in all_files:
		m = p.search(f)
		if m is not None and int(m.group(1)) > 1999:
			shutil.copy(f, output_file_path + "RePORTER_PRJ_C_FY" + m.group(1) + "_cleaned.csv")
			print("Finished", m.group(1))


def select_RD_projects():
	def find_RD_apps(input_file, rd_ids):
		'''
		Extract the applications that are rare disease related
		Parameters:
		input_file: path and filename of the file
		rd_ids: a list of rare disease related application IDs
		'''
		apps = pd.read_csv(input_file, encoding=ENCODING, low_memory=False)

		# Searches the Project CSVs for Projects listed in the GARD-Project mapping results
		rd_related = apps['APPLICATION_ID'].isin(rd_ids)
		apps = apps[rd_related]
		apps.sort_values(by=['APPLICATION_ID'], inplace=True)

		return apps

	# Read the list of RD Application IDs
	rd_ids = pd.read_csv(data_neo4j('NormMap_mapped_app_ids.csv'))
	rd_ids = rd_ids['ID'].tolist()

	# Get CSV files lists from a folder
	input_path = data_neo4j('projects_with_funds/')
	files = glob.glob(input_path + '*.csv')

	for file in files:
		output_file = data_neo4j('projects/RD_PROJECTS_' + file[-16:-12] + '.csv')

		apps = find_RD_apps(file, rd_ids)
		apps.to_csv(output_file, index=False, encoding=ENCODING)
		print('Finished ', output_file)

def clean_pi (pi_info):
	pi_info = pi_info[:len(pi_info)-1]
	return pi_info

def cleanup_project_IC_NAME_totalcost():
	# Get CSV files lists from a folder
	input_path = data_neo4j('projects/')
	files = glob.glob(input_path + '*.csv')
	cols_to_read = ['APPLICATION_ID' , 'APPLICATION_TYPE', 'CORE_PROJECT_NUM', 'FY', 'IC_NAME',
									'ORG_NAME', 'ORG_STATE', 'PHR', 'PI_IDS', 'PI_NAMEs',
									'PROJECT_TERMS', 'PROJECT_TITLE', 'SUBPROJECT_ID', 'TOTAL_COST', 'TOTAL_COST_SUB_PROJECT']

	# Build Agent names lookup dictionary
	agents = pd.read_csv(data_raw('agent_names.csv'))
	# Creates a dictionary in the formation {'IC_NAME_OLD': 'IC_NAME_NEW'}
	agent_lkup = dict(zip(agents['IC_NAME_OLD'], agents['IC_NAME_NEW']))

	# Clean all files
	output_path = data_neo4j('projects/')

	for file in files:
		# Clean Agent names by filling NULL values with string 'Unknown'
		app = pd.read_csv(file, usecols=cols_to_read, encoding=ENCODING, low_memory=False)
		app['IC_NAME'] = app['IC_NAME'].fillna('Unknown')
		# Replaces the IC_NAME with the new one from the dictionary
		app['IC_NAME'] = app['IC_NAME'].map(agent_lkup)

		# Clean PI_IDS and PI_NAMES
		# Results are listed as a string seperated by semi-colons, this removes the last semi-colon in the string because it causes issues when converting to a list
		app['PI_IDS'] = app['PI_IDS'].apply(clean_pi)
		app['PI_NAMEs'] = app['PI_NAMEs'].apply(clean_pi)

		# Finds NULL TOTAL_COST values and replaces them with the value in field TOTAL_COST_SUB_PROJECT
		app.loc[app['TOTAL_COST'].isnull(), 'TOTAL_COST'] = app['TOTAL_COST_SUB_PROJECT']
		app.drop(columns=['TOTAL_COST_SUB_PROJECT'], inplace=True)

		output_file = output_path + "RD_PROJECTS_" + file[-8:-4] + '.csv'
		app.to_csv(output_file, index=False)
		print('Finished', output_file)


def find_RD_core_projects():
	# Load GARD-Project mapping results
	apps = pd.read_csv(data_neo4j('NormMap_mapped_app_ids.csv'))

	match_col = 'ID'
	new_col = 'CORE_PROJECT_NUM'

	input_path = data_neo4j('projects/')
	files = glob.glob(input_path + '*.csv')

	for file in files:
		# For each project CSV, gets the application ID and Core Project Number
		proj = pd.read_csv(file, usecols=['APPLICATION_ID', 'CORE_PROJECT_NUM'], encoding=ENCODING)
		proj.sort_values('APPLICATION_ID', inplace=True)
		# Searches rows in the GARD-Project mapping for Application IDs that are in the Project CSV and then replace the application ID with the projects Core Project number
		apps.loc[apps[match_col].isin(proj['APPLICATION_ID']), new_col] = proj.loc[proj['APPLICATION_ID'].isin(apps[match_col]), new_col].values

		# Export RD related APPLICATION_ID and CORE_PROJECT_NUM pairs
		apps.to_csv(data_neo4j("RD_appID_coreProjNum.csv"), index=False)


		# Export unique CORE_PROJECT_NUM
		core_proj_num = apps['CORE_PROJECT_NUM'].unique()
		core_proj_num_df = pd.DataFrame(core_proj_num)
		core_proj_num_df.columns = ['CORE_PROJECT_NUM']
		core_proj_num_df.sort_values('CORE_PROJECT_NUM', inplace=True)
		core_proj_num_df.to_csv(data_neo4j("RD_coreProjNum.csv"), index=False)


def select_RD_patents():
	def find_RD_core_project(input_file, col_name_to_replace, core_proj_nums):
		'''
		Extract the applications that are rare disease related
		Parameters:
		input_file: path and filename of the file
		core_proj_nums: a list of rare disease related core project numbers
		'''

		proj = pd.read_csv(input_file, encoding=ENCODING, low_memory=False)
		headers = proj.columns
		# Replaces the PROJECT_ID listed with the patents Core Project number
		proj.columns = list(map(lambda x: x.replace(col_name_to_replace, 'CORE_PROJECT_NUM'), headers))

		# Get RD-related applications
		rd_related = proj['CORE_PROJECT_NUM'].isin(core_proj_nums)

		proj = proj[rd_related]
		proj.sort_values(by=['CORE_PROJECT_NUM'], inplace=True)

		# Returns back patents that are only rare disease related
		return proj

	# Read the list of RD related core project numbers
	core_proj_nums = pd.read_csv(data_neo4j('RD_coreProjNum.csv'))
	core_proj_nums = core_proj_nums['CORE_PROJECT_NUM'].tolist()

	file = data_raw('patents/patents.csv')
	proj = find_RD_core_project(file, 'PROJECT_ID', core_proj_nums)
	output_file = data_neo4j('patents/RD_PATENTS.csv')

	proj.to_csv(output_file, index=False, encoding=ENCODING)
	print('Finished!')


def select_RD_clinical_studies():
	def find_RD_core_project(input_file, col_name_to_replace, core_proj_nums):
		'''
		Extract the applications that are rare disease related
		Parameters:
		input_file: path and filename of the file
		core_proj_nums: a list of rare disease related core project numbers
		'''

		proj = pd.read_csv(input_file, encoding=ENCODING, low_memory=False)
		headers = proj.columns
		# Replaces the Core Project Number field with CORE_PROJECT_NUM
		proj.columns = list(map(lambda x: x.replace(col_name_to_replace, 'CORE_PROJECT_NUM'), headers))

		# Get RD-related applications
		rd_related = proj['CORE_PROJECT_NUM'].isin(core_proj_nums)
		proj = proj[rd_related]
		proj.sort_values(by=['CORE_PROJECT_NUM'], inplace=True)

		# Returns back only rare disease related clinical studies
		return proj

	# Read the list of RD related core project numbers
	core_proj_nums = pd.read_csv(data_neo4j('RD_coreProjNum.csv'))
	core_proj_nums = core_proj_nums['CORE_PROJECT_NUM'].tolist()

	file = data_raw('clinical_studies/clinical_studies.csv')
	proj = find_RD_core_project(file, 'Core Project Number', core_proj_nums)
	output_file = data_neo4j('clinical_studies/RD_CLINICAL_STUDIES.csv')

	proj.to_csv(output_file, index=False, encoding=ENCODING)
	print('Finished!')


def select_RD_link_tables():
	def find_RD_core_project(input_file, col_name_to_replace, core_proj_nums):
		'''
		Extract the applications that are rare disease related
		Parameters:
		input_file: path and filename of the file
		core_proj_nums: a list of rare disease related core project numbers
		'''

		proj = pd.read_csv(input_file, encoding=ENCODING, low_memory=False)
		headers = proj.columns
		# Replaces the PROJECT_NUMBER column with CORE_PROJECT_NUM
		proj.columns = list(map(lambda x: x.replace(col_name_to_replace, 'CORE_PROJECT_NUM'), headers))

		# Get RD-related applications
		rd_related = proj['CORE_PROJECT_NUM'].isin(core_proj_nums)
		proj = proj[rd_related]
		proj.sort_values(by=['CORE_PROJECT_NUM'], inplace=True)

		# Returns only rare disease related link table rows
		return proj

	# Read the list of RD related core project numbers
	core_proj_nums = pd.read_csv(data_neo4j('RD_coreProjNum.csv'))
	core_proj_nums = core_proj_nums['CORE_PROJECT_NUM'].tolist()

	# Gets all link tables in the folder
	input_path = data_raw('link_tables/')
	files = glob.glob(input_path + '*.csv')

	for file in files:
		proj = find_RD_core_project(file, 'PROJECT_NUMBER', core_proj_nums)

		output_file = data_neo4j('link_tables/RD_LINK_TABLE_' + file[-8:-4] + '.csv')
		proj.to_csv(output_file, index=False, encoding=ENCODING)
		print('Finished ', output_file)


def select_RD_publications():
	pub_path = data_raw('publications/')
	pub_files = glob.glob(pub_path + '*.csv')

	lnk_path = data_neo4j('link_tables/')
	lnk_files = glob.glob(lnk_path + '*.csv')

	for pub_file in pub_files:
		pub = pd.read_csv(pub_file, encoding=ENCODING)
		mask = [False for i in range(pub.shape[0])]

		for lnk_file in lnk_files:
			lnk = pd.read_csv(lnk_file)
			# Get unique PMIDs
			pmid_lst = lnk['PMID'].unique()

			# Search the Publication CSV for PMIDs that are in the respective link table
			is_rd = pub['PMID'].isin(pmid_lst).tolist()
			# Labels publications as either rare disease related or not
			mask = [mask or is_rd for mask, is_rd in zip(mask, is_rd)]

		output_file = data_neo4j('publications/RD_PUB_' + pub_file[-8:-4] + '.csv')
		# Gets only rows of publications that are rare disease realted
		pub = pub[mask]
		pub.to_csv(output_file, index=False)
		print("Finished ", output_file)


def cleanup_pub_country():
	# Get CSV files lists from a folder
	input_path = data_neo4j('publications/')
	files = glob.glob(input_path + '*.csv')

	# Build country lookup dictionary
	countries = pd.read_csv(data_raw('countries.csv'))
	# Builds dictionary in format {'COUNTRY_OLD': 'COUNTRY_NEW'}
	country_lkup = dict(zip(countries['COUNTRY_OLD'], countries['COUNTRY_NEW']))

	# Clean all publication files
	output_path = data_neo4j('publications/')
	for file in files:
		pub = pd.read_csv(file, encoding=ENCODING, low_memory=False)
		# Fills null Country values with 'Unknown'
		pub['COUNTRY'] = pub['COUNTRY'].fillna('Unknown')
		# Replaces countries found in publication CSV with new country value
		pub['COUNTRY'] = pub['COUNTRY'].map(country_lkup)

		output_file = output_path + "RD_PUB_" + file[-8:-4] + '.csv'
		pub.to_csv(output_file, index=False)
		print('Finished', output_file)


def fix_escaped_endings():
	def tf(val):
		if type(val) == str and val[-1] == '\\':
			return val[:-1]
		else:
			return val

	files = glob.glob(data_neo4j("*/*.csv"))
	for file in files:
		print(file)
		df = pd.read_csv(file, low_memory=False, dtype=str, encoding=ENCODING)
		df = df.applymap(tf)
		df.to_csv(file, index=False, encoding="utf-8")
		print("Finished", file)


def select_RD_abstracts():
	def find_RD_apps(input_file, rd_ids):
		'''
		Extract the applications that are rare disease related
		Parameters:
		input_file: path and filename of the file
		rd_ids: a list of rare disease related application IDs
		'''
		
		apps = pd.read_csv(input_file, encoding=ENCODING, low_memory=False)

		# Finds the Abstracts CSV application IDs that are also found in the GARD-Project mapping results
		rd_related = apps['APPLICATION_ID'].isin(rd_ids)
		apps = apps[rd_related]
		apps.sort_values(by=['APPLICATION_ID'], inplace=True)

		# Returns sorted list of rare disease related abstract rows
		return apps

	# Read the list of RD Application IDs
	rd_ids = pd.read_csv(data_neo4j('NormMap_mapped_app_ids.csv'))
	rd_ids = rd_ids['ID'].tolist()

	# Get abstracts CSV files lists from a folder
	input_path = data_raw('abstracts/')
	files = glob.glob(input_path + '*.csv')

	for file in files:
		# This accounts for updated information with "new.csv" appended to them
		if file.endswith("new.csv"):
			output_file = data_neo4j('abstracts/RD_ABSTRACTS_' + file[-12:-8] + '.csv')
		else:
			output_file = data_neo4j('abstracts/RD_ABSTRACTS_' + file[-8:-4] + '.csv')

		apps = find_RD_apps(file, rd_ids)
		apps.to_csv(output_file, index=False, encoding=ENCODING)
		print('Finished ', output_file)


def annotation_preprocess_grant():
	# Get CSV files lists from projects and abstracts folders
	projects_files = sorted(years_to_files("projects"))
	abstracts_files = sorted(years_to_files("abstracts"))

	for projects_file, abstracts_file in zip(projects_files, abstracts_files):
		# print(projects_file)
		# print(abstracts_file)
		if projects_file == None or abstracts_file == None:
			continue

		# Preprocesses information related to PHR and AIM (May not be needed due to Jaber's code)
		annotate_text = prepare_phr_aim(projects_file, abstracts_file)

		year = projects_file[-8:-4]

		output_file = data_neo4j("annotation_files/annotation_text_" + year + ".csv")
		annotate_text.to_csv(output_file, index=False, encoding=ENCODING)

		print("Finished", output_file)
		print("................. ALL DONE! .................")

def annotate_grant_abstracts():
	MODELS = ['en_ner_craft_md', 'en_ner_jnlpba_md', 'en_ner_bc5cdr_md', 'en_ner_bionlp13cg_md']


	# Get CSV files lists from projects and abstracts folders
	input_files = years_to_files("annotation_files/")

	output_file_path = data_neo4j("grants_umls/grants_umls_")


	# Annotate text with four scispaCy models
	for model in MODELS:
		print(f'*** Annotate with {model} model ***')

		nlp = load_model(model)
		for file in input_files:
			year = file[-8:-4]
			print('///////')
			
			print('///////')
			
			try:
				text = pd.read_csv(file, encoding=ENCODING, dtype={'APPLICATION_ID':int, 'ABSTRACT_TEXT':str})

				if len(text) == 0:
					continue

				umls = get_umls_concepts(nlp, text)

				output_file = output_file_path + year + ".csv"

				if model == 'en_ner_craft_md':
					umls.to_csv(output_file, index=False)
				else:
					umls.to_csv(output_file, index=False, mode='a', header=False)

				print("Added annotations to", output_file)

			except Exception as e:
				print(e)
				continue

	print("***** ALL DONE *****")


def clean_umls_concepts():
	# Get CSV files lists from a folder
	files = years_to_files(data_neo4j("grants_umls/"))

	# Clean all files
	output_path = data_neo4j(data_neo4j('grants_umls/'))
	keep_semantic_types = pd.read_csv(data_raw('semantic_type_keep.csv'), usecols=['TUI'])
	keep_semantic_types = keep_semantic_types['TUI'].to_list()

	remove_umls_concepts = pd.read_csv(data_raw('umls_concepts_remove.csv'), usecols=['UMLS_CUI'])
	remove_umls_concepts = remove_umls_concepts['UMLS_CUI'].to_list()

	for file in files:
		# Removes unwanted semantic types
		umls = clean_annotation_output(file, keep_semantic_types, remove_umls_concepts)
		output_file = output_path + "RD_UMLS_CONCEPTS_" + file[-8:-4] + '.csv'
		umls.to_csv(output_file, index=False)


def clean_annotation_source():
	# Get CSV files lists from a folder
	files = years_to_files("annotation_files/")
	cols_to_read = ['APPLICATION_ID' , 'SOURCE']

	# Clean all files
	output_path = data_neo4j('annotation_source/')

	for file in files:
		app = pd.read_csv(file, usecols=cols_to_read, encoding=ENCODING, )
		output_file = output_path + "RD_ANNOTATE_SRC_" + file[-8:-4] + '.csv'
		app.to_csv(output_file, index=False)
		print('Finished', output_file)


def map_semantic_types():
	names = pd.read_csv(data_raw("SemanticTypes_2018AB.csv"), delimiter='|', header=None)
	names = {k: v for [k, v] in names.values}
	input_file_path = data_neo4j("grants_umls/")
	output_file_path = data_neo4j("annotation_umls/")
	all_files = glob.glob(input_file_path + "RD_UMLS_CONCEPTS_*.csv")
	for file in all_files:
		data = pd.read_csv(file, encoding=ENCODING)
		semantic_types_names = []
		for row in data["SEMANTIC_TYPES"]:
			row = ast.literal_eval(row)
			row = list(map(lambda x: names[x], row))
			semantic_types_names.append(row)
		data["SEMANTIC_TYPES_NAMES"] = semantic_types_names
		data.to_csv(output_file_path + "RD_ANNOTATION_" + file[-8:-4] + ".csv", index=False)
		print("Finished", file)


def to_abs_path(path: str):
	return os.path.abspath(os.path.expanduser(os.path.expandvars(path)))

def get_disease_data():
        # Gather GARD information from RDAS GARD neo4j
	db = AlertCypher(f'{sysvars.gard_db}')
	query = 'MATCH (x:GARD) RETURN COLLECT(x)'

	response = db.run(query).data()[0]['COLLECT(x)']
	
	json_list = list()
	for res in response:
		json_dict = dict()
		json_dict['GARD id'] = res['GardId']
		json_dict['Name'] = res['GardName']
		json_dict['Synonyms'] = res['Synonyms']
		json_list.append(json_dict)
        
        # Convert dictionary to JSON and save file
	json_df = pd.DataFrame.from_records(json_list)
	json_df.to_json(data_raw('all_gards.json'))

	with open(data_raw('all_gards.json'), 'w+') as f:
		json.dump(json_list, f)

def prep_data(data_raw_path: str, data_neo4j_path: str) -> FilesToAdd:
	"""
	The most important function of this module. Given the path to raw ExPORTER data,
	runs the preprocessing stages on the data (situationally skipping some slower
	stages on files that have not changed) and determines which output files are
	new/modified and thus contain data that would need to be inserted into the
	database.
	@param data_raw_path: the path, relative or absolute, to the folder containing
	                      raw CSV data from ExPORTER
	@param data_neo4j_path: the path, relative or absolute, to the folder containing
	                        (or that will contain) processed and annotated CSV data.
	"""

	# Initializing some variables
	global raw_path, neo4j_path, years_to_annotate
	raw_path = to_abs_path(data_raw_path)
	neo4j_path = to_abs_path(data_neo4j_path)

	

	# Initializes folders to be created that store processed CSV files
	print("Reading raw data from", raw_path, "and outputting processed data to", neo4j_path)
	folders_to_create = [
			"abstracts",
			"disease",
			"annotation_files",
			"annotation_source",
			"annotation_umls",
			"clinical_studies",
			"grants_umls",
			"link_tables",
			"patents",
			"projects",
			"projects_with_funds",
			"publications"
	]

	

	# Clear out old files, or initialize new repo for them if the given data_neo4j path does
	# not already exist.
	repo = None
	is_new_repo = False
	'''
	try:
		files = os.listdir(neo4j_path)
		files.remove(".git")
		print("Found existing git repo at neo4j_path, clearing out old files")
		repo = pygit2.Repository(data_neo4j(".git"))
		is_new_repo = False
		for f in files:
			# skip annotation-related files because we may not want to reannotate some files
			if f in ["annotation_files", "annotation_source", "annotation_umls", "grants_umls"]:
				folders_to_create.remove(f)
				continue
			fpath = data_neo4j(f)
			shutil.rmtree(fpath) if os.path.isdir(fpath) else os.remove(fpath)
	except (NotADirectoryError, FileNotFoundError, ValueError):
		print("Not existing repo at given data_neo4j path, initializing")
		repo = pygit2.init_repository(neo4j_path)
		is_new_repo = True

	print("Creating empty output directories")
	'''

	
	# add empty folders for the generated files
	add_folder = lambda folder_name: os.mkdir(data_neo4j(folder_name))
	for folder in folders_to_create:
		if os.path.exists(data_neo4j(folder)):
			continue
		add_folder(folder)

	##############################################
	# Run preprocessing stages one after another.#
	##############################################
	
	print('Running get_disease_data')
	get_disease_data()
	print("Running get_RD_project_ids")
	get_RD_project_ids()
	print("Running merge_project_funding")
	merge_project_funding()
	print("Running select_RD_projects")
	select_RD_projects()
	
	print("Running cleanup_project_IC_NAME_totalcost")
	cleanup_project_IC_NAME_totalcost()
	
	print("Running find_RD_core_projects")
	find_RD_core_projects()
	print("Running select_RD_patents")
	select_RD_patents()
	print("Running select_RD_clinical_studies")
	select_RD_clinical_studies()
	print("Running select_RD_link_tables")
	select_RD_link_tables()
	print("Running select_RD_publications")
	select_RD_publications()
	print("Running cleanup_pub_country")
	cleanup_pub_country()
	print("Running select_RD_abstracts")
	select_RD_abstracts()
	
	# The below stages are extremely slow, so we will only run them for
	# years that have changed data.
	
	'''
	years_to_annotate = {k[-8:-4] for k,v in repo.status().items()
											 if (k.startswith("abstracts/") or k.startswith("projects/"))
											 and v in [pygit2.GIT_STATUS_WT_MODIFIED, pygit2.GIT_STATUS_WT_NEW]}
	'''	
	
	print("Running annotation_preprocess_grant")
	annotation_preprocess_grant()
	print("Running annotate_grant_abstracts")
	annotate_grant_abstracts()
	print("Running clean_umls_concepts")
	clean_umls_concepts()
	print("Running clean_annotation_source")
	clean_annotation_source()
	print("Running map_semantic_types")
	map_semantic_types()
	
	print("Running fix_escaped_endings")
	fix_escaped_endings()
	
	
	################################################

	# Finished running stages, now we figure out which files have changed
	# (and store them in a FilesToAdd object to be returned) and commit the changes
	'''
	print("Getting current repo status")
	status = repo.status().items()
	fta = {}
	for subdir in FilesToAdd.__required_keys__:
		fta[subdir] = [data_neo4j(k) for k,v in status
									 if k.startswith(subdir)
									 and v in [pygit2.GIT_STATUS_WT_MODIFIED, pygit2.GIT_STATUS_WT_NEW]]

	print("adding all changes to commit")
	if is_new_repo:
		ref = "HEAD"
		parents = []
	else:
		ref = repo.head.name
		parents = [repo.head.target]
	index = repo.index
	index.add_all()
	index.write()
	print("Committing")
	author = pygit2.Signature('script authored', 'no@email.given')
	committer = pygit2.Signature('script committed', 'no@email.given')
	message = "regular update commit"
	tree = index.write_tree()
	repo.create_commit(ref, author, committer, message, tree, parents)
	'''

	# Gets the names of every processed file added for the rest of the code to add to the neo4j
	fta = {}
	for subdir in FilesToAdd.__dict__['__annotations__'].keys():
		fta[subdir] = ['file:///' + data_neo4j(subdir)+'/'+d for d in os.listdir(data_neo4j(subdir))] #'file:/'
		print(fta)
	return fta


# For testing purposes; this file is typically not run directly, but instead called
# by update_grant.py with an appropriate raw and output path
if __name__ == "__main__":
	prep_data("~/testneo4jprep/data_raw", "~/testneo4jprep/data_neo4j")

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
import re
import ast
import shutil
import pygit2
import glob
from typing import TypedDict
import pandas as pd
from prepare_annotation_text import prepare_phr_aim
from annotate_text import *
from remove_general_umls_concepts import clean_annotation_output

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
	patents: list
	projects: list
	link_tables: list


def data_raw(subpath: str):
	global raw_path
	return os.path.join(raw_path, subpath)


def data_neo4j(subpath: str):
	global neo4j_path
	return os.path.join(neo4j_path, subpath)


def years_to_files(subdir: str):
	all_files = glob.glob(data_neo4j(subdir) + "*.csv")
	return [f for f in all_files if f[-8:-4] in years_to_annotate]


def get_RD_project_ids():
	apps = pd.read_csv(data_raw("abstract_matches_2mil_ALL.csv"), usecols=["APPLICATION_ID"])
	apps = apps.drop_duplicates()
	apps = apps.sort_values(by=["APPLICATION_ID"])

	apps.to_csv(data_neo4j("NormMap_mapped_app_ids.csv"), index=None)


def merge_project_funding():
	input_file_path = data_raw("projects/")
	output_file_path = data_neo4j("projects_with_funds/")

	for year in range(1985, 2000, 1):
		funding_file_name = input_file_path + "RePORTER_PRJFUNDING_C_FY" + str(year) + ".csv"
		funding = pd.read_csv(funding_file_name, encoding=ENCODING)
		funding.columns = ['APPLICATION_ID','FULL_PROJECT_NUM','FUNDING_ICs','FY','ORG_DUNS','SUBPROJECT_ID','TOTAL_COST','TOTAL_COST_SUB_PROJECT']
		funding.sort_values('APPLICATION_ID', inplace=True)

		project_file_name = input_file_path + "RePORTER_PRJ_C_FY" + str(year) + ".csv"
		project = pd.read_csv(project_file_name, encoding=ENCODING, low_memory=False)
		project.sort_values('APPLICATION_ID', inplace=True)

		match_col = 'APPLICATION_ID'
		replace_cols = ['FULL_PROJECT_NUM','FUNDING_ICs','FY','ORG_DUNS','SUBPROJECT_ID','TOTAL_COST','TOTAL_COST_SUB_PROJECT']

		project.loc[project[match_col].isin(funding[match_col]), replace_cols] = funding.loc[funding[match_col].isin(project[match_col]), replace_cols].values

		output_file_name = output_file_path + "RePORTER_PRJ_C_FY" + str(year) + "_cleaned.csv"
		project.to_csv(output_file_name, index=False, encoding=ENCODING)

		print("Finished", year)

	print("Copying over post-1999 project files")
	all_files = glob.glob(input_file_path + "*.csv")
	p = re.compile("([0-9]{4})[^0-9]*$")
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

		# Get RD-related applications
		rd_related = apps['APPLICATION_ID'].isin(rd_ids)
		apps = apps[rd_related]
		apps.sort_values(by=['APPLICATION_ID'], inplace=True)

		return apps

	# Read the list of RD Application IDs
	rd_ids = pd.read_csv(data_neo4j('NormMap_mapped_app_ids.csv'))
	rd_ids = rd_ids['APPLICATION_ID'].tolist()

	# Get CSV files lists from a folder
	input_path = data_neo4j('projects_with_funds/')
	files = glob.glob(input_path + '*.csv')

	for file in files:
		output_file = data_neo4j('projects/RD_PROJECTS_' + file[-16:-12] + '.csv')

		apps = find_RD_apps(file, rd_ids)
		apps.to_csv(output_file, index=False, encoding=ENCODING)
		print('Finished ', output_file)


def cleanup_project_IC_NAME_totalcost():
	# Get CSV files lists from a folder
	input_path = data_neo4j('projects/')
	files = glob.glob(input_path + '*.csv')
	cols_to_read = ['APPLICATION_ID' , 'APPLICATION_TYPE', 'CORE_PROJECT_NUM', 'FY', 'IC_NAME',
									'ORG_NAME', 'ORG_STATE', 'PHR', 'PI_IDS', 'PI_NAMEs',
									'PROJECT_TERMS', 'PROJECT_TITLE', 'SUBPROJECT_ID', 'TOTAL_COST', 'TOTAL_COST_SUB_PROJECT']

	# Build Agent names lookup dictionary
	agents = pd.read_csv('new/grant/agent_names.csv')
	agent_lkup = dict(zip(agents['IC_NAME_OLD'], agents['IC_NAME_NEW']))

	# Clean all files
	output_path = data_neo4j('projects/')

	for file in files:
		# Clean Agent names
		app = pd.read_csv(file, usecols=cols_to_read, encoding=ENCODING, low_memory=False)
		app['IC_NAME'] = app['IC_NAME'].fillna('Unknown')
		app['IC_NAME'] = app['IC_NAME'].map(agent_lkup)

		# Combine TOTAL_COST and TOTAL_COST_SUB_PROJECT
		app.loc[app['TOTAL_COST'].isnull(), 'TOTAL_COST'] = app['TOTAL_COST_SUB_PROJECT']
		app.drop(columns=['TOTAL_COST_SUB_PROJECT'], inplace=True)

		output_file = output_path + "RD_PROJECTS_" + file[-8:-4] + '.csv'
		app.to_csv(output_file, index=False)
		print('Finished', output_file)


def find_RD_core_projects():
	apps = pd.read_csv(data_neo4j('NormMap_mapped_app_ids.csv'))

	match_col = 'APPLICATION_ID'
	new_col = 'CORE_PROJECT_NUM'

	input_path = data_neo4j('projects/')
	files = glob.glob(input_path + '*.csv')

	for file in files:
		proj = pd.read_csv(file, usecols=['APPLICATION_ID', 'CORE_PROJECT_NUM'], encoding=ENCODING)
		proj.sort_values('APPLICATION_ID', inplace=True)
		apps.loc[apps[match_col].isin(proj[match_col]), new_col] = proj.loc[proj[match_col].isin(apps[match_col]), new_col].values

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
		proj.columns = list(map(lambda x: x.replace(col_name_to_replace, 'CORE_PROJECT_NUM'), headers))

		# Get RD-related applications
		rd_related = proj['CORE_PROJECT_NUM'].isin(core_proj_nums)

		proj = proj[rd_related]
		proj.sort_values(by=['CORE_PROJECT_NUM'], inplace=True)

		return proj

	# Read the list of RD related core project numbers
	core_proj_nums = pd.read_csv(data_neo4j('RD_coreProjNum.csv'))
	core_proj_nums = core_proj_nums['CORE_PROJECT_NUM'].tolist()

	file = data_raw('patents/Patents_1659288919587.csv')
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
		proj.columns = list(map(lambda x: x.replace(col_name_to_replace, 'CORE_PROJECT_NUM'), headers))

		# Get RD-related applications
		rd_related = proj['CORE_PROJECT_NUM'].isin(core_proj_nums)
		proj = proj[rd_related]
		proj.sort_values(by=['CORE_PROJECT_NUM'], inplace=True)

		return proj

	# Read the list of RD related core project numbers
	core_proj_nums = pd.read_csv(data_neo4j('RD_coreProjNum.csv'))
	core_proj_nums = core_proj_nums['CORE_PROJECT_NUM'].tolist()

	file = data_raw('clinical_studies/ClinicalStudies_1659286507775.csv')
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
		proj.columns = list(map(lambda x: x.replace(col_name_to_replace, 'CORE_PROJECT_NUM'), headers))

		# Get RD-related applications
		rd_related = proj['CORE_PROJECT_NUM'].isin(core_proj_nums)
		proj = proj[rd_related]
		proj.sort_values(by=['CORE_PROJECT_NUM'], inplace=True)

		return proj

	# Read the list of RD related core project numbers
	core_proj_nums = pd.read_csv(data_neo4j('RD_coreProjNum.csv'))
	core_proj_nums = core_proj_nums['CORE_PROJECT_NUM'].tolist()

	##### For files in a folder #####
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
			pmid_lst = lnk['PMID'].unique()

			is_rd = pub['PMID'].isin(pmid_lst).tolist()
			mask = [mask or is_rd for mask, is_rd in zip(mask, is_rd)]

		output_file = data_neo4j('publications/RD_PUB_' + pub_file[-8:-4] + '.csv')
		pub = pub[mask]
		pub.to_csv(output_file, index=False)
		print("Finished ", output_file)


def cleanup_pub_country():
	# Get CSV files lists from a folder
	input_path = data_neo4j('publications/')
	files = glob.glob(input_path + '*.csv')

	# Build country lookup dictionary
	countries = pd.read_csv('new/grant/countries.csv')
	country_lkup = dict(zip(countries['COUNTRY_OLD'], countries['COUNTRY_NEW']))

	# Clean all files
	output_path = data_neo4j('publications/')
	for file in files:
		pub = pd.read_csv(file, encoding=ENCODING, low_memory=False)
		pub['COUNTRY'] = pub['COUNTRY'].fillna('Unknown')
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

		# Get RD-related applications
		rd_related = apps['APPLICATION_ID'].isin(rd_ids)
		apps = apps[rd_related]
		apps.sort_values(by=['APPLICATION_ID'], inplace=True)

		return apps

	# Read the list of RD Application IDs
	rd_ids = pd.read_csv(data_neo4j('NormMap_mapped_app_ids.csv'))
	rd_ids = rd_ids['APPLICATION_ID'].tolist()

	# Get CSV files lists from a folder
	input_path = data_raw('abstracts/')
	files = glob.glob(input_path + '*.csv')

	for file in files:
		if file.endswith("new.csv"):
			output_file = data_neo4j('abstracts/RD_ABSTRACTS_' + file[-12:-8] + '.csv')
		else:
			output_file = data_neo4j('abstracts/RD_ABSTRACTS_' + file[-8:-4] + '.csv')

		apps = find_RD_apps(file, rd_ids)
		apps.to_csv(output_file, index=False, encoding=ENCODING)
		print('Finished ', output_file)


def annotation_preprocess_grant():
	# Get CSV files lists from projects and abstracts folders
	projects_files = sorted(years_to_files("projects/"))
	abstracts_files = sorted(years_to_files("abstracts/"))

	for projects_file, abstracts_file in zip(projects_files, abstracts_files):
		annotate_text = prepare_phr_aim(projects_file, abstracts_file)

		year = projects_file[-8:-4]

		output_file = data_neo4j("annotation_files/annotation_text_" + year + ".csv")
		annotate_text.to_csv(output_file, index=False, encoding=ENCODING)

		print("Finished", output_file)

		print("................. ALL DONE! .................")
	pass

def annotate_grant_abstracts():
	MODELS = ['en_ner_craft_md', 'en_ner_jnlpba_md', 'en_ner_bc5cdr_md', 'en_ner_bionlp13cg_md']


	# Get CSV files lists from projects and abstracts folders
	input_files = years_to_files("annotation_files/")

	output_file_path = data_neo4j("grants_umls/grants_umls_")


	# Annotate text with four scispaCy models
	for model in MODELS:
		print(f'*** Annotate with {model} model ***')

		for file in input_files:
			year = file[-8:-4]

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


def clean_umls_concepts():
	# Get CSV files lists from a folder
	files = years_to_files("grants_umls/")

	# Clean all files
	output_path = data_neo4j('grants_umls/')

	keep_semantic_types = pd.read_csv('semantic_type_keep.csv', usecols=['TUI'])
	keep_semantic_types = keep_semantic_types['TUI'].to_list()

	remove_umls_concepts = pd.read_csv('umls_concepts_remove.csv', usecols=['UMLS_CUI'])
	remove_umls_concepts = remove_umls_concepts['UMLS_CUI'].to_list()

	for file in files:
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
	names = pd.read_csv("SemanticTypes_2018AB.csv", delimiter='|', header=None)
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

	print("Reading raw data from", raw_path, "and outputting processed data to", neo4j_path)
	folders_to_create = [
			"abstracts",
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

	# add empty folders for the generated files
	add_folder = lambda folder_name: os.mkdir(data_neo4j(folder_name))
	for folder in folders_to_create:
		add_folder(folder)

	##############################################
	# Run preprocessing stages one after another.#
	##############################################
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
	years_to_annotate = {k[-8:-4] for k,v in repo.status().items()
											 if (k.startswith("abstracts/") or k.startswith("projects/"))
											 and v in [pygit2.GIT_STATUS_WT_MODIFIED, pygit2.GIT_STATUS_WT_NEW]}
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

	return fta


# For testing purposes; this file is typically not run directly, but instead called
# by update_grant.py with an appropriate raw and output path
if __name__ == "__main__":
	prep_data("~/testneo4jprep/data_raw", "~/testneo4jprep/data_neo4j")

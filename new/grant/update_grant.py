import os
import time
import json
from neo4j import GraphDatabase, Session, Record
from typing import TypedDict, Any, Callable, Optional
from steps import steps

class FilesToAdd(TypedDict):
	"""
	FilesToAdd is just a dict with these particular keys (corresponding to the
	names of subdirectories of CSV files) and values that are of type list[str].
	Each element of that list[str] is a path (i.e. "subdir/file.csv")
	"""
	abstracts: list[str]
	annotation_source: list[str]
	annotation_umls: list[str]
	clinical_studies: list[str]
	disease: list[str]
	patents: list[str]
	projects: list[str]
	publications: list[str]
	link_tables: list[str]


def write(session: Session, query: str, params: dict[str, Any]) -> list[Record]:
	"""
	A small convenience function for running write transactions against the
	database.
	:param query: The query to execute
	:param params: The parameters to substitute in the query
	:return: The returned records from the query
	"""
	return session.write_transaction(
		lambda tx: [record for record in tx.run(query, **params)])

def to_list(st: set[str]) -> list[str]:
	"""
	to_list converts a string set to a list, sorted alphabetically (unlike the
	builtin list() function). In particular, this'll sort RD_something_XXXX.csv
	paths in order of increasing year. This sorting might not be necessary? but
	not a big deal to do it anyway
	:param st: the string set to convert to a sorted list
	:return: a list of strings from the set
	"""
	lst = list(st)
	lst.sort()
	return lst


def get_files_to_add(configdata: dict[str, Any]) -> FilesToAdd:
	"""
	Since it is a lot of data, for future updates we don't need to go back and
	re-add all of the data we've already added previously. Instead, this program
	will go back through each directory and compare each file's latest
	modification time to the last time this program was run. Only if the file was
	added/modified after the previous run will we go back in and add all the
	data again.

	It's not a big deal if a file's data is added redundantly, since all the
	Cypher queries are built with MERGE instead of CREATE, but it's just slower.
	"""

	# Get set of previously visited files, the path to the import directory, and
	# the last run time (seconds since epoch) from config file.
	visited = set(configdata["internal"]["visited"])
	data_directory = configdata["settings"]["import_directory"]
	last_run = float(configdata["internal"]["last_run"])
	files_to_add = {}

	# For each directory (abstracts, annotation_umls, projects, disease, etc)
	for subdir in FilesToAdd.__required_keys__:
		# get all the files in the directory.
		subdir_path = os.path.join(data_directory, subdir)
		files = {os.path.join(subdir_path, x) for x in os.listdir(subdir_path)}

		# There are two cases where we need to add all the data in the file:
		# It's been modified since the last time the program was run, or it's never
		# been seen before.
		modified_since_last_run = {x for x in files if os.path.getmtime(x) > last_run}
		never_seen_before = files - visited
		absolute_files = to_list(never_seen_before | modified_since_last_run)
		files_to_add[subdir] = []
		for file in absolute_files:
			tmp, fname = os.path.split(file)
			_, dirname = os.path.split(tmp)
			files_to_add[subdir].append("file:///" + os.path.join(dirname, fname))
			
	return files_to_add

def update_configdata(configdata: dict[str, Any], fta: FilesToAdd) -> None:
	"""
	After running this script, we need to modify the config data to remember which
	files we visited on this run and when this run occurred, so next time we can
	check back to figure out which files have changed.
	"""
	new_visited = set(configdata["internal"]["visited"])
	for lst in fta.values():
		new_visited |= set(lst)
	configdata["internal"]["visited"] = list(new_visited)
	configdata["internal"]["last_run"] = time.time()

def step_to_fn(
	data_folder: str,
	query: str,
	description: str,
	constraint: Optional[str] = None) -> Callable[[Session, FilesToAdd], None]:
	"""
	Converts a `step` (one of the steps in steps.py) to an actual executable
	function that will be run in `main`. Basically just populates a template
	function (`fn`) with the data_folder, constraint, and query from the step.
	"""
	def fn(session: Session, fta: FilesToAdd) -> None:
		if constraint is not None:
			write(session, constraint, {})
		for file in fta[data_folder]:
			print("Processing file " + file)
			write(session, "LOAD CSV WITH HEADERS FROM $path AS data\n" + query, {"path": file})
	return fn

def main():
	# Open grant.conf for reading and writing
	print("Reading grant.conf")
	with open("grant.conf", "r+") as configfile:
		configdata = json.load(configfile)

		# Get all the new/modified files to add
		fta = get_files_to_add(configdata)

		print("Connecting to database")
		# Establish a connection to the database
		with GraphDatabase.driver(
						configdata["settings"]["neo4j_uri"],
						auth=(configdata["settings"]["neo4j_username"],
									configdata["settings"]["neo4j_password"])) as driver:
			with driver.session() as session:

				# run all the steps
				for step in steps:
					print("\n\n" + step["description"] + "...")
					step_to_fn(**step)(session, fta)

				print("\nWriting grant.conf")
				# Write the new configdata out to the file
				update_configdata(configdata, fta)
				configfile.seek(0)
				json.dump(configdata, configfile)
				configfile.truncate()

if __name__ == "__main__":
	main()

import os
import time
from neo4j import GraphDatabase, Session, Record
from typing import TypedDict, Any, Callable, Optional
from AlertCypher import AlertCypher
from steps import steps
from prep_neo4j_data import FilesToAdd, prep_data


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

def main(db: AlertCypher):
	#return
	# TODO: specify which folders store the raw and processed data on the server
	fta = prep_data("raw data folder here", "output data folder here")

	# run database upgrade steps on only new/modified files
	for step in steps:
		print("\n\n" + step["description"] + "...")
		step_to_fn(**step)(db.session, fta)

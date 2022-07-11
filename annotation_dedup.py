import configparser
from neo4j import GraphDatabase
from annotations import AnnotationManager

"""
This module contains various routines for deduplicating PubtatorAnnotation nodes
in the database. It consists of three steps:
- First we need to convert all text properties from str to list[str], as our
	duplicate-checking and merging functions all assume that the text property is
	a list
- Then we remove all duplicate PubtatorAnnotation nodes from the database.
- Finally, we prune all duplicate relationships connecting Gene
	PubtatorAnnotation nodes to Article nodes. We do this last as the previous
	step will already remove a lot of duplicate relationships, so this should be
	a relatively small step.
"""


def convert_text_property_to_strlist() -> None:
	"""
	Converts the text property of all PubtatorAnnotation nodes to a single-element
	list that just contains what the original string was.
	"""
	session.write_transaction(
		lambda tx: tx.run("MATCH (n:PubtatorAnnotation) SET n.text = [n.text]"))


def remove_all_duplicates() -> None:
	"""
	Removes all duplicate PubtatorAnnotations from the database.
	"""

	am = AnnotationManager(session)

	# This is quite an expensive query, but we have to get all the data from the
	# database anyway at some point.
	print("Getting all PubtatorAnnotation nodes from database.")
	nodes = am.get_single_nodes("MATCH (n:PubtatorAnnotation) RETURN n", {})
	print(f"Successfully got all {len(nodes)} PubtatorAnnotation nodes.")

	# We initialize a dictionary of "seen" nodes, i.e. duplicate nodes that we've
	# already merged and deleted. Because our local copy of the nodes won't update
	# when this happens, we need to know to skip over them while iterating through
	# the node list.
	seen = {node.id: False for node in nodes}

	for node in nodes:
		print("-----------------------------------------------------------------")

		# seen before (so this node is a duplicate that's already been merged). skip
		if seen[node.id]:
			print("Skipping below node as it has already been eliminated as a duplicate:")
			print(node)
			continue

		print("Searching for all duplicates for the below node:")
		print(node)

		# Find all duplicates of the current node in the database
		ad = AnnotationManager.node_to_annotationdata(node)
		duplicates = am.get_duplicates(ad)

		if len(duplicates) == 1 and duplicates[0].id == node.id:
			# Although we technically don't have to handle this as a separate case
			# (as create_or_merge below will work fine and have no effect), it's much
			# faster to skip here, as we won't have to wait for any further queries
			# with the database.
			print("No duplicates for this node.")
			seen[node.id] = True
			continue

		print("Duplicates to be merged:")
		print(duplicates)

		# Set all the duplicate nodes in our seen dict to True so we know to skip
		# them later on
		for dupe in duplicates:
			seen[dupe.id] = True

		# Merge all duplicates together
		am.create_or_merge(ad, duplicates)


def clean_all_article_connections():
	"""
	Prune all redundant Gene->Article connections in the database.
	"""

	am = AnnotationManager(session)

	# This query should be fairly fast as we only get the ids of the article
	# nodes that need cleaning (i.e. the article nodes that have exactly one
	# Species connection and more than one Gene connection).
	ids_need_cleaning = session.read_transaction(lambda tx: tx.run(
		"MATCH (p:PubtatorAnnotation {infons_type: 'Gene'})"
		"-[:ANNOTATION_FOR]->"
		"(a:Article)"
		"<-[:ANNOTATION_FOR]-"
		"(q:PubtatorAnnotation {infons_type: 'Species'}) "
		"WITH a,p,count(q) AS cntq "
		"WITH a,cntq,count(p) AS cntp "
		"WHERE cntq = 1 AND cntp > 1 "
		"RETURN id(a)"
	))

	# Clean all article nodes that satisfied the previous query.
	for id_to_clean in ids_need_cleaning:
		am.prune_article_rels(id_to_clean)


if __name__ == "__main__":

	# Read neo4j connection info in from the config file.
	# Make sure your config file has the correct credentials in it!
	config = configparser.ConfigParser()
	config.read("config.ini")
	user = config.get("credentials", "neo4j_username")
	password = config.get("credentials", "neo4j_password")
	neo4j_uri = config.get("credentials", "neo4j_uri")

	with GraphDatabase.driver(neo4j_uri, auth=(user, password)) as driver:
		with driver.session() as session:

			# Perform the three steps as described in this module's documentation.

			print("Converting the text property for all PubtatorAnnotation nodes to list...", end="")
			convert_text_property_to_strlist()
			print("...done.")

			print("Removing duplicates.")
			remove_all_duplicates()
			print("Done removing duplicates.")

			print("Cleaning article connections...", end="")
			clean_all_article_connections()
			print("...done.")

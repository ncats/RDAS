import configparser
from neo4j import Transaction, GraphDatabase


def fix_property(prop: str) -> None:
	"""
	For a given property name (e.g. pubmed_evidence, omim_evidence, refInOMIM),
	set that property to false if it is NULL on a given Article node.
	After this is run, no Article nodes should have this property as null - only
	true/false.

	:param prop: The property to "fix" (convert nulls to false)
	"""

	def do_query(tx: Transaction):
		# Cypher doesn't allow for setting a node property dynamically
		# (e.g. SET a[$prop] = false), but we can easily get around it by adding
		# a map of the desired property to the node. This will override the value
		# of the existing property.
		tx.run(
			"MATCH (a:Article) WHERE a[$prop] IS NULL "
			"SET a += $false_map",
			prop=prop, false_map={prop: False})

	session.write_transaction(do_query)


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

			# Fix each of pubmed_evidence, omim_evidence, refInOMIM in the existing
			# database, converting all null to false
			for prop in ["pubmed_evidence", "omim_evidence", "refInOMIM"]:
				fix_property(prop)

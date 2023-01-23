import configparser
from neo4j import GraphDatabase
from update_pubmed import create_epidemiology

def get_article_ids_and_abstracts(tx):
	query = "MATCH (a:Article) WHERE a.isEpi = 'Y' RETURN id(a), a.title, a.abstractText"
	res = tx.run(query)
	return [({'title': rec[1], 'abstractText': rec[2]}, rec[0]) for rec in res]


if __name__ == "__main__":

	# Read neo4j connection info in from the config file.
	# Make sure your config file has the correct credentials in it!
	config = configparser.ConfigParser()
	config.read("../config.ini")
	user = config.get("CREDENTIALS", "neo4j_username")
	password = config.get("CREDENTIALS", "neo4j_password")
	neo4j_uri = config.get("CREDENTIALS", "neo4j_uri")

	with GraphDatabase.driver(neo4j_uri, auth=(user, password)) as driver:
		with driver.session() as session:
			print("Established connection to database. Querying for existing article nodes with isEpi = 'Y'.")
			articles = session.read_transaction(get_article_ids_and_abstracts)
			print(f"Query returned {len(articles)} articles.")
			for article in articles:
				print(f"Adding EpidemiologyAnnotation node to article with id {article[1]}... ", end="")
				session.write_transaction(create_epidemiology, *article)
				print("Done.")

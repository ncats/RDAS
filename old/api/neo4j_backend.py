from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class Neo4jPubmed:

    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)

    def close(self):
        self.driver.close()

    def get_diseases(self):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(self._get_and_return_diseases)
            return result
    
    @staticmethod
    def _get_and_return_diseases(tx):
        query = (
            "MATCH(d:Disease) "
            "RETURN d.gard_id as gard_id, "
            "d.name as name, "
            "d.is_rare as is_rare, "
            "d.synonyms as synonyms "
            "limit 10"
        )
        result = tx.run(query)
        try:
            return [{"gard_id": row["gard_id"], "name": row["name"], "is_rare": row["is_rare"], "synonyms":row["synonyms"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "bolt+s://rdip2.ncats.io"
    p = Neo4jPubmed(uri)
    dlist = p.get_diseases()
    print(dlist)
    p.close()

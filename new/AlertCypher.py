from neo4j import GraphDatabase
import configparser
import os

class AlertCypher ():
    def __init__(self, db):
        workspace = os.path.dirname(os.path.abspath(__file__))
        init = os.path.join(workspace, 'config.ini')
        configuration = configparser.ConfigParser()
        configuration.read(init)

        server_uri = configuration.get("CREDENTIALS", "neo4j_uri")
        user = configuration.get("CREDENTIALS", "neo4j_username")
        password = configuration.get("CREDENTIALS", "neo4j_password")

        connection = GraphDatabase.driver(uri=server_uri, auth=(user, password))
        self.session = connection.session(database=db)

    def run(self, query):
        response = self.session.run(query)
        return response
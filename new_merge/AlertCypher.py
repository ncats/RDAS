from neo4j import GraphDatabase
import configparser
import os

# add access to config file through object

class AlertCypher ():
    def __init__(self, db):
        workspace = os.path.dirname(os.path.abspath(__file__))
        self.init = os.path.join(workspace, 'config.ini')
        self.configuration = configparser.ConfigParser()
        self.configuration.read(self.init)
        
        # Connects to neo4j databasej
        server_uri = self.configuration.get("CREDENTIALS", "neo4j_uri")
        user = self.configuration.get("CREDENTIALS", "neo4j_username")
        password = self.configuration.get("CREDENTIALS", "neo4j_password")

        connection = GraphDatabase.driver(uri=server_uri, auth=(user, password))
        self.session = connection.session(database=db)
        self.type = db

    def run(self, query, args=None):
        # Runs and returns cypher query
        if args == None:
            response = self.session.run(query)
        else:
            response = self.session.run(query, **args)
        return response
    
    def close(self):
        # Closes connection to database
        self.session.close()

    def DBtype(self):
        return self.type

    def getConf(self, section, field):
        value = self.configuration.get(section, field)
        return value

    def setConf(self, section, field, value):
        with open(self.init,"w") as conf:
            self.configuration.set(section, field, value)
            self.configuration.write(conf)
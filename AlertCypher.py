from neo4j import GraphDatabase,Auth
from neo4j.debug import watch
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
        server_uri = os.environ['NEO4J_URI']
        user = os.environ['NEO4J_USERNAME']
        password = os.environ['NEO4J_PASSWORD']

        #watch("neo4j")

        neo4j_auth = Auth(scheme='basic',principal=user,credentials=password)
        connection = GraphDatabase.driver(uri=server_uri, auth=neo4j_auth) 
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

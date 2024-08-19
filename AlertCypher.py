import sys
# sys.path.append('/home/aom2/.conda/envs/rdas/lib/python3.8/site-packages')
from neo4j import GraphDatabase,Auth
from neo4j.debug import watch
import configparser
import os

class AlertCypher ():
    """
    A class for interacting with a Neo4j database using the neo4j Python driver.
    """

    def __init__(self, db):
        """
        Initializes the AlertCypher class.

        Parameters:
        - db (str): The name of the Neo4j database to connect to.
        """
        
        # Initializes the object with the config.ini file
        """
        Initializes the AlertCypher class.

        Parameters:
        - db (str): The name of the Neo4j database to connect to.
        """

        # Initializes the object with the config.ini file
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
        """
        Executes a Cypher query on the Neo4j database.

        Parameters:
        - query (str): The Cypher query to execute.
        - args (dict, optional): Additional parameters for the query.

        Returns:
        - Response from the Neo4j database.
        """

        if args == None:
            response = self.session.run(query)
        else:
            response = self.session.run(query, **args)
        return response
    
    def close(self):
        """
        Closes the connection to the Neo4j database.
        """

        self.session.close()

    def DBtype(self):
        """
        Returns the type of the connected Neo4j database.

        Returns:
        - str: The type of the connected database.
        """

        return self.type

    def getConf(self, section, field):
        """
        Retrieves a configuration value from the specified section and field.

        Parameters:
        - section (str): The section in the configuration file.
        - field (str): The field in the configuration file.

        Returns:
        - str: The retrieved configuration value.
        """

        value = self.configuration.get(section, field)
        return value

    def setConf(self, section, field, value):
        """
        Sets a configuration value in the specified section and field.

        Parameters:
        - section (str): The section in the configuration file.
        - field (str): The field in the configuration file.
        - value (str): The value to set in the configuration.

        Writes the updated configuration to the config file.
        """
        
        with open(self.init,"w") as conf:
            self.configuration.set(section, field, value)
            self.configuration.write(conf)

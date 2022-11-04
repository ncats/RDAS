from neo4j import GraphDatabase
from datetime import date
from time import sleep
import load_neo4j

connection = GraphDatabase.driver(uri='bolt://localhost:7687', auth=('neo4j', 'test'))
session = connection.session()

while True:
    current_time = date.today()
    current_time = current_time.strftime("%m/%d/%y")
    print(current_time)
    sleep(10)
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from datetime import date
from http import client
from neo4j import GraphDatabase
from csv import DictReader
import configparser
import threading
import pandas as pd
lock = threading.Lock()

def main(db):
    print('update')
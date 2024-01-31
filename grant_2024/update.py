import sys
import os
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import methods as rdas
from AlertCypher import AlertCypher
import json
from time import sleep
from datetime import date
import grant_2024.init #change to import grant.init when done

def main ():
    grant_2024.init.main()
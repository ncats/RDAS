import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from datetime import datetime, date
from AlertCypher import AlertCypher
import sysvars
import grant.methods as rdas
from time import sleep

def main():
    db = AlertCypher(sysvars.gnt_db)
    rdas.start(db)

main()
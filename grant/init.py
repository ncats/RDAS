import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from datetime import datetime, date
from AlertCypher import AlertCypher
import sysvars
import grant.methods as rdas
import gard.methods as gmethods
from time import sleep

def main():
    print(f"[GNT] Database Selected: {sysvars.gnt_db}\nContinuing with script in 5 seconds...")
    sleep(5)

    db = AlertCypher(sysvars.gnt_db)

    rdas.start(db)

    gmethods.get_node_counts()


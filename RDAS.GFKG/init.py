import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from datetime import datetime, date
from AlertCypher import AlertCypher
import sysvars
import grant.methods as rdas
from time import sleep

def main(restart_raw=False, restart_processed=False):
    print(f"[CT] Database Selected: {sysvars.gnt_db}\nContinuing with script in 5 seconds...")
    print(f"Variables initialized: restart_raw -> {restart_raw}, restart_processed -> {restart_processed}")
    sleep(5)

    db = AlertCypher(sysvars.gnt_db)
    rdas.start(db, restart_raw=restart_raw, restart_processed=restart_processed)
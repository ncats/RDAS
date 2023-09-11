import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from datetime import datetime, date
from AlertCypher import AlertCypher
import sysvars
import pubmed.methods as rdas
import gard.methods as gmethods
from time import sleep

today = datetime.now()

def main(update_from=False):
    print(f"[PM] Database Selected: {sysvars.pm_db}\nContinuing with script in 5 seconds...")
    sleep(5)

    db = AlertCypher(sysvars.pm_db)

    if update_from:
        last_update = datetime.strptime(update_from, "%m/%d/%y")
        last_update = last_update.strftime("%Y/%m/%d")
        rdas.update_missing_abstracts(db,today)
    else:
        last_update = datetime.strptime(today, "%Y/%m/%d") - relativedelta(years=50)

    rdas.retrieve_articles(db, last_update, today)

    gmethods.get_node_counts()

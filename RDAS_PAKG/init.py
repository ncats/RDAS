import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append('/home/leadmandj/RDAS/')
sys.path.append(workspace)
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from AlertCypher import AlertCypher
import sysvars
import RDAS_PAKG.methods as rdas
from time import sleep

today = datetime.now().strftime("%Y/%m/%d")

def main(update_from=False, update_to=False):
    """
    Main function for a script that interacts with a database, related to PubMed data.

    Parameters:
    :param update_from: If provided, specifies the date from which to update. Defaults to False.

    Returns:
    None
    """

    print(f"[PM] Database Selected: {sysvars.pm_db}\nContinuing with script in 5 seconds...")
    sleep(5)

    # Initialize a connection to the database using the AlertCypher class
    db = AlertCypher(sysvars.pm_db)

    # Determine the last update date based on the provided parameter or default to 50 years ago
    if update_from:
        last_update = datetime.strptime(update_from, "%m/%d/%y")
        last_update = last_update.strftime("%Y/%m/%d")
        # If update_from is provided, call the update_missing_abstracts function
    else:
        last_update = datetime.strptime(today, "%Y/%m/%d") - relativedelta(years=50)
        last_update = last_update.strftime("%Y/%m/%d")

    if update_to:
        updating_to = datetime.strptime(update_to, "%m/%d/%y")
        updating_to = updating_to.strftime("%Y/%m/%d")
    else:
        updating_to = today

    print(last_update, updating_to)

    # Call the retrieve_articles function to gather and save articles in the database
    rdas.retrieve_articles(db, last_update, updating_to, today)
    rdas.update_missing_abstracts(db,today)

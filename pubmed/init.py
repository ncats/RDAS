import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from AlertCypher import AlertCypher
import sysvars
import pubmed.methods as rdas
from time import sleep

today = datetime.now().strftime("%Y/%m/%d")

def main(update_from=False):
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
        rdas.update_missing_abstracts(db,today)
    else:
        last_update = datetime.strptime(today, "%Y/%m/%d") - relativedelta(years=50)
        last_update = last_update.strftime("%Y/%m/%d")

    print(last_update, today)

    # Call the retrieve_articles function to gather and save articles in the database
    rdas.retrieve_articles(db, last_update, today)

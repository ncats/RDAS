import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
print(workspace)
sys.path.append(workspace)
sys.path.append('/home/leadmandj/RDAS/')
import sysvars
import RDAS_PAKG.init

def start_update (update_from=False, update_to=False):
    """
    Main function that initializes or updates a process related to PubMed data.
    Calls pubmed.init.py because the process to update is similar to creating from scratch.
    The only difference is the date range in which articles are searched for in.

    Parameters:
    :param update_from: Specifies the date to update from for the process. This is typically the date of the database's most recent update

    Returns:
    None
    """

    RDAS_PAKG.init.main(update_from=update_from, update_to=update_to)
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from AlertCypher import AlertCypher
from src import data_model as dm 
import methods as rdas
from datetime import date
import sysvars
from time import sleep
import threading
import numpy as np

#FUNCTIONS FOR MULTITHREADING, COMMENTED OUT FOR NOW
"""
def process_trial_add(thr, db, today, ids_to_add):
    for idx,ID in enumerate(ids_to_add):
        print(thr, idx, ID)
        trial_info = rdas.extract_fields(ID)
        if trial_info:
            print(f'Adding {ID}...')
            for node_type in dm.node_names:
                data_string = rdas.format_node_data(db,today,trial_info,node_type)
        else:
            print('Error in add for finding full trial data for ' + ID)

def process_trial_update(thr, db, today, current_nctids, ids_to_update):
    for idx,ID in enumerate(ids_to_update):
        print(thr, idx, ID)

        trial_info = rdas.extract_fields(ID)
        if trial_info:
            if not trial_info['LastUpdatePostDate'] == current_nctids[ID]:
                print('UPDATING...')
                for node_type in dm.node_names:
                    data_string = rdas.format_node_data(db,today,trial_info,node_type,update=True)


                #BELOW CREATES HISTORY NODE, POSTPONED FOR NOW

                #create_history_query = 'MATCH (x:ClinicalTrial {{NCTId:\"{ID}\"}}) CREATE (y:History) SET y=properties(x) CREATE (z:ClinicalTrial {data_string}) MERGE (y)<-[:updated_from]-(x) SET x=properties(z) SET x.DateCreatedRDAS=\"{today}\" SET x.LastUpdatedRDAS=\"{today}\" DELETE z return y'.format(ID=ID,data_string=data_string,today=today)
                #db.run(create_history_query)

        else:
            print('Error in update for finding full trial data for ' + ID)
"""

def main():
    """
    Main function for the data processing and updating of the Clinical Trial Neo4j Database.

    Parameters:
    - None

    Returns:
    - None
    """

    print(f"[CT] Database Selected: {sysvars.ct_db}\nContinuing with script in 5 seconds...")
    sleep(5)

    # Connect to the Neo4j database
    db = AlertCypher(sysvars.ct_db)

    # Get the current date
    today = date.today().strftime('%m/%d/%y')
    refreshed_ctgov_trials = list()
    
    print('Webscraping rare disease list')
    # Webscrape rare disease list from ClinicalTrials.gov
    ctgov_diseases,listed_trials = rdas.webscrape_ctgov_diseases()
    
    # Get NCT IDs for each disease
    for idx,ct_disease in enumerate(ctgov_diseases):
        ctgov_trials = rdas.get_nctids([ct_disease])
        refreshed_ctgov_trials.extend(ctgov_trials)
        print(idx, ct_disease, len(ctgov_trials))

    # Retrieve NCT IDs and last update dates from the database
    response = db.run('MATCH (x:ClinicalTrial) RETURN x.NCTId,x.LastUpdatePostDate').data()
    current_nctids = {i['x.NCTId']:i['x.LastUpdatePostDate'] for i in response}

    # Initialize lists for trials to update and add
    ids_to_update = list()
    length = len(refreshed_ctgov_trials)
    ids_to_add = list()

    print('Sorting trials to update and addition lists')

    # Check which trials to update and add
    for idx,k in enumerate(refreshed_ctgov_trials): #current_nctids
        if not k in current_nctids.keys(): #refreshed_ctgov_trials
            print(f'NCTID {k} NOT IN REFRESHED LIST, CHECKING FOR UPDATE')
            exist = db.run('MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{k}\" RETURN x.NCTId'.format(k=k)).data()
            if len(exist) == 0:
                print('+',end="")
                ids_to_add.append(k)
        else:
            print('.',end="")
            ids_to_update.append(k)
        print(str(idx) + '/' + str(length))

    print("")
    print('Checking ' + str(len(ids_to_update)) + ' Trials in Refreshed List for Updates')
    print('Adding ' + str(len(ids_to_add)) + ' Brand New Trials')

    print('Updating trials already in database')

    # Update trials already in the database
    for idx,ID in enumerate(ids_to_update):
        print(idx, ID)

        # Extract trial information from ClinicalTrials.gov
        trial_info = rdas.extract_fields(ID)
        if trial_info:
            if not trial_info['LastUpdatePostDate'] == current_nctids[ID]:
                print('UPDATING...')
                for node_type in dm.node_names:
                    data_string = rdas.format_node_data(db,today,trial_info,node_type,update=True)
                
                #BELOW CREATES HISTORY NODE, POSTPONED FOR NOW
                
                #create_history_query = 'MATCH (x:ClinicalTrial {{NCTId:\"{ID}\"}}) CREATE (y:History) SET y=properties(x) CREATE (z:ClinicalTrial {data_string}) MERGE (y)<-[:updated_from]-(x) SET x=properties(z) SET x.DateCreatedRDAS=\"{today}\" SET x.LastUpdatedRDAS=\"{today}\" DELETE z return y'.format(ID=ID,data_string=data_string,today=today)
                #db.run(create_history_query)  
        else:
            print('Error in update for finding full trial data for ' + ID)
    
    # Add brand new trials
    print('Adding non existent trials in database')
    for idx,ID in enumerate(ids_to_add):
        print(idx, ID)
        trial_info = rdas.extract_fields(ID)
        if trial_info:
            print(f'Adding {ID}...')
            for node_type in dm.node_names:
                data_string = rdas.format_node_data(db,today,trial_info,node_type)
        else:
            print('Error in add for finding full trial data for ' + ID)
     
    # Perform condition mapping
    rdas.condition_map(db, update_metamap=False)

    # Perform RxNorm mapping
    rdas.rxnorm_map(db)

if __name__ == "__main__":
    main()

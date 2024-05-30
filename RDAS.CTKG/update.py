import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
from AlertCypher import AlertCypher
from src import data_model as dm 
from datetime import date,datetime
import methods as rdas
from datetime import date
import sysvars
from time import sleep
import threading
import numpy as np
import csv
import json

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

    # Initialize variables containing NCTIDs to add and update
    ids_to_update = list()
    ids_to_add = list()
    # Retrieve NCT IDs and last update dates from the database
    response = db.run('MATCH (x:ClinicalTrial) RETURN x.NCTId,x.LastUpdatePostDate').data()
    current_nctids = {i['x.NCTId']:i['x.LastUpdatePostDate'] for i in response}

    # Get the current date
    today = date.today().strftime('%m/%d/%y')
    refreshed_ctgov_trials = list()

    in_progress = db.getConf('UPDATE_PROGRESS', 'clinical_in_progress')
    print(f'in_progress:: {in_progress}')
    if in_progress == 'True':
        with open(f'{sysvars.ct_files_path}ids_to_add.csv', 'r') as file1:
            ids_to_add = [line.rstrip('\n') for line in file1]
        with open(f'{sysvars.ct_files_path}ids_to_update.csv', 'r') as file2:
            ids_to_update = [line.rstrip('\n') for line in file2]
    
    else:
        # Set database config to say that database is in middle of an update and has not finished
        db.setConf('UPDATE_PROGRESS', 'clinical_in_progress', 'True')

        print('Webscraping rare disease list')
        # Webscrape rare disease list from ClinicalTrials.gov
        ctgov_diseases,listed_trials = rdas.webscrape_ctgov_diseases()
        
        # Get NCT IDs for each disease
        for idx,ct_disease in enumerate(ctgov_diseases):
            ctgov_trials = rdas.get_nctids([ct_disease])
            refreshed_ctgov_trials.extend(ctgov_trials)
            print(idx, ct_disease, len(ctgov_trials))

        length = len(refreshed_ctgov_trials)

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

        # There are duplicate trials because of the combined lists, remove duplicates
        ids_to_update = list(set(ids_to_update))

        # Save lists of ids in case error occurs during update
        with open(f'{sysvars.ct_files_path}ids_to_add.csv', 'w') as f:
            wr = csv.writer(f,delimiter="\n")
            wr.writerow(ids_to_add)
        with open(f'{sysvars.ct_files_path}ids_to_update.csv', 'w') as f:
            wr = csv.writer(f,delimiter="\n")
            wr.writerow(ids_to_update)

        print('lists of ids to add and update added locally')

    print("")
    print('Checking ' + str(len(ids_to_update)) + ' Trials in Refreshed List for Updates')
    print('Adding ' + str(len(ids_to_add)) + ' Brand New Trials')

    if in_progress == 'True':
        clinical_add_progress = db.getConf('UPDATE_PROGRESS', 'clinical_add_progress')
        if not clinical_add_progress == '':
            clinical_add_progress = int(clinical_add_progress)
        else:
            clinical_add_progress = 0

        clinical_update_progress = db.getConf('UPDATE_PROGRESS', 'clinical_update_progress')
        if not clinical_update_progress == '':
            clinical_update_progress = int(clinical_update_progress)
        else:
            clinical_update_progress = 0

        clinical_required_update_progress = db.getConf('UPDATE_PROGRESS', 'clinical_required_update_progress')
        if not clinical_required_update_progress == '':
            clinical_required_update_progress = int(clinical_required_update_progress)
        else:
            clinical_required_update_progress = 0

        clinical_rxnorm_progress = db.getConf('UPDATE_PROGRESS', 'clinical_rxnorm_progress')
        if not clinical_rxnorm_progress == '':
            clinical_rxnorm_progress = int(clinical_rxnorm_progress)
        else:
            clinical_required_update_progress = 0

        clinical_current_step = db.getConf('UPDATE_PROGRESS', 'clinical_current_step')
        
    else:
        clinical_add_progress = 0
        clinical_update_progress = 0
        clinical_required_update_progress = 0
        clinical_rxnorm_progress = 0
        clinical_current_step = ''

    # Add brand new trials
    print('Adding non existent trials in database')
    for idx,ID in enumerate(ids_to_add):
        if idx < clinical_add_progress:
            continue

        db.setConf('UPDATE_PROGRESS', 'clinical_add_progress', str(idx))
        print(idx, ID)

        trial_info = rdas.extract_fields(ID)
        if trial_info:
            print(f'Adding {ID}...')
            for node_type in dm.node_names:
                data_string = rdas.format_node_data(db,today,trial_info,node_type,ID)
        else:
            print('Error in add for finding full trial data for ' + ID)

    # Update trials already in the database
    print('Updating trials already in database')
    # Starts a new file if file exists but in_progress is false
    if in_progress == 'False' and os.path.exists(f'{sysvars.ct_files_path}ids_to_update_confirmed.csv'):
        os.remove(f'{sysvars.ct_files_path}ids_to_update_confirmed.csv')

    required_updates_nctids = list()
    # If update date files already exists and update in progress, load existing file
    if in_progress == 'True' and os.path.exists(f'{sysvars.ct_files_path}ids_to_update_confirmed.csv'):
        with open(f'{sysvars.ct_files_path}ids_to_update_confirmed.csv', 'r') as file3:
            required_updates_nctids = [line.rstrip('\n') for line in file3]

    # If update is in progress and file does not exist, create the file
    else:
        print('Sorting our clinical trials that actually need updates')
        for idx,ID in enumerate(ids_to_update):
            if idx < clinical_required_update_progress:
                continue
            db.setConf('UPDATE_PROGRESS', 'clinical_required_update_progress', str(idx))
            print(str(idx))

            postdate = rdas.get_lastupdated_postdate(ID)
            if postdate:
                if not postdate == current_nctids[ID]:
                    with open(f'{sysvars.ct_files_path}ids_to_update_confirmed.csv', 'a') as f:
                        wr = csv.writer(f,delimiter="\n")
                        wr.writerow([ID])

    for idx,ID in enumerate(required_updates_nctids):
        if idx < clinical_update_progress:
            continue
        db.setConf('UPDATE_PROGRESS', 'clinical_update_progress', str(idx))
        print(idx, ID)

        trial_info = rdas.extract_fields(ID)
        if trial_info:
            for node_type in dm.node_names:
                data_string = rdas.format_node_data(db,today,trial_info,node_type,ID,update=True)
        else:
            print('Error in add for finding full trial data for ' + ID)
            
                
            #BELOW CREATES HISTORY NODE, POSTPONED FOR NOW
            
            #create_history_query = 'MATCH (x:ClinicalTrial {{NCTId:\"{ID}\"}}) CREATE (y:History) SET y=properties(x) CREATE (z:ClinicalTrial {data_string}) MERGE (y)<-[:updated_from]-(x) SET x=properties(z) SET x.DateCreatedRDAS=\"{today}\" SET x.LastUpdatedRDAS=\"{today}\" DELETE z return y'.format(ID=ID,data_string=data_string,today=today)
            #db.run(create_history_query)

    # Perform condition mapping
    if clinical_current_step == '':
        rdas.condition_map(db)
        db.setConf('UPDATE_PROGRESS', 'clinical_current_step', 'rxnorm_map')

    # Perform RxNorm mapping
    if clinical_current_step == 'rxnorm_map':
        rdas.rxnorm_map(db, clinical_rxnorm_progress)

    # Update config values
    db.setConf('DATABASE', 'clinical_update', datetime.strftime(datetime.now(),"%m/%d/%y"))
    db.setConf('UPDATE_PROGRESS', 'clinical_in_progress', 'False')
    db.setConf('UPDATE_PROGRESS', 'clinical_add_progress', '')
    db.setConf('UPDATE_PROGRESS', 'clinical_current_step', '')
    db.setConf('UPDATE_PROGRESS', 'clinical_update_progress', '')
    db.setConf('UPDATE_PROGRESS', 'clinical_rxnorm_progress', '')
    db.setConf('UPDATE_PROGRESS', 'clinical_required_update_progress', '')

if __name__ == "__main__":
    main()

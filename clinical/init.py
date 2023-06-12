import sys
import os
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import methods as rdas
from src import data_model as dm
from AlertCypher import AlertCypher
import pandas as pd
import json

def main ():
    db = AlertCypher('alldata')

    if not os.path.exists(f'{sysvars.ct_files_path}ctgov_webscraped_names.csv'):
        print('[CT] WEBSCRAPING CTGOV RARE DISEASE NAMES')
        ctgov_diseases,listed_trials = rdas.webscrape_ctgov_diseases()
        ctgov_df = pd.DataFrame(ctgov_diseases)
        ctgov_df.to_csv(f'{sysvars.ct_files_path}ctgov_webscraped_names.csv', index=False)
    
    if not os.path.exists(f'{sysvars.ct_files_path}ctgov_nctids.json'):
        print('[CT] GATHERING RARE DISEASE RELATED NCTIDS')
        all_trials = dict()
        ctgov_diseases = pd.read_csv(f'{sysvars.ct_files_path}ctgov_webscraped_names.csv', index_col=False)
        ctgov_diseases = ctgov_diseases[ctgov_diseases.columns[0]].values.tolist()
        
        num_diseases = len(ctgov_diseases)
        for idx,ct_disease in enumerate(ctgov_diseases):
            print((int(idx)/num_diseases)*100)
            ctgov_trials = rdas.get_nctids([ct_disease])
            if len(ctgov_trials) > 0:
                all_trials[ct_disease] = ctgov_trials

        with open(f'{sysvars.ct_files_path}ctgov_nctids.json', 'w') as outfile:
            json.dump(all_trials, outfile)
            all_trials = None
   
    
    if not os.path.exists(f'{sysvars.ct_files_path}all_trial_data.json'):
        print('[CT] GATHERING FULL DATA FOR EACH RARE DISEASE RELATED TRIAL') 
        with open(f'{sysvars.ct_files_path}ctgov_nctids.json') as json_file:
            ctgov_nctids = json.load(json_file)
            
            all_info = list()
            num_diseases = len(ctgov_nctids)
            for idx,(k,v) in enumerate(ctgov_nctids.items()):
                print((int(idx)/num_diseases)*100)
                for ID in v:
                    trial_info = rdas.extract_fields(ID)

                    if trial_info:
                        all_info.append(trial_info)

            with open(f'{sysvars.ct_files_path}all_trial_data.json', 'w') as outfile:
                json.dump(all_info, outfile)

    
    if os.path.exists(f'{sysvars.ct_files_path}all_trial_data.json') and not os.path.exists(f'{sysvars.ct_files_path}queries.csv'):
        print('[CT] BUILDING QUERIES FOR NODE CREATION')
        with open(f'{sysvars.ct_files_path}all_trial_data.json', 'r') as json_file:
            queries = list()
            full_data = json.load(json_file)
            num_trials = len(full_data)

            for idx,trial_data in enumerate(full_data):
                print((int(idx)/num_trials)*100)
                for node_type in dm.node_names:
                    trial_string = rdas.format_node_data(trial_data, node_type)
                    #print(trial_string)
                    queries.extend(trial_string)
            print(queries)

            queries = pd.DataFrame(queries)
            #print(queries)
            queries.to_csv(f'{sysvars.ct_files_path}queries.csv',index=False)

    if os.path.exists(f'{sysvars.ct_files_path}queries.csv'):
        print('[CT] POPULATING DATABASE')
        queries = pd.read_csv(f'{sysvars.ct_files_path}queries.csv', dtype=str)
        queries = queries[queries.columns[0]].values.tolist()
        num_queries = len(queries)        

        for idx,query in enumerate(queries):
            print((int(idx)/num_queries)*100)
            print(query)
            db.run(query)

    else:
        print('No clinical trial data saved in src directory')

if __name__ == "__main__":
    main()

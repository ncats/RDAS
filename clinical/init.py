import sys
import os
import multiprocessing
from multiprocessing import Process
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import methods as rdas
from src import data_model as dm
from AlertCypher import AlertCypher
import pandas as pd
import json
from datetime import date

cores = multiprocessing.cpu_count()
now = date.today()
now = now.strftime("%m/%d/%y")


def generate_queries_populate(db, pnum, file_batch):
    for idx,filename in enumerate(file_batch):
        print(f'[{pnum}] ' + str(idx) + f' Disease Query Files Generated [{filename}]')
        with open(f'{sysvars.ct_files_path}full_trial_data/{filename}', 'r') as json_file:
            #queries = list()
            full_data = json.load(json_file)
            num_trials = len(full_data)

            for trial_data in full_data:
                for node_type in dm.node_names:
                    trial_string = rdas.format_node_data(db, trial_data, node_type)
                    #queries.extend(trial_string)

            #queries = pd.DataFrame(queries)
            #filename = filename.replace('.json','')
            #queries.to_csv(f'{sysvars.ct_files_path}all_queries/{filename}.csv',index=False)

def gather_full_data(pnum, ctgov_nctids):
  total_diseases = len(ctgov_nctids)
  for idx,(k,v) in enumerate(ctgov_nctids.items()):
    all_info = list()
    gard_filename = k.replace(':','').replace('/',' ').replace(',','')
    num_diseases = len(ctgov_nctids[k])
    print('[' + str(pnum) + '] '+ str(idx) + '/' + str(total_diseases) + ' Diseases Processed; ' + str(num_diseases) + ' Clinical Trials in Section [' + k + ']')

    for ID in v:
      trial_info = rdas.extract_fields(ID)
      if trial_info:
        all_info.append(trial_info)
      else:
        print('Error finding full trial data for ' + ID)

      with open(f'{sysvars.ct_files_path}full_trial_data/{gard_filename}.json', 'w') as outfile:
        json.dump(all_info, outfile)

def main ():
    # Initialize database driver object, script will write to specified database
    db = AlertCypher('clinical')

    # Webscrapes rare disease names from the clinicaltrial.gov website
    if not os.path.exists(f'{sysvars.ct_files_path}ctgov_webscraped_names.csv'):
        print('[CT] WEBSCRAPING CTGOV RARE DISEASE NAMES')
        ctgov_diseases,listed_trials = rdas.webscrape_ctgov_diseases()
        ctgov_df = pd.DataFrame(ctgov_diseases)
        ctgov_df.to_csv(f'{sysvars.ct_files_path}ctgov_webscraped_names.csv', index=False)
    
    # Gathers all NCTIDs for each webscraped rare disease names
    if not os.path.exists(f'{sysvars.ct_files_path}ctgov_nctids.json'):
        print('[CT] GATHERING RARE DISEASE RELATED NCTIDS')
        all_trials = dict()
        ctgov_diseases = pd.read_csv(f'{sysvars.ct_files_path}ctgov_webscraped_names.csv', index_col=False)
        ctgov_diseases = ctgov_diseases[ctgov_diseases.columns[0]].values.tolist()
        
        num_diseases = len(ctgov_diseases)
        for idx,ct_disease in enumerate(ctgov_diseases):
            print((int(idx)/num_diseases)*100)
            ctgov_trials = rdas.get_nctids([ct_disease])
            print(str(len(ctgov_trials)) + ' NCTIDs Gathered [' + ct_disease + ']')
            if len(ctgov_trials) > 0:
                all_trials[ct_disease] = ctgov_trials

        with open(f'{sysvars.ct_files_path}ctgov_nctids.json', 'w') as outfile:
            json.dump(all_trials, outfile)
            all_trials = None

   
    # Retrieves all the data for the trial for each NCTID
    if not os.path.exists(f'{sysvars.ct_files_path}full_trial_data/'):
        if not os.path.exists(f'{sysvars.ct_files_path}full_trial_data/'):
            os.makedirs(f'{sysvars.ct_files_path}/full_trial_data')

        print('[CT] GATHERING FULL DATA FOR EACH RARE DISEASE RELATED TRIAL') 
        with open(f'{sysvars.ct_files_path}ctgov_nctids.json') as json_file:
            ctgov_nctids = json.load(json_file)
            total_diseases = len(ctgov_nctids)
            processes = list()
            batch_size = (total_diseases//cores)+1
            batches = [ctgov_nctids[i:i + batch_size] for i in range(0, total_diseases, batch_size)]
            
            for idx, batch in enumerate(batches):
                proc = Process(target=gather_full_data, args=(idx,batch))
                processes.append(proc)
                proc.start()

            for process in processes:
                process.join()

    # Generates queries to create each clinical trial and all additional nodes connected to it    
    if os.path.exists(f'{sysvars.ct_files_path}full_trial_data/'):
      if not os.path.exists(f'{sysvars.ct_files_path}all_queries/'):
        os.makedirs(f'{sysvars.ct_files_path}/all_queries')
        print('[CT] BUILDING QUERIES FOR NODE CREATION')
      
      processes = list()
      filelist = os.listdir(f'{sysvars.ct_files_path}full_trial_data/')[progress:]
      batch_size = (len(filelist)//cores)+1
      batches = [filelist[i:i + batch_size] for i in range(0, len(filelist), batch_size)]

      print(f'[CT] STARTING PROCESSING FROM IDX {progress}')
      for idx, batch in enumerate(batches):
          proc = Process(target=generate_queries_populate, args=(db,idx,batch))
          processes.append(proc)
          proc.start()
        
      for process in processes:
          process.join()
    
      # Maps clinical trial condition nodes to GARD disease nodes
      print('[CT] MAPPING CONDITION NODES TO GARD')
      rdas.condition_map(db)
        
      # Creates Drug nodes from Intervention nodes; Maps RxNorm data to the Drug nodes
      print('[CT] MAPPING DRUG INTERVENTIONS TO RXNORM')
      rdas.rxnorm_map(db)

      print('[CT] DATABASE BUILD COMPLETED')

    else:
      print('No clinical trial data saved in src directory')

if __name__ == "__main__":
    main()

import os
import re
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
import json
from dotenv import load_dotenv
load_dotenv()

import spacy
from spacy.matcher import Matcher
# Setup NLP for RxNORM Mapping
nlp = spacy.load('en_ner_bc5cdr_md')
pattern = [{'ENT_TYPE':'CHEMICAL'}]
matcher = Matcher(nlp.vocab)
matcher.add('DRUG',[pattern])

import requests
from utils.conn import DBConnection as db
from utils.tools import  _clean, _id_range_generator, ask_to_continue

from utils.applogger import AppLogger
logger = AppLogger().get_logger()

#MySQL
"""
# Step 3: Store Clinical-Trial Intervertion-Drug properties MySQL database
# Creatde index on gard_id
# Create index on nctid
# (There is no SPL_SET_ID property added to "Drug" in step 2, see init_clinical_trial_step_2.py)
#
# conda activate rds
# python clinical_trial/init_3_clinical_trial_step_3.py  
#
"""

class InterventionDrugInitializer:

    def __init__(self):

        self.mysql = db().mysql_conn()


    def do_init(self, start_id, end_id):
        # 1. Get the GARD id, nctid and study from the MySQl database
        results = self._get_GARD_CT_study_from_database(start_id, end_id)

        for idx, row in enumerate(results):

            gardid = row[0]
            disease = row[1] 
            nctid = row[2] 
            study = json.loads(row[3])
            id = row[4]
            print(f"\n### [{start_id} - {end_id}] Id: {id}, Gard_ID: {gardid}, NCTID: {nctid}, Disease: {disease}")


            intervention_module = study.get('protocolSection', dict()).get('armsInterventionsModule', dict())
            interventions = intervention_module.get('interventions', list())

            if interventions == list(): # if is empty
                continue

            for intervention in interventions: 

                intervention_name = _clean(intervention.get('name','')) 
                intervention_type = _clean(intervention.get('type',''))
        
                if intervention_type == 'DRUG':                    
                    self.rxnorm_map(gardid, disease, nctid, intervention_name)



    def rxnorm_map(self, gardid, disease, nctid, intervention_name):

        cursor = self.mysql.cursor()
        sql = "INSERT INTO clinical_trial_intervention_drug (gardId, disease, nctid, rxnormid, intervention, drug_name, wspacy, property_key, property_val) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

        def add_to_db(rxdata, intervention, drug_name, wspacy):

            rxnormid = rxdata['RxNormID']

            # Set additional properties on the Drug node
            for property_key, property_val in rxdata.items():

                property_key = property_key.replace(' ','')
                if isinstance(property_val, list):
                    property_val = json.dumps(property_val) 

                #print(f'{gardid}\t{disease}\t{nctid}\t{rxnormid}\t{intervention}\t{property_key}\t{property_val}')
                print(f'{gardid}\t{nctid}\t{rxnormid}\t{property_key}')

                val = (gardid, disease, nctid, rxnormid, intervention, drug_name, wspacy, property_key, property_val)
                cursor.execute(sql, val)


        def nlp_to_drug(intervention, matches):

            for match_id, start, end in matches:
                drug = doc[start:end].text

                # Retrieve RxNorm data for the drug name
                rxdata = self.get_rxnorm_data(drug.replace(' ','+'))

                if rxdata:
                    # Create connections in the database using RxNorm data
                    add_to_db(rxdata, intervention, drug, wspacy=1)

                else:
                    print(f'\t\tMap to RxNorm failed for intervention name:{intervention}, drug name: {drug}')
    

        def drug_normalize(drug_name):

            # Remove non-ASCII characters
            new_val = drug_name.encode("ascii", "ignore")
            # Decode the bytes to string
            updated_str = new_val.decode()
            # Replace non-word characters with spaces
            updated_str = re.sub('\W+',' ', updated_str)
            return updated_str
        

        drug = drug_normalize(intervention_name)
        the_drug = drug.replace(' ','+')

        # Retrieve RxNorm data for the drug name
        rxdata = self.get_rxnorm_data(the_drug)

        if rxdata:
            print(f'\t\tDrug: {the_drug}, rxdata.RxNormID = {rxdata["RxNormID"]}') ########
            # Create connections in the database using RxNorm data
            add_to_db(rxdata, intervention_name, drug, 0)

        else:
            # If RxNorm data not found, use SpaCy NLP to detect drug names and map to RxNorm
            doc = nlp(drug)
            matches = matcher(doc)
            
            nlp_to_drug(intervention_name, matches)

        self.mysql.commit()


    def get_rxnorm_data(self, drug_name):
            
            # Initialize retry counter
            retries = 0
            rxnormid = None
            max_retries=10

            rxdata = dict()
            while retries < max_retries:
                try:
                    # Form RxNav API request to get RxNormID based on drug name
                    rq = f'https://rxnav.nlm.nih.gov/REST/rxcui.json?name={drug_name}&search=2'
                    response = requests.get(rq)
                    response.raise_for_status()  # Raise an exception for HTTP errors (4xx and 5xx)
                                    
                    # Extract RxNormID from the response 
                    try:
                        obj = response.json()
                        rxnormid = obj['idGroup']['rxnormId'][0]
                        rxdata['RxNormID'] = rxnormid

                    except KeyError as e:
                        print(f"KeyError: {e} - The required key does not exist in the JSON structure.")
                        print(f'\n{obj}\n')
                        rxnormid = None  # or some default value or behavior
                    except IndexError:
                        print("IndexError: The 'rxnormId' list is empty or does not have an element at index 0.")
                        print(f'\n{obj}\n')
                        rxnormid = None  # or handle this case appropriately
                    except (TypeError, AttributeError):
                        print("The JSON structure is not as expected or 'response' might not be JSON.")
                        rxnormid = None  # or handle this case appropriately
                    
                    break  # Exit the loop if successful
                except requests.exceptions.Timeout:
                    retries += 1
                    time.sleep(1)
                except requests.exceptions.RequestException as e:
                    break  # Exit the loop for non-retryable errors

            if not rxnormid:
                return None
            
            # re-init
            retries = 0
            max_retries=10
            while retries < max_retries:
                try:        

                    # Form RxNav API request to get all properties of the drug using RxNormID
                    rq2 = f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxnormid}/allProperties.json?prop=codes+attributes+names+sources'
                    response = requests.get(rq2)
                    results = response.json()['propConceptGroup']['propConcept']

                    # Extract and organize properties of the drug
                    for r in results:
                        propName = r['propName']
                        if propName in rxdata:
                            rxdata[propName].append(r['propValue'])
                        else:
                            rxdata[propName] = [r['propValue']]
                    return rxdata
                
                except requests.exceptions.Timeout:
                    retries += 1
                    time.sleep(1)
                except requests.exceptions.RequestException as e:
                    break  # Exit the loop for non-retryable errors
 

    def _get_GARD_CT_study_from_database(self, start_id, end_id):
       
        # Query to fetch data within an id range
        query = f'''
            SELECT gardid, disease, nctid, studies, id
            FROM clinical_trial 
            WHERE nctid IS NOT NULL 
            AND id BETWEEN {start_id} AND {end_id}
            ORDER BY id
        '''

        print(query)
        try:
            cursor = self.mysql.cursor()

            cursor.execute(query)
            results = cursor.fetchall() 
            return results
        except Exception as err:
            print(f"Error: {err}")
            return None
    
        finally:
            # Ensure resources are closed
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'connection' in locals() and self.mysql.is_connected():
                self.mysql.close()



if __name__ == '__main__':

    
    ok = ask_to_continue('Store Clinical-Trial Intervertion-Drug properties MySQL database?')
    if not ok:
        sys.exit('------Stopped ------')


    initlzr = InterventionDrugInitializer()

    # Total nodes '499264' 

    # Starting and ending IDs
    min_id = 3
    max_id = 1497792
    step = 3 # Id increases by 3
    batch_size = 1000

    # Calculate number of rows (total steps)
    total_rows = (max_id - min_id) // step + 1  # 499,263 rows
    print(f"Total rows to fetch: {total_rows}")
 
 
    id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
 
    # For loop over ID ranges
    for start_id, end_id in id_ranges: 
         
        print(f"Fetching batch: id {start_id} to {end_id}")

        initlzr.do_init(start_id, end_id) 

    print('\n------------ All done -------------\n')

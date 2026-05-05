import os
import sys
import json
import re
import time
import requests

# Add the project root to the Python path
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
    os.path.abspath(os.path.join(_dir, '../..'))
]) 

import spacy
from spacy.matcher import Matcher
# Setup NLP for RxNORM Mapping
nlp = spacy.load('en_ner_bc5cdr_md')
pattern = [{'ENT_TYPE':'CHEMICAL'}]
matcher = Matcher(nlp.vocab)
matcher.add('DRUG',[pattern])

# Initialize colorama for Windows compatibility
from colorama import init, Fore, Style
init()

# Import custom utilities
from utils.tools import ( _clean, ask_to_continue, _date_string)
from utils.quality import exclude_words
from utils.https_request import HTTPSUtils as HttpsUtil
from utils.file_appender import FileAppender 
from baseclass.init_base import InitBase

class InterventionDrugDataUpdater(InitBase):

    def __init__(self):

        super().__init__('clinical_trial_intervention_drug', 'Data-ClinicalTrial-InterventionDrugDataUpdater')

        self.timeout = 10
        self.max_retries = 10         
        
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/G-2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    # generator function
    def _get_new_clinical_trial(self, batch_size=20):
    
        # Query to fetch data with is_new = 1
        query = 'SELECT id, gardid, disease, nctid, studies FROM clinical_trial WHERE is_new = 1'
        
        try:
            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            cursor.execute(query)
            
            # Fetch results in batches
            while True:
                results = cursor.fetchmany(batch_size)
                if not results:
                    break

                yield results
                
        except Exception as err:
            self.appender.log_stdout(f"Error: {Fore.RED}{err}{Style.RESET_ALL}")
            return None
        
        finally:
            # Ensure resources are closed
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'connection' in locals() and self.mysql.is_connected():
                self.mysql.close()


    # Overwrite
    def update(self):
        """Process new clinical trials and map drug interventions to RxNorm."""
        
        # 1. Get new clinical trials in batches (default batch_size = 20)
        clinical_trial_generator = self._get_new_clinical_trial()

        # 2. Process each batch
        for batch in clinical_trial_generator:

            # 2. Process each row in the batch
            for row in batch:
                # 2.1 Skip rows without study data early
                study_str = row.get('studies')
                if not study_str:
                    continue

                # 2.2 Extract basic fields
                id = row.get('id')
                gardid = row.get('gardid')
                disease = row.get('disease')
                nctid = row.get('nctid')

                # 2.3 Parse study JSON
                try:
                    study = json.loads(study_str)
                except json.JSONDecodeError as e:
                    self.appender(f"Error parsing JSON for ID {id}: {Fore.RED}{e}{Style.RESET_ALL}")
                    continue

                self.appender.log_stdout(f"#[Id: {id}, Gard_ID: {gardid}, NCTID: {nctid}, Disease: {disease}]")

                # 3. Navigate to interventions with safe dictionary access
                interventions = (study.get('protocolSection', {})
                            .get('armsInterventionsModule', {})
                            .get('interventions', []))

                # 3.1 Skip if no interventions
                if not interventions:
                    continue

                # 3.2 Process only drug interventions
                for intervention in interventions:
                    intervention_type = _clean(intervention.get('type', ''))
                    
                    if intervention_type == 'DRUG':
                        intervention_name = _clean(intervention.get('name', ''))

                        if intervention_name:  # Only process if name exists
                            self.rxnorm_map(gardid, disease, nctid, intervention_name)


    def rxnorm_map(self, gardid, disease, nctid, intervention_name):

        cursor = self.mysql.cursor()

        sql = f'''
            INSERT INTO {self.table_name} 
                (gardId, disease, nctid, rxnormid, intervention, drug_name, wspacy, property_key, property_val, is_new) 
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''

        def add_to_db(rxdata, intervention, drug_name, wspacy):

            rxnormid = rxdata['RxNormID']

            # Set additional properties on the Drug node
            for property_key, property_val in rxdata.items():

                property_key = property_key.replace(' ','')
                if isinstance(property_val, list):
                    property_val = json.dumps(property_val) 

                self.appender.log_stdout(f'{gardid}\t{nctid}\t{rxnormid}\t{property_key}')

                #
                val = (gardid, disease, nctid, rxnormid, intervention, drug_name, wspacy, property_key, property_val, 1)
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
                    self.appender.log_stdout(f'\t\tMap to RxNorm failed for intervention name:{intervention}, drug name: {drug}')
    

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
            self.appender.log_stdout(f'\t\tDrug: {the_drug}, rxdata.RxNormID = {rxdata["RxNormID"]}') ########
            # Create connections in the database using RxNorm data
            add_to_db(rxdata, intervention_name, drug, 0)

        else:
            # If RxNorm data not found, use SpaCy NLP to detect drug names and map to RxNorm
            doc = nlp(drug)
            matches = matcher(doc)
            
            nlp_to_drug(intervention_name, matches)

        self.mysql.commit()


    def get_rxnorm_data(self, drug_name):
        """Retrieve RxNorm data for a given drug name."""
        MAX_RETRIES = 10
        RETRY_DELAY = 1  # seconds
        
        # Step 1: Get RxNormID
        rxnormid = self._get_rxnormid(drug_name, MAX_RETRIES, RETRY_DELAY)
        if not rxnormid:
            return None
        
        # Step 2: Get drug properties
        return self._get_drug_properties(rxnormid, MAX_RETRIES, RETRY_DELAY)


    def _get_rxnormid(self, drug_name, max_retries, retry_delay):
        """Retrieve RxNormID for a given drug name."""
        url = f'https://rxnav.nlm.nih.gov/REST/rxcui.json?name={drug_name}&search=2'
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                obj = response.json()
                rxnormid = obj.get('idGroup', {}).get('rxnormId', [None])[0]
                
                if rxnormid:
                    return rxnormid
                else:
                    self.appender.log_stdout(f"No RxNormID found for drug: {drug_name}")
                    self.appender.log_stdout(f'\n{obj}\n')
                    return None
                    
            except requests.exceptions.Timeout:
                self.appender.log_stdout(f"Timeout on attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
            except requests.exceptions.RequestException as e:
                self.appender.log_stdout(f"Request error: {e}")
                return None
        
        self.appender.log_stdout(f"Failed to retrieve RxNormID after {max_retries} attempts")
        return None


    def _get_drug_properties(self, rxnormid, max_retries, retry_delay):
        """Retrieve all properties for a given RxNormID."""

        url = f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxnormid}/allProperties.json?prop=codes+attributes+names+sources'
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                obj = response.json()
                results = obj.get('propConceptGroup', {}).get('propConcept', [])
                
                rxdata = {'RxNormID': rxnormid}
                for r in results:
                    prop_name = r.get('propName')
                    prop_value = r.get('propValue')
                    
                    if prop_name and prop_value:
                        if prop_name in rxdata:
                            rxdata[prop_name].append(prop_value)
                        else:
                            rxdata[prop_name] = [prop_value]
                
                return rxdata
                
            except requests.exceptions.Timeout:
                self.appender.log_stdout(f"Timeout on attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
            except requests.exceptions.RequestException as e:
                self.appender.log_stdout(f"Request error: {e}")
                return None
        
        self.appender.log_stdout(f"Failed to retrieve drug properties after {max_retries} attempts")
        return None
    


if __name__ == '__main__':

    ok = ask_to_continue('Update clinical trial intervention and drug data?')
    
    if not ok:
        sys.exit(f'{Fore.RED}------Stopped------{Style.RESET_ALL}')
 
    # Use context manager to ensure proper cleanup
    with InterventionDrugDataUpdater() as updater:
        updater.update()

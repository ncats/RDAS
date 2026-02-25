import os
import sys
import json
import time
import requests

# Add the project root to the Python path
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
    os.path.abspath(os.path.join(_dir, '../..'))
]) 

# Initialize colorama for Windows compatibility
from colorama import init, Fore, Style
init()

# Import custom utilities
from utils.tools import ( _clean, ask_to_continue, _date_string)
from utils.quality import exclude_words
from utils.https_request import HTTPSUtils as HttpsUtil
from utils.file_appender import FileAppender 
from baseclass.init_base import InitBase

class ClinicalTrialPubchemIdDataUpdater(InitBase):


    def __init__(self):

        super().__init__('clinical_trial_nctid_pmids_mapping', 'Data-ClinicalTrialPubchemIdDataUpdater')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/G-3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    # Overwrite
    def update(self):
        
        total = 0
        batch_idx = 0 

        # 1. Get new clinical trials as batches
        batches = self._get_new_clinical_trial() # generator function

        for batch in batches:

            batch_idx += 1
            self.appender.log_stdout(f'\n=== batch {batch_idx} ===')

            pairs = set()  # Use set for automatic deduplication

            for row in batch:
                try:
                    nctid = row['nctid']
                    pmids = self._extract_pmids_from_study(row['studies'])
                    
                    for pmid in pmids:
                        pairs.add((nctid, pmid))
                        
                except (KeyError, json.JSONDecodeError) as e:
                    self.appender.log_stdout(f"Error processing row for NCTID {row.get('nctid', 'unknown')}: {e}")
                    continue          


            # 3. insert pairs into database
            insert_sql = f'INSERT INTO {self.table_name} (nctid, pmid, is_new) VALUES (%s, %s, 1)' 
                        
            try:
                cursor = self.mysql.cursor()
                cursor.executemany(insert_sql, list(pairs))
                self.mysql.commit()
                
                total += len(pairs)
                self.appender.log_stdout(f'{total} pairs saved')
            except Exception as e:
                self.appender.log_stdout(f"Error: {Fore.RED}{e}{Style.RESET_ALL}")
                return
        
  
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and self.mysql.is_connected():
            self.mysql.close()

        self.appender.log_stdout(f'\n{"*"*27} Total pairs: {total} saved into database {"*"*27} \n')
               


    def get_nctid_pmids_unique_pairs(self):
        """Extract all unique (NCTID, PMID) pairs from new clinical trials."""
        pairs = set()  # Use set for automatic deduplication

        batches = self._get_new_clinical_trial() # generator function

        for batch in batches:
            for row in batch:
                try:
                    nctid = row['nctid']
                    pmids = self._extract_pmids_from_study(row['studies'])
                    
                    for pmid in pmids:
                        pairs.add((nctid, pmid))
                        
                except (KeyError, json.JSONDecodeError) as e:
                    self.appender.log_stdout(f"Error processing row for NCTID {row.get('nctid', 'unknown')}: {e}")
                    continue
        
        return list(pairs)


    def _extract_pmids_from_study(self, study_json):
        """Extract and clean all PMIDs from a study JSON string."""
        try:
            study = json.loads(study_json)
        except json.JSONDecodeError as e:
            self.appender.log_stdout(f"Failed to parse study JSON: {e}")
            return []
        
        ref_module = study.get('protocolSection', {}).get('referencesModule', {})
        references = ref_module.get('references', [])
        
        pmids = []
        for ref in references:
            pmid = ref.get('pmid')
            if pmid:
                cleaned_pmid = _clean(pmid)
                if cleaned_pmid:  # Only add if cleaning didn't return None/empty
                    pmids.append(cleaned_pmid)
        
        return pmids
    
    
    # generator function
    def _get_new_clinical_trial(self, batch_size=20):
    
        # Query to fetch data with is_new = 1
        query = 'SELECT nctid, studies  FROM clinical_trial_unique WHERE is_new = 1'  
        
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



if __name__ == '__main__':

    ok = ask_to_continue('Update clinical trial Pubchem ID data ?')

    if not ok:
        sys.exit(f'{Fore.RED}------Stopped------{Style.RESET_ALL}')
 
    # Use context manager to ensure proper cleanup
    with ClinicalTrialPubchemIdDataUpdater() as updater:
        updater.update()



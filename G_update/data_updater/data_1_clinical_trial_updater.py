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
from utils.tools import ( _is_english,  _is_under_char_threshold, ask_to_continue, _len_greater_than_threshold, elapsed_time,  _to_txt,  _date_string)
from utils.quality import exclude_words
from utils.https_request import HTTPSUtils as HttpsUtil
from utils.file_appender import FileAppender 
from baseclass.init_base import InitBase
 
class ClinicalTrialDataUpdater(InitBase):
    """
    Updates clinical trial data from ClinicalTrials.gov API for GARD disease nodes.
    """
 
    def __init__(self):

        super().__init__('clinical_trial_unique', 'Data-ClinicalTrialUpdater')

        self.timeout = 10
        self.max_retries = 10        
        self.last_update= '01/01/2025' # important starting date
        
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/G-1-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
 

    def _call_get_nctids(self, query, pageToken=None):
        """
        Make API call to ClinicalTrials.gov to get NCT IDs.        
        Args:
            query (str): API query string
            pageToken (str, optional): Pagination token for subsequent requests            
        Returns:
            dict: JSON response from the API, or None if failed
        """
        try:
            if pageToken: 
                query += f'&pageToken={pageToken}'
            
            response = requests.get(query, timeout=self.timeout)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            self.appender.log_stdout(f'{Fore.RED}Timeout for query: {query}{Style.RESET_ALL}')
            return None
        except requests.exceptions.RequestException as e:
            self.appender.log_stdout(f'Request failed for query: {query}\nError: {Fore.RED}{e}{Style.RESET_ALL}')
            return None
        except Exception as e:
            self.appender.log_stdout(f'Unexpected error processing query: {query}\n{Fore.RED}{e}{Style.RESET_ALL}')
            return None

 
    def _fetch_clinical_trial_details(self, nctid):
        """
        Fetch detailed information for a specific clinical trial.        
        Args:
            nctid (str): NCT ID of the clinical trial            
        Returns:
            dict: Trial details JSON, or None if failed
        """
        # Initialize retry counter
        retries = 0

        while retries < self.max_retries:
            try:
                response = requests.get(f'https://clinicaltrials.gov/api/v2/studies/{nctid}', timeout=self.timeout)
                response.raise_for_status()  # Raise an exception for HTTP errors (4xx and 5xx)

                # Parse JSON response
                return response.json()

            except requests.exceptions.Timeout:
                self.appender.log_stdout(f"{Fore.RED}Timeout occurred for {nctid}, retrying...{Style.RESET_ALL}")
                retries += 1
                time.sleep(1)

            except requests.exceptions.RequestException as e:
                self.appender.log_stdout(f"Request failed for {nctid}: {Fore.RED}{e}{Style.RESET_ALL}")
                break  # Exit the loop for non-retryable errors
 
        return None

    
    def _is_new_nctid(self, nctid):
        """
        Securely and efficiently checks if an nctid exists in the database.
        Returns True if the nctid is new (does not exist), False otherwise.
        """
        # 1. Use a parameterized query to prevent SQL injection.
        # 2. Select a constant '1' for maximum efficiency.
        check_sql = f"SELECT 1 FROM {self.table_name} WHERE nctid = %s"
        
        # 3. Use a 'with' statement to ensure the cursor is always closed.
        with self.mysql.cursor() as cursor:
            # Pass the parameters separately from the query string.
            cursor.execute(check_sql, (nctid,))
            
            # 4. Fetch only one row. It's all we need.
            result = cursor.fetchone()
        
        # fetchone() returns None if no row is found. So, if result is None, the nctid is new.
        return result is None



    def _save_new(self, gardId, name, nctid, initial_query): 

        # 6.
        insert_into_clinical_trial_unique_table = f"INSERT INTO {self.table_name} (nctid, studies, is_new) values (%s, %s, %s) " 
        insert_into_clinical_trial_table = "INSERT INTO clinical_trial (gardId, disease, nctid, studies, url, is_new) VALUES (%s, %s, %s, %s, %s, %s)"

        studies_json = self._fetch_clinical_trial_details(nctid)

        # 6.1
        if studies_json is not None:
            try:
                # The 'with' statement ensures the cursor is always closed automatically
                with self.mysql.cursor() as cursor:

                    studies = json.dumps(studies_json)

                    val1 = (nctid, studies, 1)
                    val2 = (gardId, name, nctid, studies, initial_query, 1)
                    
                    cursor.execute(insert_into_clinical_trial_unique_table, val1)
                    cursor.execute(insert_into_clinical_trial_table, val2)
                
                # If the 'with' block completes without errors, commit the transaction
                self.mysql.commit()
                self.appender.log_stdout(f'\t{Fore.GREEN}# Added new NCTID: {nctid} for: {gardId}{Style.RESET_ALL}')

            except Exception as e:
                # If an error occurred inside the 'try' block, roll back
                self.appender.log_stdout(f"An error occurred, rolling back transaction: {Fore.RED}{e}{Style.RESET_ALL}")
                if self.mysql.is_connected():
                    self.mysql.rollback()
                #raise


     
    def _save_new_clinical_trail_to_database(self, gardId, names):
        """
        Generate and store clinical trial data for a GARD ID and its associated names.        
        Args:
            gardId (str): GARD identifier
            names (list): List of disease names and synonyms
        """
        count = 0

        # 5.
        for name in names:
            # Escape quotes in disease name
            name = name.replace('"', '\"')

            # Build API query
            # Documentation: https://clinicaltrials.gov/find-studies/constructing-complex-search-queries
            initial_query = (
                f'https://clinicaltrials.gov/api/v2/studies?'
                f'query.cond=(EXPANSION[Term]{name} '
                f'OR AREA[DetailedDescription]EXPANSION[Term]{name} '
                f'OR AREA[BriefSummary]EXPANSION[Term]{name}) '
                f'AND AREA[LastUpdatePostDate]RANGE[{self.last_update},MAX]'
                f'&fields=NCTId&pageSize=1000&countTotal=true'
            )
            # https://clinicaltrials.gov/api/v2/studies?query.cond=(EXPANSION[Term]alternating hemiplegia of childhood OR AREA[DetailedDescription]EXPANSION[Term]alternating hemiplegia of childhood OR AREA[BriefSummary]EXPANSION[Term]alternating hemiplegia of childhood) AND AREA[LastUpdatePostDate]RANGE[01/01/2020,MAX]&fields=NCTId&pageSize=1000&countTotal=true
             
            try:
                pageToken = None
               
                # 5.1
                while True:
                    response_txt = self._call_get_nctids(initial_query, pageToken=pageToken)
                    
                    # 5.2
                    if response_txt is None:
                        self.appender.log_stdout(f"\tFailed to get trials for - {Fore.RED}{name}{Style.RESET_ALL}")
                        break
                    
                    # 5.3
                    trials_list = response_txt.get('studies', None)

                    if not trials_list:
                        self.appender.log_stdout(f"\tNo trials found for - {Fore.RED}{name}{Style.RESET_ALL}")
                        break

                    else:
                        # 5.4
                        for trial in trials_list:
                            nctid = trial['protocolSection']['identificationModule']['nctId']
                            
                            # 5.5 check
                            is_new_clinical_trial = self._is_new_nctid(nctid)

                            if not is_new_clinical_trial:
                                self.appender.log_stdout(f'\tNCTID: {nctid} is already in database')
                                break
                            
                            # 6. save
                            self._save_new(gardId, name, nctid, initial_query)
                            count += 1
                            
                        # Check for pagination
                        if 'nextPageToken' not in response_txt:
                            break
                        else:
                            pageToken = response_txt['nextPageToken']
               
            except Exception as e:
                self.appender.log_stdout(f"\tError processing - {name}: {Fore.RED}{e}{Style.RESET_ALL}")

        return count
 

    def do_clinical_trial_update(self, gard_nodes_generator):
        
        total = 0

        # 4
        for node in gard_nodes_generator:
            
            # 4.1 Extract node information
            name = node['gardName']
            gid = node['gardId']
            syns = node.get('synonyms', [])
         
            # 4.2 Filter synonyms
            syn_list = [syn for syn in syns if syn not in exclude_words]
            syns_eng = [syn for syn in syn_list if _is_english(syn)]
            filtered_syns = [syn for syn in syns_eng if _len_greater_than_threshold(syn, 4)] 
            names = [name] + filtered_syns
        
            self.appender.log_stdout(f'Processing GARD ID: {gid}')

            # 5. Generate the nctId list by the names
            count =self._save_new_clinical_trail_to_database(gid, names)
            total += count
  
            # commit
            self.mysql.commit()

        return total
            

    # Overwrite
    def update(self):

        total = 0

        # 1. Count how many nodes of GARD in Memgraph database
        # 1.1 The execute_and_fetch method returns a generator
        results_generator = self.memgraph.execute_and_fetch('MATCH (x:GARD) RETURN COUNT(x) AS COUNT')

        # 1.2 Use next() to get the first (and only) result from the generator
        first_result = next(results_generator)

        # 1.3 Access the dictionary item as you intended
        gard_count = first_result['COUNT']
        self.appender.log_stdout(f'There are {Fore.RED}{gard_count}{Style.RESET_ALL} GARD nodes in Memgraph database')

        # 2. Batch fetch GARD nodes, batch size = 100
        # Creates ranges: (0-100), (100-200), (200-300), etc.
        for start, end in zip(range(0, gard_count, 100), range(100, gard_count + 1, 100)):

            self.appender.log_stdout(f"\n {Fore.BLUE}--- Processing records {start} to {end} ---{Style.RESET_ALL}")

            # 3. Fetch the GARD nodes
            query = f'MATCH (x:GARD) RETURN x.gardId AS gardId, x.gardName AS gardName, x.synonyms AS synonyms ORDER BY x.gardId ASC SKIP {start} LIMIT {end - start}'
            
            # 3.1 execute_and_fetch returns a generator
            gard_nodes_generator = self.memgraph.execute_and_fetch(query)

            # 4
            count = self.do_clinical_trial_update(gard_nodes_generator)
            total += count

        self.appender.log_stdout(f'\n{"*"*27} Total new clinical trials: {Fore.GREEN}{total}{Style.RESET_ALL} inserted to the database {"*"*27} ')


    def __enter__(self):
        """Context manager entry."""
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):        
        """Context manager exit - ensure database connection is closed."""
        if hasattr(self, 'mysqldb') and self.mysqldb:
            self.mysqldb.commit()
            self.mysqldb.close()
            self.appender.log_stdout("Database connection closed")



if __name__ == '__main__':

    ok = ask_to_continue('Update clinical trial data from ClinicalTrials.gov API for GARD disease nodes?')

    if not ok:
        sys.exit(f'{Fore.RED}------Stopped------{Style.RESET_ALL}')
 
    # Use context manager to ensure proper cleanup
    with ClinicalTrialDataUpdater() as updater:
        updater.update()
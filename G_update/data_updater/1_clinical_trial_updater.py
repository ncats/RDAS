import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

#
import mysql.connector
#
import json
import time
import requests
from utils.conn import DBConnection as db
from utils.tools import _is_english, _is_under_char_threshold, ask_to_continue, _len_greater_than_threshold
from utils.applogger import AppLogger
logger = AppLogger().get_logger()
from utils.quality import exclude_words

import os
import sys
# Add the project root to the Python path
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
    os.path.abspath(os.path.join(_dir, '../..'))
]) 

import json
import time
import hashlib
from dotenv import load_dotenv
load_dotenv() 
from utils.https_request import HTTPSUtils as HttpsUtil
from concurrent.futures import ThreadPoolExecutor
from utils.tools import ask_to_continue, elapsed_time, _to_txt, _date_string

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from baseclass.init_base import InitBase
from utils.file_appender import FileAppender 
 


class ClinicalTrialUpdater(InitBase):
 
    def __init__(self):

        super().__init__('clinical_trial', 'ClinicalTrialUpdater')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/G-1-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    # 1. Get all GARD nodes (simple)
    def get_gard_nodes(self):
         
        get_all_gard_nodes = 'MATCH (x:GARD) RETURN x.GardId AS GardId, x.Name AS Name, x.Synonyms AS Synonyms ORDER BY x.GardId ASC'

        try:
            gard_nodes = self.memgraph.execute_and_fetch(get_all_gard_nodes)
            return gard_nodes
            
        except Exception as e:
            self.appender.log_stdout(f"An error occurred while executing the query: {e}")
            return None


    def call_get_nctids (self, query, pageToken=None):
        try:
            if pageToken: 
                query += f'&pageToken={pageToken}'
            
            #url_logger.info(query)
            response = requests.get(query)
            response_txt = response.json()

        except Exception as e:
            print(f'Unable to Process Query: {query}\n{e}') 
            response_txt = None

        return response_txt

 
     
    def _generate_GARD_ID_and_nctId(self, gardId, names):
        
        mycursor = self.mysqldb.cursor()

        for name in names:
            #
            # Check the name like: 
            # GARD:0000536	Acute myeloid leukemia with abnormal bone marrow eosinophils inv(16)(p13q22) or t(16;16)(p13;q22)
            # GARD:0000538	AML with t(15;17)(q22;q12);(PML/RARalpha) and variants
            #
            
            nctid_list = list()
            name = name.replace('"','\"')

            # Documentation: https://clinicaltrials.gov/find-studies/constructing-complex-search-queries
            initial_query = f'https://clinicaltrials.gov/api/v2/studies?query.cond=(EXPANSION[Term]{name} OR AREA[DetailedDescription]EXPANSION[Term]{name} OR AREA[BriefSummary]EXPANSION[Term]{name}) AND AREA[LastUpdatePostDate]RANGE[{self.last_update},MAX]&fields=NCTId&pageSize=1000&countTotal=true'
            
            sql = "INSERT INTO clinical_trial (gardId, disease, nctid, studies, url) VALUES (%s, %s, %s, %s, %s)"
            
            try:
                pageToken = None
               
                while True:
                    response_txt = self.call_get_nctids(initial_query, pageToken=pageToken)
                    #response_txt example:
                    '''
                    {
                        "totalCount":3,
                        "studies":[
                            {"protocolSection":{"identificationModule":{"nctId":"NCT06098430"}}},
                            {"protocolSection":{"identificationModule":{"nctId":"NCT05886036"}}},
                            {"protocolSection":{"identificationModule":{"nctId":"NCT06294652"}}}
                        ]
                    }
                    '''                    
                    trials_list = response_txt['studies']
                   
                    if trials_list:
                        for trial in trials_list:

                            nctid = trial['protocolSection']['identificationModule']['nctId']
                           
                            # Initialize retry counter
                            retries = 0
                            response_txt = None
                            max_retries=10

                            while retries < max_retries:
                                try:
                                    response = requests.get(f'https://clinicaltrials.gov/api/v2/studies/{nctid}', timeout=10)
                                    response.raise_for_status()  # Raise an exception for HTTP errors (4xx and 5xx)

                                    # Parse JSON response
                                    response_txt = response.json()
                                    break  # Exit the loop if successful

                                except requests.exceptions.Timeout:
                                    print(f"Timeout occurred for {nctid}, retrying...")
                                    retries += 1
                                    time.sleep(1)
                                except requests.exceptions.RequestException as e:
                                    print(f"Request failed for {nctid}: {e}")
                                    break  # Exit the loop for non-retryable errors

                            if response_txt is not None:
                               
                                try:
                                    val = (gardId, name, nctid, json.dumps(response_txt), initial_query)
                                    mycursor.execute(sql, val)
                                    #db.commit()

                                    print(f'Add : {nctid} for: {gardId}')

                                except mysql.connector.Error as error:
                                    print(f"Failed to insert record into table: {error}")
  
                        if not 'nextPageToken' in response_txt:
                            break
                        else:
                            pageToken = response_txt['nextPageToken']
                    else: 
                        #print(f'No nctid for: {gardId}')
                        val = (gardId, name, None, None, initial_query)
                        mycursor.execute(sql, val)

                        break
               
            except Exception as e:
                print(e)


    def do_clinic_trial_update(self, gard_node, db):

        # 1.
        name = gard_node['Name']
        gid = gard_node['GardId']
        syns = gard_node['Synonyms'] 
         
        # 2.
        syn_list =[syn for syn in syns if syn not in exclude_words]
        syns_eng = [syn for syn in syn_list if _is_english(syn)]
        filtered_syns = [syn for syn in syns_eng if _len_greater_than_threshold(syn, 4)] 
        names = [name] + filtered_syns # names list

        ### 
        # 3. All clinical trials of a GARD node
        self._generate_GARD_ID_and_nctId(gid, names, db)

        


    def update(self):

        # 1. Get all GARD nodes (simple)
        gard_nodes = self.get_gard_nodes()

        # Since gard_nodes seems to be a generator directly, convert it to a list
        gard_records = list(gard_nodes)

        # Slice the list
        #sliced_records = gard_records[self.start:self.end]

        '''For avoiding the error, don't slice the list'''
        sliced_records = gard_records

        # 2.       
        for idx, gardNode in enumerate(sliced_records):
 

            gardId = gardNode['GardId']                
            #print(f"({self.start}-{self.end}): Index: {self.start + idx}, gardId: {gardId}")     

            self.do_clinic_trial_update(gardNode)

            if idx % 100 == 0:
                self.mysqldb.commit()

        self.mysqldb.commit()
      


if __name__ == '__main__':

    ok = ask_to_continue('Fetch and Store Clinical-Trials into  MySQL database?')
    if not ok:
        sys.exit('------Stopped ------')

    # Total in memgraph database is: 15,323
    initlzr = ClinicalTrialInitializer(0, 15500)

    initlzr.do_init()

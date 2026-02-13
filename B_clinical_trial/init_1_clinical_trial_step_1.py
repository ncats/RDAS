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
from utils.tools import _is_english, _is_under_char_threshold, ask_to_continue
from utils.applogger import AppLogger
logger = AppLogger().get_logger()

#MySQL
""" 
# Step 1: Fetch and Store Clinical-Trials into  MySQL database
# (Step 2: See init_clinical_trial_step_2.py)
#
# conda activate rds
# python clinical_trial/init_1_clinical_trial_step_1.py  
#
"""

class ClinicalTrialInitializer:
 
    """
        SELECT count(distinct gardid, nctid) FROM rdas.clinical_trial;

       SELECT  gardId, 
            GROUP_CONCAT(nctid ORDER BY nctid ASC SEPARATOR ';') AS nctids
        FROM  rdas.gard_nctid GROUP BY  gardId;

        # By nctid
        select nctid, count(*) as cnt, group_concat(gardId) from rdas.clinical_trial where nctid is not null group by nctid order by cnt desc;

        # By gardId
        select gardid, count(*) as cnt, group_concat(nctid) from rdas.clinical_trial where nctid is not null group by gardid order by cnt desc;
    """

    def __init__(self, start, end):

        self.start = start
        self.end = end
        self.records =[]

        self.last_update= '01/01/1970'
        print(f'\n### Last update: {self.last_update} ###\n')

        self.conn = db().memgraph_conn()
        if self.conn is None:
            raise ConnectionError("Failed to connect to Memgraph") 
        
        self.mysqldb = db().mysql_conn() 


    def get_gard_nodes(self):
        # 1. Get all GARD nodes (simple)        
        get_all_gard_nodes = 'MATCH (x:GARD) RETURN x.GardId AS GardId, x.Name AS Name, x.Synonyms AS Synonyms ORDER BY x.GardId ASC'

        try:
            gard_nodes = self.conn.execute_and_fetch(get_all_gard_nodes)
            if gard_nodes is None:
                logger.error("\n\n*** Query returned None, stop here ***\n\n")
                exit(1)
            else:
                return gard_nodes
            
        except Exception as e:
            print(f"An error occurred while executing the query: {e}")
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

 
    
    # For testing
    def _generate_GARD_ID_and_nctId(self, gardId, names, db):
        
        mycursor = self.mysqldb.cursor()

        for name in names:

            #
            # Check the name like: 
            # GARD:0000536	Acute myeloid leukemia with abnormal bone marrow eosinophils inv(16)(p13q22) or t(16;16)(p13;q22)
            # GARD:0000538	AML with t(15;17)(q22;q12);(PML/RARalpha) and variants
            #
            
            nctid_list = list()
            name = name.replace('"','\"')

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

        db.commit()


    def do_clinic_trial_update(self, gard_node, db):

        # 1.
        name = gard_node['Name']
        gid = gard_node['GardId']
        syns = gard_node['Synonyms'] 
         
       
        # 2.
        gardsyns_eng = [syn for syn in syns if _is_english(syn)]
        gardsyns_char_threshold = [syn for syn in syns if _is_under_char_threshold(syn)]

        filtered_syns = [x for x in syns if not x in gardsyns_eng]
        filtered_syns = [x for x in filtered_syns if not x in gardsyns_char_threshold]

        ''' ??? Is this logic correct ??? '''
        ''' names = [primary Name] + filtered_non_English_long_synonyms '''
        names = [name] + filtered_syns # names list

        ### 
        # 3. All clinical trials of a GARD node
        self._generate_GARD_ID_and_nctId(gid, names, db)

        


    def do_init(self):

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

            self.do_clinic_trial_update(gardNode, self.mysqldb)

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

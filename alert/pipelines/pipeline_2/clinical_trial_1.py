import os
import sys
import json
import time
import requests
import mysql.connector

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

class ClinicalTrialPipeline_1(PipelineBase):

    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=True)

        self.LOOKBACK_DAYS =7


    # Not implemented
    def process_new_data(self) -> None:
        raise NotImplementedError("ClinicalTrialPipeline_1 does not implement process_new_data().")
    

    def find_new_data(self, gard_node) -> None:

        gid = gard_node['gardId']
        names = gard_node['filtered_names']
        last_update_date = gard_node.get("updated")

        self._generate_GARD_ID_and_nctId(gid, names, last_update_date)



    def _generate_GARD_ID_and_nctId(self, gardId, names, last_update_date):
        
        mycursor = self.mysql.cursor() 

        for name in names:
            #
            # Check the name like: 
            # GARD:0000536	Acute myeloid leukemia with abnormal bone marrow eosinophils inv(16)(p13q22) or t(16;16)(p13;q22)
            # GARD:0000538	AML with t(15;17)(q22;q12);(PML/RARalpha) and variants
            #
            
            nctid_list = list()
            name = name.replace('"','\"')

            #print(f'Get nctid for: {name}')

            initial_query = f'https://clinicaltrials.gov/api/v2/studies?query.cond=(EXPANSION[Term]{name} OR AREA[DetailedDescription]EXPANSION[Term]{name} OR AREA[BriefSummary]EXPANSION[Term]{name}) AND AREA[LastUpdatePostDate]RANGE[{last_update_date},MAX]&fields=NCTId&pageSize=1000&countTotal=true'
            
            insert_sql = """
                INSERT INTO update_clinical_trial (gardId, disease, nctid, studies, url)
                SELECT %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM clinical_trial ct
                    WHERE ct.nctid = %s
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM update_clinical_trial uct
                    WHERE uct.nctid = %s
                )
            """
            
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
                                    val = (
                                        gardId,
                                        name,
                                        nctid,
                                        json.dumps(response_txt),
                                        initial_query,
                                        nctid,
                                        nctid,
                                    )

                                    mycursor.execute(insert_sql, val)
                                    
                                    if mycursor.rowcount == 1:
                                        #print(initial_query)

                                        self.appender.log_stdout(f"New nctid added: {nctid} for: {gardId}")
                                        self.mysql.commit() 

                                except mysql.connector.Error as error:
                                    print(f"Failed to insert record into table: {error}")
  
                        if not 'nextPageToken' in response_txt:
                            break
                        else:
                            pageToken = response_txt['nextPageToken']
                    
                    else: 
                        #print(f'No nctid for: {gardId}')
                        #val = (gardId, name, None, None, initial_query)
                        #mycursor.execute(insert_sql, val)

                        break 
               
            except Exception as e:
                print(e)

        self.mysql.commit()


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

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

"""
Find newly updated clinical trials for GARD diseases.

For each updated GARD node, this pipeline searches ClinicalTrials.gov with the
node's filtered disease names and last update date. It fetches each matching
study by NCT ID, then stores only trials that are not already present in
clinical_trial. New rows are marked with is_new = 1 for later pipeline steps.
"""
# Reference: B_clinical_trial/init_1_clinical_trial_step_1.py

class NewClinicalTrialDiscoveryTask(PipelineBase):

    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=False)

        self.LOOKBACK_DAYS =7
        self.clinical_trials_studies_api = (
            os.getenv("CLINICAL_TRIAL_STUDIES_API")
            or os.getenv("CLINICAL_TRAIL_STUDY_URL")
        )


    # Not implemented
    def process_new_data(self) -> None:
        raise NotImplementedError("NewClinicalTrialDiscoveryTask does not implement process_new_data().")


    def find_new_data(self, gard_node) -> None:

        # Search ClinicalTrials.gov using each filtered disease name for this GARD node.
        gid = gard_node['gardId']
        names = gard_node['filtered_names']
        last_update_date = gard_node.get("updated")

        self._generate_GARD_ID_and_nctId(gid, names, last_update_date)



    def _generate_GARD_ID_and_nctId(self, gardId, names, last_update_date):

        if not self.clinical_trials_studies_api:
            self.logger.error("CLINICAL_TRIAL_STUDIES_API is not configured.")
            return

        studies_api = self.clinical_trials_studies_api.rstrip("/")

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

            # Search for studies whose condition, detailed description, or brief summary
            # match this disease name and whose last update is newer than the GARD update.
            initial_query = f'{studies_api}?query.cond=(EXPANSION[Term]{name} OR AREA[DetailedDescription]EXPANSION[Term]{name} OR AREA[BriefSummary]EXPANSION[Term]{name}) AND AREA[LastUpdatePostDate]RANGE[{last_update_date},MAX]&fields=NCTId&pageSize=1000&countTotal=true'

            # Insert each new NCT ID only if it is absent from clinical_trial.
            # clinical_trial.is_new defaults to 0, so new discovery rows set it
            # explicitly to 1 for the downstream alert workflow.
            insert_sql = """
                INSERT INTO clinical_trial (gardId, disease, nctid, studies, url, is_new)
                SELECT %s, %s, %s, %s, %s, 1
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM clinical_trial ct
                    WHERE ct.nctid = %s
                )
            """

            try:
                pageToken = None
                current_nctid = None
                current_stage = "fetch_nct_ids"
                response_txt = None

                while True:
                    current_stage = "fetch_nct_ids"
                    current_nctid = None
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

                            current_stage = "read_nct_id"
                            nctid = trial['protocolSection']['identificationModule']['nctId']
                            current_nctid = nctid

                            # Fetch the full ClinicalTrials.gov study JSON for the NCT ID.
                            current_stage = "fetch_study_details"
                            retries = 0
                            response_txt = None
                            max_retries=10

                            while retries < max_retries:
                                try:
                                    response = requests.get(f'{studies_api}/{nctid}', timeout=10)

                                    if response.status_code >= 400:
                                        print(f"Request failed for {nctid}: status={response.status_code}")
                                        break

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
                                    current_stage = "insert_study"
                                    val = (
                                        gardId,
                                        name,
                                        nctid,
                                        json.dumps(response_txt),
                                        initial_query,
                                        nctid,
                                    )

                                    mycursor.execute(insert_sql, val)

                                    if mycursor.rowcount == 1:
                                        #print(initial_query)

                                        self.logger.info(f"New nctid added: {nctid} for: {gardId}")
                                        self.mysql.commit()

                                except mysql.connector.Error as error:
                                    print(f"Failed to insert record into table: {error}")

                        current_stage = "paginate_nct_ids"
                        if not 'nextPageToken' in response_txt:
                            break
                        else:
                            pageToken = response_txt['nextPageToken']

                    else:
                        #self.logger.info(f'No new NCTIDs found for: {gardId}')
                        break

            except Exception as e:
                response_type = type(response_txt).__name__
                response_keys = list(response_txt.keys())[:10] if isinstance(response_txt, dict) else None
                self.logger.error(
                    f"Error discovering clinical trials for gardId={gardId}, "
                    f"disease={name[:200]}, last_update_date={last_update_date}, "
                    f"stage={current_stage}, nctid={current_nctid}, pageToken={pageToken}, "
                    f"response_type={response_type}, response_keys={response_keys}, "
                    f"query={initial_query}: {e}",
                    exc_info=True,
                )

        self.mysql.commit()


    def call_get_nctids (self, query, pageToken=None):
        try:
            if pageToken:
                query += f'&pageToken={pageToken}'

            # Return a page of matching NCT IDs from the ClinicalTrials.gov search API.
            #url_logger.info(query)
            response = requests.get(query)
            response_txt = response.json()

        except Exception as e:
            print(f'Unable to Process Query: {query}\n{e}')
            response_txt = None

        return response_txt

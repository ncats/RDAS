import os
import sys
import time
import requests

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from pipelines.pipeline_2_clinical_trial.clinical_trial_study_change_handler import ClinicalTrialStudyChangeHandler

"""
Find newly updated clinical trials for GARD diseases.

For each updated GARD node, this pipeline searches ClinicalTrials.gov with the
node's filtered disease names and last update date. It fetches each matching NCT
ID, inserts brand-new studies, and marks changed existing studies as is_new = 1
so the later clinical-trial pipeline steps can process them.
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
        self.study_change_handler = ClinicalTrialStudyChangeHandler(self.mysql, self.logger)


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

        for name in names:
            '''
            Check the name like:
            GARD:0000536 Acute myeloid leukemia with abnormal bone marrow eosinophils inv(16)(p13q22) or t(16;16)(p13;q22)
            GARD:0000538 AML with t(15;17)(q22;q12);(PML/RARalpha) and variants
            '''

            name = name.replace('"','\"')

            #print(f'Get nctid for: {name}')

            '''
            Search for studies whose condition, detailed description, or brief
            summary match this disease name and whose last update is newer than
            the GARD update.
            '''
            initial_query = f'{studies_api}?query.cond=(EXPANSION[Term]{name} OR AREA[DetailedDescription]EXPANSION[Term]{name} OR AREA[BriefSummary]EXPANSION[Term]{name}) AND AREA[LastUpdatePostDate]RANGE[{last_update_date},MAX]&fields=NCTId&pageSize=1000&countTotal=true'

            try:
                pageToken = None
                current_nctid = None
                current_stage = "fetch_nct_ids"
                search_response = None
                study_response = None

                while True:
                    current_stage = "fetch_nct_ids"
                    current_nctid = None
                    search_response = self.call_get_nctids(initial_query, pageToken=pageToken)
                    # search_response example:
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
                    if not search_response:
                        self.logger.error(f"No ClinicalTrials.gov search response for gardId={gardId}, disease={name[:200]}")
                        break

                    trials_list = search_response.get('studies') or []

                    if trials_list:

                        for trial in trials_list:

                            current_stage = "read_nct_id"
                            nctid = trial['protocolSection']['identificationModule']['nctId']
                            current_nctid = nctid

                            # Fetch the full ClinicalTrials.gov study JSON for the NCT ID.
                            current_stage = "fetch_study_details"
                            study_response = self._fetch_study_by_nctid(studies_api, nctid)

                            if study_response is not None:

                                try:
                                    current_stage = "save_or_update_study"
                                    result = self.study_change_handler.save_or_update_study(gardId, name, nctid, study_response, initial_query)

                                    if result.get("action") == "updated":
                                        self.logger.info(f"Clinical trial study JSON changed for NCTID={nctid}; differences={result.get('differences')}")

                                except Exception as error:
                                    self.logger.error(f"Failed to save or update clinical trial study for nctid={nctid}: {error}", exc_info=True)

                            else:
                                self.logger.error(f"No ClinicalTrials.gov study detail response for nctid={nctid}")

                        current_stage = "paginate_nct_ids"
                        if not 'nextPageToken' in search_response:
                            break
                        else:
                            pageToken = search_response['nextPageToken']

                    else:
                        #self.logger.info(f'No new NCTIDs found for: {gardId}')
                        break

            except Exception as e:
                response_type = type(search_response).__name__
                response_keys = list(search_response.keys())[:10] if isinstance(search_response, dict) else None
                self.logger.error(
                    f"Error discovering clinical trials for gardId={gardId}, "
                    f"disease={name[:200]}, last_update_date={last_update_date}, "
                    f"stage={current_stage}, nctid={current_nctid}, pageToken={pageToken}, "
                    f"response_type={response_type}, response_keys={response_keys}, "
                    f"query={initial_query}: {e}",
                    exc_info=True,
                )

        self.mysql.commit()


    def _fetch_study_by_nctid(self, studies_api, nctid):

        '''
        Fetch the full ClinicalTrials.gov study JSON for one NCT ID.

        The search endpoint returns only the matching NCT IDs. The pipeline needs
        the full study response so ClinicalTrialStudyChangeHandler can compare
        the fetched study against clinical_trial_unique.studies.
        '''
        retries = 0
        max_retries = 10

        while retries < max_retries:
            try:
                response = requests.get(f'{studies_api}/{nctid}', timeout=10)

                if response.status_code >= 400:
                    print(f"Request failed for {nctid}: status={response.status_code}")
                    break

                return response.json()

            except requests.exceptions.Timeout:
                print(f"Timeout occurred for {nctid}, retrying...")
                retries += 1
                time.sleep(1)

            except requests.exceptions.RequestException as e:
                print(f"Request failed for {nctid}: {e}")
                break

        return None


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

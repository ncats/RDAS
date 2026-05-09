import os
import sys
import json
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

import re
import time
import requests
import spacy
from spacy.matcher import Matcher
# Setup NLP for RxNORM Mapping
nlp = spacy.load('en_ner_bc5cdr_md')
pattern = [{'ENT_TYPE':'CHEMICAL'}]
matcher = Matcher(nlp.vocab)
matcher.add('DRUG',[pattern])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _clean

"""
Store Clinical-Trial Intervertion-Drug properties into clinical_trial_intervention_drug table
"""
# Reference: B_clinical_trial/init_3_clinical_trial_step_3.py

class ClinicalTrialDrugInterventionMappingTask(PipelineBase):

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)
        self.rxnav_rxcui_api = os.getenv("RXNAV_RXCUI_API")
        self.rxnav_all_properties_api_template = os.getenv("RXNAV_ALL_PROPERTIES_API_TEMPLATE")


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("ClinicalTrialDrugInterventionMappingTask does not implement find_new_data().")


    def process_new_data(self) -> None:

        select_new_clinic_trial_sql = '''
            SELECT gardid, disease, nctid, studies, id
            FROM clinical_trial
            WHERE nctid IS NOT NULL
            AND is_new = 1
        '''

        batch_num = 0
        batch_size = 100

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(select_new_clinic_trial_sql)

            while True:

                batch_num += 1
                rows = fetch_cursor.fetchmany(batch_size)

                if not rows:
                    self.logger.info(f"No more rows to fetch.")
                    break

                self.logger.info(f'\n--- batch# = {batch_num} ---')

                for row in rows:
                    gardid = row['gardid']
                    disease = row['disease']
                    nctid = row['nctid']
                    study = json.loads(row['studies'])
                    id = row['id']

                    self.logger.info(f"# Id: {id}, Gard_ID: {gardid}, NCTID: {nctid}, Disease: {disease}")

                    intervention_module = study.get('protocolSection', dict()).get('armsInterventionsModule', dict())
                    interventions = intervention_module.get('interventions', list())

                    if interventions == list(): #is empty
                        continue

                    for intervention in interventions:

                        intervention_name = _clean(intervention.get('name',''))
                        intervention_type = _clean(intervention.get('type',''))

                        if intervention_type == 'DRUG':
                            self.rxnorm_map(gardid, disease, nctid, intervention_name)


            fetch_cursor.close()

        except Exception as err:
            self.logger.error(f"Error: {err}")

        finally:
            # close all connections
            self.close()



    def rxnorm_map(self, gardid, disease, nctid, intervention_name):

        cursor = self.mysql.cursor()

        sql = '''
            INSERT INTO clinical_trial_intervention_drug (gardId, disease, nctid, rxnormid, intervention, drug_name, wspacy, property_key, property_val, is_new)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        def add_to_db(rxdata, intervention, drug_name, wspacy):

            rxnormid = rxdata['RxNormID']

            ''' Set additional properties on the Drug node '''
            for property_key, property_val in rxdata.items():

                property_key = property_key.replace(' ','')

                if isinstance(property_val, list):
                    property_val = json.dumps(property_val)

                print(f'{gardid}\t{nctid}\t{rxnormid}\t{property_key}')

                val = (gardid, disease, nctid, rxnormid, intervention, drug_name, wspacy, property_key, property_val, 1)

                cursor.execute(sql, val)


        def nlp_to_drug(intervention, matches):

            for match_id, start, end in matches:
                drug = doc[start:end].text

                ''' Retrieve RxNorm data for the drug name '''
                rxdata = self.get_rxnorm_data(drug.replace(' ','+'))

                if rxdata:
                    ''' Create connections in the database using RxNorm data '''
                    add_to_db(rxdata, intervention, drug, wspacy=1)

                else:
                    self.logger.error(f'\t\tMap to RxNorm failed for intervention name:{intervention}, drug name: {drug}')


        def drug_normalize(drug_name):

            ''' Remove non-ASCII characters '''
            new_val = drug_name.encode("ascii", "ignore")
            ''' Decode the bytes to string '''
            updated_str = new_val.decode()
            ''' Replace non-word characters with spaces '''
            updated_str = re.sub('\W+',' ', updated_str)
            return updated_str

        # -----------------------------------------------------------------------------------------------------------------------------------------------------

        drug = drug_normalize(intervention_name)
        the_drug = drug.replace(' ','+')

        '''  Retrieve RxNorm data for the drug name '''
        rxdata = self.get_rxnorm_data(the_drug)

        if rxdata:
            self.logger.info(f'\t\tSave to database :: Drug: {the_drug}, rxdata.RxNormID = {rxdata["RxNormID"]}')
            ''' Create connections in the database using RxNorm data '''
            add_to_db(rxdata, intervention_name, drug, 0)

        else:
            # If RxNorm data not found, use SpaCy NLP to detect drug names and map to RxNorm
            doc = nlp(drug)
            matches = matcher(doc)

            nlp_to_drug(intervention_name, matches)

        cursor.close()
        self.mysql.commit()




    def get_rxnorm_data(self, drug_name):

            if not self.rxnav_rxcui_api or not self.rxnav_all_properties_api_template:
                self.logger.error("RXNAV_RXCUI_API or RXNAV_ALL_PROPERTIES_API_TEMPLATE is not configured.")
                return None

            ''' Initialize retry counter '''
            retries = 0
            rxnormid = None
            max_retries=10

            rxdata = dict()
            while retries < max_retries:
                try:
                    ''' Form RxNav API request to get RxNormID based on drug name '''
                    rq = f'{self.rxnav_rxcui_api}?name={drug_name}&search=2'
                    response = requests.get(rq)

                    if response.status_code >= 400:
                        self.logger.error(f"RxNav request failed: status={response.status_code}, url={rq}")
                        break

                    ''' Extract RxNormID from the response '''
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

                    ''' Form RxNav API request to get all properties of the drug using RxNormID '''
                    rq2 = f'{self.rxnav_all_properties_api_template.format(rxnormid=rxnormid)}?prop=codes+attributes+names+sources'
                    response = requests.get(rq2)
                    results = response.json()['propConceptGroup']['propConcept']

                    ''' Extract and organize properties of the drug '''
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

import os
import sys
import json
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

import re
import spacy
from spacy.matcher import Matcher
# Setup NLP for RxNORM Mapping
nlp = spacy.load('en_ner_bc5cdr_md')
pattern = [{'ENT_TYPE':'CHEMICAL'}]
matcher = Matcher(nlp.vocab)
matcher.add('DRUG',[pattern])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _clean
from utils.https_request import HTTPSUtils

"""
Store Clinical-Trial Intervertion-Drug properties into clinical_trial_intervention_drug table
"""
# Reference: B_clinical_trial/init_3_clinical_trial_step_3.py

class ClinicalTrialDrugInterventionMappingTask(PipelineBase):
    """
    Map new clinical-trial drug interventions to RxNorm properties.

    The task reads newly imported clinical trials, extracts interventions whose
    type is DRUG, looks them up through RxNav, and stores the resulting RxNorm
    IDs/properties in clinical_trial_intervention_drug for later graph loading.
    """

    def __init__(self):

        """Initialize MySQL access and RxNav endpoint configuration."""

        super().__init__(init_mysql=True, init_memgraph=False)
        self.rxnav_rxcui_api = os.getenv("RXNAV_RXCUI_API")
        self.rxnav_all_properties_api_template = os.getenv("RXNAV_ALL_PROPERTIES_API_TEMPLATE")


    # Not implemented
    def find_new_data(self, gard_node) -> None:

        raise NotImplementedError("ClinicalTrialDrugInterventionMappingTask does not implement find_new_data().")


    def process_new_data(self) -> None:

        """Process new clinical trials and map each drug intervention to RxNorm."""

        '''
        Only newly discovered or changed clinical trials need drug-intervention
        extraction in this incremental alert pipeline.
        '''
        select_new_clinic_trial_sql = '''
            SELECT gardId AS gardid, disease, nctid, studies, id
            FROM clinical_trial
            WHERE nctid IS NOT NULL
            AND is_new = 1
        '''

        '''
        Clear the current-run marker from old derived RxNorm rows before
        remapping changed studies. The rows remain available as history, but the
        graph task will only consume mappings inserted by this run with is_new=1.
        '''
        reset_current_drug_mapping_sql = '''
            UPDATE clinical_trial_intervention_drug AS cid
            INNER JOIN clinical_trial AS ct
                ON ct.gardId = cid.gardId
                AND ct.disease <=> cid.disease
                AND ct.nctid = cid.nctid
            SET cid.is_new = 0
            WHERE ct.is_new = 1
            AND cid.is_new = 1
        '''

        batch_num = 0
        batch_size = 100

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(reset_current_drug_mapping_sql)
            self.mysql.commit()
            self.logger.info(f"Reset {fetch_cursor.rowcount} existing clinical_trial_intervention_drug rows before current RxNorm remapping.")

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

                    '''
                    ClinicalTrials.gov stores interventions inside the
                    protocolSection. This task only keeps intervention records
                    explicitly marked as DRUG.
                    '''
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
            self.mysql.rollback()

        finally:
            # close all connections
            self.close()



    def rxnorm_map(self, gardid, disease, nctid, intervention_name):

        """
        Resolve one intervention name to RxNorm data and insert property rows.

        The first lookup uses the full normalized intervention name. If that
        does not map cleanly, SpaCy extracts chemical entities and each detected
        drug mention is retried against RxNav.
        """

        cursor = self.mysql.cursor()

        insert_intervention_drug_sql = '''
            INSERT INTO clinical_trial_intervention_drug (gardId, disease, nctid, rxnormid, intervention, drug_name, wspacy, property_key, property_val, is_new)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        def add_to_db(rxdata, intervention, drug_name, wspacy):

            """Insert one row per RxNorm property for a mapped drug."""

            rxnormid = rxdata['RxNormID']

            ''' Set additional properties on the Drug node '''
            for property_key, property_val in rxdata.items():

                property_key = property_key.replace(' ','')

                if isinstance(property_val, list):
                    property_val = json.dumps(property_val)

                self.logger.info(f'{gardid}\t{nctid}\t{rxnormid}\t{property_key}')

                val = (gardid, disease, nctid, rxnormid, intervention, drug_name, wspacy, property_key, property_val, 1)

                cursor.execute(insert_intervention_drug_sql, val)


        def nlp_to_drug(intervention, matches):

            """Use SpaCy chemical entities as fallback drug names."""

            for match_id, start, end in matches:
                drug = doc[start:end].text

                ''' Retrieve RxNorm data for the drug name '''
                rxdata = self.get_rxnorm_data(drug.replace(' ','+'))

                if rxdata:
                    ''' Create connections in the database using RxNorm data '''
                    add_to_db(rxdata, intervention, drug, wspacy=1)

                else:
                    '''
                    An extracted chemical can also have no RxNorm match, for
                    example an investigational drug code. The lookup already
                    logs actual API failures as warnings; an unmapped candidate
                    alone must not fail the GARD node's completion checkpoint.
                    '''
                    self.logger.warning(f'\t\tNo RxNorm mapping for intervention name:{intervention}, drug name: {drug}')


        def drug_normalize(drug_name):

            ''' Remove non-ASCII characters '''
            new_val = drug_name.encode("ascii", "ignore")
            ''' Decode the bytes to string '''
            updated_str = new_val.decode()
            ''' Replace non-word characters with spaces '''
            updated_str = re.sub(r'\W+',' ', updated_str)
            return updated_str

        # -----------------------------------------------------------------------------------------------------------------------------------------------------

        drug = drug_normalize(intervention_name)
        the_drug = drug.replace(' ','+')

        '''
        Try the full intervention name first because it preserves the trial's
        original drug context.
        '''
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

        """
        Resolve a drug through both RxNav endpoints without ending the run.
        A valid empty idGroup is an ordinary no-match, e.g. {"idGroup": {}}.
        An unavailable RxCUI/property request returns None after bounded retries;
        rxnorm_map() can try its existing chemical fallback and other drugs.
        Never insert an invented RxNorm ID or empty properties after an outage.
        """
        if not self.rxnav_rxcui_api or not self.rxnav_all_properties_api_template:
            self.logger.error("RXNAV_RXCUI_API or RXNAV_ALL_PROPERTIES_API_TEMPLATE is not configured.")
            return None

        def parse_rxnorm_ids(response):

            obj = response.json()
            if not isinstance(obj, dict) or "idGroup" not in obj:
                raise ValueError("Expected an RxNav idGroup.")
            group = obj["idGroup"]
            if group is not None and not isinstance(group, dict):
                raise ValueError("RxNav idGroup must be an object or null.")
            identifiers = (group or {}).get("rxnormId") or []
            if not isinstance(identifiers, list) or any(not isinstance(value, str) or not value.strip() for value in identifiers):
                raise ValueError("RxNav returned an invalid RxNorm identifier list.")
            return identifiers

        # Existing callers replace spaces with '+': decode those before params re-encodes them.
        identifiers = HTTPSUtils.with_api_retry_GET(self.rxnav_rxcui_api, parse_rxnorm_ids, params={"name": drug_name.replace("+", " "), "search": 2}, logger=self.logger, description=f"RxNav RxCUI for drug {drug_name}")
        if identifiers is None:
            return None
        if not identifiers:
            self.logger.warning(f"No RxNorm match for drug_name={drug_name}; trying remaining mapping fallbacks when available.")
            return None
        rxnormid = identifiers[0]

        def parse_properties(response):

            results = response.json()["propConceptGroup"]["propConcept"]
            if not isinstance(results, list):
                raise ValueError("RxNav property list is malformed.")
            rxdata = {"RxNormID": rxnormid}
            for result in results:
                prop_name, prop_value = result["propName"], result["propValue"]
                if not isinstance(prop_name, str) or prop_name == "RxNormID":
                    raise ValueError("RxNav returned an invalid property name.")
                rxdata.setdefault(prop_name, []).append(prop_value)
            return rxdata

        properties_url = self.rxnav_all_properties_api_template.format(rxnormid=rxnormid)
        return HTTPSUtils.with_api_retry_GET(properties_url, parse_properties, params={"prop": "codes attributes names sources"}, logger=self.logger, description=f"RxNav properties for {rxnormid}, drug {drug_name}")

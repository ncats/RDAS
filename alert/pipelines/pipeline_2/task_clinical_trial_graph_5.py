import os
import sys
import json
from typing import Any, Dict

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _make_hash_key


"""
Create Drug nodes and Intervention/Drug mappings for new clinical trials.
"""
# Reference: B_clinical_trial/initializer/drug.py


class ClinicalTrialGraphTask_5(PipelineBase):

    BATCH_SIZE = 200

    BATCH_CREATE = '''
        UNWIND $chunks AS chunk

        MERGE (x: Drug {rxnormID: chunk.rxnormID})
        ON CREATE SET
            x = chunk.props

        WITH x, chunk
        OPTIONAL MATCH (y: Intervention {_intervention_name_key: chunk._intervention_name_key})

        WITH x, y, chunk
        WHERE y IS NOT NULL
        MERGE (y)-[:mapped_to_rxnorm {with_spacy: chunk.wspacy}]->(x)
    '''


    '''<=> is MySQL’s null-safe equality operator.
    So: uct.disease <=> cid.disease matches when both diseases are equal, and also matches when both are NULL.
    '''
    FETCH_NEW_DRUG_QUERY = '''
        SELECT
            cid.RxNormID,
            cid.intervention,
            cid.wspacy,
            GROUP_CONCAT(
                DISTINCT CONCAT('"', cid.property_key, '":', cid.property_val)
                ORDER BY cid.property_key, cid.property_val SEPARATOR ','
            ) AS props
        FROM clinical_trial_intervention_drug AS cid
        INNER JOIN update_clinical_trial AS uct
            ON uct.gardId = cid.gardId
            AND uct.disease <=> cid.disease
            AND uct.nctid = cid.nctid
        WHERE uct.is_new = 1
        AND cid.RxNormID IS NOT NULL
        GROUP BY
            cid.RxNormID,
            cid.intervention,
            cid.wspacy
    '''

    KEY_MAP = {
        'ATC': 'atc',
        'AVAILABLE_STRENGTH': 'availableStrength',
        'DRUGBANK': 'drugBank',
        'MMSL_CODE': 'mmslCode',
        'PRESCRIBABLE': 'prescribable',
        'QUANTITY': 'quantity',
        'RxCUI': 'RxCUI',
        'RXNAV_HUMAN_DRUG': 'rxnavHumanDrug',
        'RXNAV_VET_DRUG': 'rxnormVetDrug',
        'RxNormID': 'rxnormID',
        'RxNormName': 'rxnormName',
        'RxNormSynonym': 'rxNormSynonym',
        'SNOMEDCT': 'snomedCt',
        'SPL_SET_ID': 'splSetId',
        'STRENGTH': 'strength',
        'TTY': 'tty',
        'UNII_CODE': 'unii',
        'USP': 'usp',
        'VUID': 'vuid'
    }

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("ClinicalTrialGraphTask_5 does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:

        count = 0
        batch_num = 0
        fetch_cursor = None

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute("SET SESSION group_concat_max_len = 10000000")
            fetch_cursor.execute(self.FETCH_NEW_DRUG_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f'--- batch# = {batch_num} ---')

                chunks = []

                for row in rows:
                    chunk = self._create_drug_chunk(row)
                    if chunk:
                        chunks.append(chunk)

                if chunks:
                    #self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f'Created {len(chunks)} drug mappings in memgraph. Total = {count}')
                else:
                    self.logger.info('No valid drug mappings to insert into memgraph.')

        except Exception as e:
            self.logger.error(f"Error executing drug graph task: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_drug_chunk(self, row: Dict[str, Any]) -> Dict[str, Any]:

        rxnorm_id = row.get('RxNormID')
        props = row.get('props')
        intervention_name = row.get('intervention')
        wspacy = row.get('wspacy')

        if not rxnorm_id or not props or not intervention_name:
            return {}

        try:
            props_obj = json.loads('{' + props + '}')
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Error parsing drug props for RxNormID {rxnorm_id}: {e}")
            self.logger.error(f"props = {props}")
            return {}

        return {
            "rxnormID": rxnorm_id,
            "props": self.transform_json_object(props_obj),
            "wspacy": "true" if wspacy == 1 else "false",
            "_intervention_name_key": _make_hash_key(intervention_name)
        }


    def transform_json_object(self, json_obj: Dict[str, Any]) -> Dict[str, Any]:

        transformed = {}

        for old_key, value in json_obj.items():
            if old_key in self.KEY_MAP:
                transformed[self.KEY_MAP[old_key]] = value

        for old_key, new_key in self.KEY_MAP.items():
            if new_key not in transformed:
                transformed[new_key] = []

        return transformed

import os
import sys
import json
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _gard_text_normalize, _is_english, _is_under_char_threshold

"""
Create Condition nodes and ClinicalTrial/Condition/GARD mappings for new clinical trials.
"""
# Reference: B_clinical_trial/initializer/condition.py


class NewClinicalTrialConditionGraphTask(PipelineBase):
    """
    Create Condition nodes and link them to ClinicalTrial and GARD nodes.

    New clinical-trial JSON provides condition strings. This task normalizes
    those strings, matches them against known GARD names/synonyms, and writes
    the ClinicalTrial -> Condition -> GARD graph pattern.
    """

    BATCH_SIZE = 200

    # For each trial, create or reuse normalized Condition nodes, connect the
    # trial to each condition, then connect matched conditions to existing GARDs.
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk
        MATCH (ct: ClinicalTrial {nctId: chunk.nctId})

        WITH ct, chunk.condition_gard_mappings AS condition_gard_mappings
        UNWIND condition_gard_mappings AS mapping

        MERGE (con: Condition {condition: mapping.condition})
        MERGE (ct)-[:has_investigated_condition]->(con)

        WITH con, mapping.gardid_list AS gardid_list
        WHERE gardid_list IS NOT NULL AND size(gardid_list) > 0

        UNWIND gardid_list AS gardId
        MATCH (g: GARD {gardId: gardId})
        MERGE (con)-[:has_mapped_condition]->(g)
    '''

    FETCH_NEW_CLINICAL_QUERY = '''
        SELECT id, nctid, studies
        FROM clinical_trial_unique
        WHERE nctid IS NOT NULL
        AND is_new = 1
    '''

    def __init__(self):
        """Initialize MySQL and Memgraph connections for condition graph loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("NewClinicalTrialConditionGraphTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Build condition graph mappings for new clinical trials."""

        count = 0
        batch_num = 0 
        fetch_cursor = None

        try:
            # Preload normalized GARD names/synonyms once so each condition can
            # be mapped without repeatedly querying Memgraph.
            term_to_gard_ids = self._get_term_to_gard_ids()

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_CLINICAL_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f'--- batch# = {batch_num} ---')

                chunks = []

                for row in rows:
                    nctid = row.get('nctid')
                    if not nctid:
                        continue

                    try:
                        study = json.loads(row.get('studies') or '{}')
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Invalid JSON for nctId {nctid}: {e}")
                        continue

                    conditions = self._extract_conditions(study)
                    if not conditions:
                        continue

                    # Each mapping keeps the normalized condition text and any
                    # matching GARD IDs found from the preloaded term index.
                    condition_gard_mappings = self._map_conditions_to_gard_ids(conditions, term_to_gard_ids)
                    if not condition_gard_mappings:
                        continue

                    chunks.append({
                        "nctId": nctid,
                        "condition_gard_mappings": condition_gard_mappings
                    })

                if chunks:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f'Created {len(chunks)} condition mappings in memgraph. Total = {count}')
                else:
                    self.logger.info('No valid condition mappings to insert into memgraph.')

        except Exception as e:
            self.logger.error(f"Error executing condition graph task: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _extract_conditions(self, study: Dict[str, Any]) -> List[str]:
        """Read the ClinicalTrials.gov conditions list from a study payload."""

        if not isinstance(study, dict):
            return []

        protocol = study.get('protocolSection', {})
        if not isinstance(protocol, dict):
            return []

        conditions_module = protocol.get('conditionsModule', {})
        if not isinstance(conditions_module, dict):
            return []

        conditions = conditions_module.get('conditions', [])
        return conditions if isinstance(conditions, list) else []


    def _map_conditions_to_gard_ids(self, conditions: List[str], term_to_gard_ids: Dict[str, List[str]] ) -> List[Dict[str, Any]]:
        """Normalize trial conditions and attach matching GARD IDs when available."""

        mappings = []
        seen_conditions = set()

        for condition in conditions:
            if not condition:
                continue

            condition_normalized = _gard_text_normalize(condition)
            if not condition_normalized or condition_normalized in seen_conditions:
                continue

            seen_conditions.add(condition_normalized)
            mappings.append({
                "condition": condition_normalized,
                "gardid_list": term_to_gard_ids.get(condition_normalized, [])
            })

        return mappings


    def _get_term_to_gard_ids(self) -> Dict[str, List[str]]:
        """Invert the GARD term dictionary into normalized term -> GARD IDs."""

        term_to_gard_ids = {}
        gard_id_names_dict = self._get_GARD_names_syns()

        for gardid, terms_list in gard_id_names_dict.items():
            for term in terms_list:
                term_to_gard_ids.setdefault(term, []).append(gardid)

        return term_to_gard_ids


    def _get_GARD_names_syns(self) -> Dict[str, List[str]]:
        """Load GARD names/synonyms from Memgraph and normalize search terms."""

        gard_terms = {}
        response = self.memgraph.execute_and_fetch(
            'MATCH (x:GARD) RETURN x.gardId AS gardId, x.gardName AS gardName, x.synonyms AS synonyms'
        )

        for res in response:
            gardid = res['gardId']
            gardname = res['gardName']
            gardsyns = res['synonyms'] or []

            if not gardid or not gardname:
                continue

            if not isinstance(gardsyns, list):
                gardsyns = []

            gardsyns_eng = [syn for syn in gardsyns if _is_english(syn)]
            gardsyns_char_threshold = [syn for syn in gardsyns if _is_under_char_threshold(syn)]

            # Keep the same synonym filtering style used by GARD discovery:
            # avoid short abbreviations and terms that are less useful for exact matching.
            filtered_syns = [syn for syn in gardsyns if syn not in gardsyns_eng]
            filtered_syns = [syn for syn in filtered_syns if syn not in gardsyns_char_threshold]

            term_list = [gardname] + filtered_syns
            gard_terms[gardid] = [_gard_text_normalize(term) for term in term_list if term]

        return gard_terms

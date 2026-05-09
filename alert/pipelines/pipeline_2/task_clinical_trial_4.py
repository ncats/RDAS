import os
import sys
import json
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _clean

"""
Save new Clinical-Trial {nctid - pubmed_id} pairs into clinical_trial_nctid_pmids_mapping table if not exist
"""
# Reference: B_clinical_trial/init_5_clinical_trial_retrieve_pmids_umlti.py

class ClinicalTrialPublicationMappingTask(PipelineBase):
    """
    Extract PubMed references from new clinical trials.

    ClinicalTrials.gov study JSON may include publication references under
    protocolSection.referencesModule. This task stores each NCT ID to PMID pair
    once so later steps can import or link the related articles.
    """

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("ClinicalTrialPublicationMappingTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Insert new NCT ID to PMID mappings in batches."""

        # The NOT EXISTS guard keeps the mapping table idempotent across reruns.
        insert_sql = '''
            INSERT INTO clinical_trial_nctid_pmids_mapping (nctid, pmid, is_new)
            SELECT %s, %s, 1
            WHERE NOT EXISTS (
                SELECT 1
                FROM clinical_trial_nctid_pmids_mapping
                WHERE nctid = %s
                AND pmid = %s
            )
        '''

        insert_cursor = self.mysql.cursor()

        for chunks in self._nctid_pmids_generator():

            if not chunks:
                continue

            insert_cursor.executemany(insert_sql, chunks)
            self.mysql.commit()

            self.logger.info(f"{insert_cursor.rowcount} [nctid - pubmed_id] pairs have been added into clinical_trial_nctid_pmids_mapping table.\n")

        insert_cursor.close()

        # Explicitly close the all the db connections
        self.close()



    def _nctid_pmids_generator(self):
        """Yield batches of NCT ID / PMID tuples from newly imported studies."""

        ''' This will not be an infinite loop within one run. It will stop when the cursor result set is exhausted.  '''
        # clinical_trial_unique contains one row per NCT ID; is_new limits this
        # incremental pipeline to current update rows.
        query = f'''
            SELECT id, nctid, studies  FROM clinical_trial_unique
            WHERE
                nctid IS NOT NULL
            AND is_new = 1
            ORDER BY id
        '''

        batch_num = 0
        batch_size = 100

        cursor = self.mysql.cursor(dictionary=True, buffered=True)
        cursor.execute(query)

        while True:

            batch_num += 1

            rows = cursor.fetchmany(batch_size)

            if not rows:
                self.logger.info(f"No more rows to fetch.")
                break

            self.logger.info(f'\n--- batch# = {batch_num} ---')

            ''' processe rows in batches '''
            chunks = []

            for row in rows:
                nctid = row['nctid']
                study = json.loads(row['studies'])

                # PubMed IDs are stored in the references module when a trial
                # cites related publications.
                ref_module = study.get('protocolSection', dict()).get('referencesModule', {})
                references = ref_module.get('references', [])

                if not references:
                    continue

                for ref in references:
                    if not ref.get('pmid'):
                        continue

                    pmid = _clean(ref.get('pmid'))
                    if not pmid:
                        continue

                    chunks.append((nctid, pmid, nctid, pmid))

            yield chunks

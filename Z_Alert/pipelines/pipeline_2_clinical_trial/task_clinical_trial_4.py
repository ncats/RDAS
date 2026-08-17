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

        """Refresh current NCT ID to PMID mappings in batches."""

        '''
        Reset previous current-run markers for changed NCTIDs before reading the
        latest referencesModule JSON. Historical rows stay in the table, but only
        PMIDs still present in the current study payload are marked is_new=1 for
        downstream article import.
        '''
        reset_current_mapping_sql = '''
            UPDATE clinical_trial_nctid_pmids_mapping AS ctnp
            INNER JOIN clinical_trial_unique AS ctu
                ON ctu.nctid = ctnp.nctid
            SET ctnp.is_new = 0
            WHERE ctu.is_new = 1
            AND ctnp.is_new = 1
        '''

        '''
        Existing current PMID pairs should become is_new=1 again so a changed
        trial can retry publication import if a referenced PMID was seen before
        but publication_article still does not contain it.
        '''
        update_existing_sql = '''
            UPDATE clinical_trial_nctid_pmids_mapping
            SET is_new = 1
            WHERE nctid = %s
            AND pmid = %s
        '''

        ''' The NOT EXISTS guard keeps the mapping table idempotent across reruns. '''
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

        mapping_cursor = None

        try:
            mapping_cursor = self.mysql.cursor()
            mapping_cursor.execute(reset_current_mapping_sql)
            self.mysql.commit()
            self.logger.info(f"Reset {mapping_cursor.rowcount} existing clinical_trial_nctid_pmids_mapping rows before current PMID remapping.")

            for current_pairs in self._nctid_pmids_generator():

                if not current_pairs:
                    continue

                mapping_cursor.executemany(update_existing_sql, current_pairs)
                updated_count = max(mapping_cursor.rowcount, 0)

                insert_pairs = [
                    (nctid, pmid, nctid, pmid)
                    for nctid, pmid in current_pairs
                ]
                mapping_cursor.executemany(insert_sql, insert_pairs)
                inserted_count = max(mapping_cursor.rowcount, 0)
                self.mysql.commit()

                self.logger.info(
                    f"Marked {updated_count} existing and inserted {inserted_count} "
                    f"[nctid - pubmed_id] pairs in clinical_trial_nctid_pmids_mapping.\n"
                )

        except Exception as e:
            self.logger.error(f"Error refreshing clinical trial PMID mappings: {e}")
            self.mysql.rollback()

        finally:
            if mapping_cursor:
                mapping_cursor.close()

            # Explicitly close the all the db connections
            self.close()



    def _nctid_pmids_generator(self):

        """Yield batches of NCT ID / PMID tuples from newly imported studies."""

        ''' This will not be an infinite loop within one run. It will stop when the cursor result set is exhausted.  '''
        '''
        clinical_trial_unique contains one row per NCT ID; is_new limits this
        incremental pipeline to current update rows.
        '''
        query = f'''
            SELECT id, nctid, studies  FROM clinical_trial_unique
            WHERE
                nctid IS NOT NULL
            AND is_new = 1
            ORDER BY id
        '''

        batch_num = 0
        batch_size = 100

        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            cursor.execute(query)

            while True:

                batch_num += 1

                rows = cursor.fetchmany(batch_size)

                if not rows:
                    self.logger.info(f"No more rows to fetch.")
                    break

                self.logger.info(f'\n--- batch# = {batch_num} ---')

                ''' Process rows in batches. '''
                chunks = []
                seen_pairs = set()

                for row in rows:
                    nctid = row['nctid']

                    try:
                        study = json.loads(row['studies'])
                    except (json.JSONDecodeError, TypeError) as e:
                        self.logger.error(f"Invalid JSON for nctId {nctid}: {e}")
                        continue

                    if not isinstance(study, dict):
                        self.logger.error(f"Invalid study JSON object for nctId {nctid}: expected object.")
                        continue

                    '''
                    PubMed IDs are stored in the references module when a trial cites
                    related publications.
                    '''
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

                        pair = (nctid, pmid)
                        if pair in seen_pairs:
                            continue

                        seen_pairs.add(pair)
                        chunks.append(pair)

                yield chunks

        finally:
            if cursor:
                cursor.close()

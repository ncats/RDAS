import os
import sys
from typing import Any, Dict

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _arr, _val

"""
Create or refresh Annotation nodes and ClinicalTrial/Annotation mappings for new or changed clinical trials.
"""
'''
Reference: B_clinical_trial/initializer/annotation.py
Reference: Z_Alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_6.py
'''


class NewClinicalTrialAnnotationGraphTask(PipelineBase):
    """
    Create or refresh Annotation nodes and link them to new or changed
    ClinicalTrial nodes.

    Upstream annotation extraction stores UMLS concepts for clinical trials in
    MySQL. This task loads those concepts into Memgraph and connects each
    annotated trial to its UMLS Annotation nodes.
    """

    BATCH_SIZE = 200

    '''
    Annotation nodes are keyed by UMLS CUI so the same biomedical concept is
    reused across trials. A plain SET after MERGE refreshes the shared Annotation
    properties when a changed trial causes the CUI to be reprocessed.
    '''
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk

        MERGE (a: Annotation {umlsCui: chunk.umlsCui})
        SET
            a.umlsConcept = chunk.umlsConcept,
            a.semanticTypes = chunk.semanticTypes,
            a.semanticTypeNames = chunk.semanticTypeNames

        WITH a, chunk
        MATCH (ct: ClinicalTrial {nctId: chunk.nctId})
        MERGE (ct)-[:has_annotation]->(a)
    '''

    '''
    Remove Annotation relationships that belonged to this ClinicalTrial in an
    earlier run but are no longer present in the latest current-run annotations.
    Annotation nodes themselves are left in place because another record may use
    the same UMLS CUI.
    '''
    BATCH_DELETE_STALE_ANNOTATION_RELATIONSHIPS = '''
        UNWIND $chunks AS chunk
        MATCH (ct: ClinicalTrial {nctId: chunk.nctId})
        WITH ct, chunk.umlsCuis AS current_umls_cuis
        OPTIONAL MATCH (ct)-[old_rel:has_annotation]->(old:Annotation)
        WHERE old_rel IS NOT NULL
        AND (
            old.umlsCui IS NULL
            OR NOT old.umlsCui IN current_umls_cuis
        )
        DELETE old_rel
    '''

    '''
    If the latest study text produces no current annotation rows, no create chunk
    exists. This query still clears stale annotation relationships for those
    reprocessed NCT IDs.
    '''
    BATCH_DELETE_ALL_ANNOTATION_RELATIONSHIPS = '''
        UNWIND $nctids AS nctid
        MATCH (ct: ClinicalTrial {nctId: nctid})-[old_rel:has_annotation]->(:Annotation)
        DELETE old_rel
    '''

    '''
    Join to clinical_trial_unique so graph loading is scoped to clinical trials
    in the current alert run. The clinical_trial_annotation.is_new filter keeps
    old rows for the same NCT ID from being treated as current annotations.
    '''
    FETCH_NEW_ANNOTATION_QUERY = '''
        SELECT
            cta.nctid,
            cta.umls_cui,
            cta.umls_concept,
            cta.semantic_types,
            cta.semantic_type_names
        FROM clinical_trial_annotation AS cta
        INNER JOIN clinical_trial_unique AS ctu
            ON ctu.nctid = cta.nctid
        WHERE ctu.nctid IS NOT NULL
        AND ctu.is_new = 1
        AND cta.is_new = 1
        ORDER BY cta.nctid, cta.umls_cui
    '''

    FETCH_NEW_NCTID_QUERY = '''
        SELECT nctid
        FROM clinical_trial_unique
        WHERE nctid IS NOT NULL
        AND is_new = 1
    '''

    def __init__(self):

        """Initialize MySQL and Memgraph connections for annotation graph loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:

        raise NotImplementedError("NewClinicalTrialAnnotationGraphTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:

        """Fetch current-run trial annotations and write Annotation graph chunks."""

        count = 0
        batch_num = 0
        current_umls_cuis_by_nctid = {}
        fetch_cursor = None

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            '''
            Load every reprocessed NCT ID before streaming annotation rows. This
            lets the task clear stale relationships for changed trials whose
            latest description text produces no current annotation rows.
            '''
            fetch_cursor.execute(self.FETCH_NEW_NCTID_QUERY)
            new_nctids = {
                row.get('nctid')
                for row in fetch_cursor.fetchall()
                if row.get('nctid')
            }

            fetch_cursor.execute(self.FETCH_NEW_ANNOTATION_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f'--- batch# = {batch_num} ---')

                chunks = []

                for row in rows:
                    annotation_chunk = self._create_annotation_chunk(row)
                    if annotation_chunk:
                        current_umls_cuis_by_nctid.setdefault(annotation_chunk["nctId"], set()).add(annotation_chunk["umlsCui"])
                        chunks.append(annotation_chunk)

                if chunks:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f'Upserted {len(chunks)} annotation mappings in memgraph. Total = {count}')
                else:
                    self.logger.info('No valid annotations to upsert into memgraph.')

            if current_umls_cuis_by_nctid:
                cleanup_chunks = [
                    {
                        "nctId": nctid,
                        "umlsCuis": sorted(umls_cuis)
                    }
                    for nctid, umls_cuis in current_umls_cuis_by_nctid.items()
                ]
                self.memgraph.execute(self.BATCH_DELETE_STALE_ANNOTATION_RELATIONSHIPS, {"chunks": cleanup_chunks})
                self.logger.info(f'Removed stale annotation mappings for {len(cleanup_chunks)} trials with current annotations.')

            empty_annotation_nctids = sorted(new_nctids - set(current_umls_cuis_by_nctid))
            if empty_annotation_nctids:
                self.memgraph.execute(self.BATCH_DELETE_ALL_ANNOTATION_RELATIONSHIPS, {"nctids": empty_annotation_nctids})
                self.logger.info(f'Removed stale annotation mappings for {len(empty_annotation_nctids)} trials with no current annotations.')

        except Exception as e:
            self.logger.error(f"Error executing annotation graph task: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _create_annotation_chunk(self, row: Dict[str, Any]) -> Dict[str, Any]:

        """Convert one MySQL annotation row into the Cypher chunk shape."""

        nctid = row.get('nctid')
        umls_cui = _val(row.get('umls_cui'))

        '''
        Both nctId and CUI are required: nctId finds the trial, and CUI keys the
        Annotation node.
        '''
        if not nctid or not umls_cui:
            return {}

        return {
            "nctId": nctid,
            "umlsCui": umls_cui,
            "umlsConcept": _val(row.get('umls_concept')),
            "semanticTypes": _arr(row.get('semantic_types')),
            "semanticTypeNames": _arr(row.get('semantic_type_names'))
        }

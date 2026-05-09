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
Create Annotation nodes and ClinicalTrial/Annotation mappings for new clinical trials.
"""
# Reference: B_clinical_trial/initializer/annotation.py
# Reference: alert/pipelines/pipeline_2_clinical_trial/task_clinical_trial_6.py


class NewClinicalTrialAnnotationGraphTask(PipelineBase):
    """
    Create Annotation nodes and link them to new ClinicalTrial nodes.

    Upstream annotation extraction stores UMLS concepts for clinical trials in
    MySQL. This task loads those concepts into Memgraph and connects each
    annotated trial to its UMLS Annotation nodes.
    """

    BATCH_SIZE = 200

    # Annotation nodes are keyed by UMLS CUI so the same biomedical concept is
    # reused across trials.
    BATCH_CREATE = '''
        UNWIND $chunks AS chunk

        MERGE (a: Annotation {umlsCui: chunk.umlsCui})
        ON CREATE SET
            a.umlsConcept = chunk.umlsConcept,
            a.semanticTypes = chunk.semanticTypes,
            a.semanticTypeNames = chunk.semanticTypeNames

        WITH a, chunk
        MATCH (ct: ClinicalTrial {nctId: chunk.nctId})
        MERGE (ct)-[:annotated]->(a)
    '''

    # Join to clinical_trial_unique so graph loading is scoped to newly imported
    # clinical trials in the current alert run.
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
    '''

    def __init__(self):
        """Initialize MySQL and Memgraph connections for annotation graph loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("NewClinicalTrialAnnotationGraphTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Fetch new trial annotations and write Annotation graph chunks."""

        count = 0
        batch_num = 0
        fetch_cursor = None

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
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
                        chunks.append(annotation_chunk)

                if chunks:
                    self.memgraph.execute(self.BATCH_CREATE, {"chunks": chunks})

                    count += len(chunks)
                    self.logger.info(f'Created {len(chunks)} annotation mappings in memgraph. Total = {count}')
                else:
                    self.logger.info('No valid annotations to insert into memgraph.')

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

        # Both nctId and CUI are required: nctId finds the trial, and CUI keys
        # the Annotation node.
        if not nctid or not umls_cui:
            return {}

        return {
            "nctId": nctid,
            "umlsCui": umls_cui,
            "umlsConcept": _val(row.get('umls_concept')),
            "semanticTypes": _arr(row.get('semantic_types')),
            "semanticTypeNames": _arr(row.get('semantic_type_names'))
        }

"""
Create Memgraph Annotation nodes for new grant project annotation rows.

This alert-pipeline graph task is based on `D_grant/initializer/annotation.py`.
The historical initializer reads unprocessed rows from `grant_project_annotation`.
This task narrows the graph update to current alert rows by reading
`grant_project_annotation` directly where `is_new = 1`.

Relationship direction:
    The initializer creates the relationship from Project to Annotation:

        (Project)-[:has_annotation]->(Annotation)

    This task keeps the same direction and relationship type.

Processing flow:
    1. Read current new rows from `grant_project_annotation`.
    2. Convert annotation rows into the Annotation node property names used in Memgraph.
    3. MERGE Annotation by `umlsCui`.
    4. Match the existing Project node by `applicationId`.
    5. MERGE the Project -> Annotation `has_annotation` relationship.

Notes:
    This task expects Project nodes to already exist in Memgraph. Run
    task_grant_graph_1.py before this task so the MATCH on
    Project.applicationId can create the relationship.
"""

# Reference: D_grant/initializer/annotation.py

import os
import sys
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _arr, _val


class NewGrantAnnotationGraphTask(PipelineBase):
    """Upsert current alert-run grant Annotation nodes and Project-Annotation links into Memgraph."""

    BATCH_SIZE = 300

    '''
    Annotation nodes are shared by UMLS CUI. The property name
    semanticTypesNames matches the grant initializer even though the singular
    semanticTypeNames spelling appears in some other pipeline graph tasks.
    '''
    UPSERT_ANNOTATIONS_CYPHER = '''
        UNWIND $chunks AS chunk

        MERGE (a:Annotation {umlsCui: chunk.umlsCui})
        ON CREATE SET
            a.umlsCui = chunk.umlsCui,
            a.umlsConcept = chunk.umlsConcept,
            a.semanticTypes = chunk.semanticTypes,
            a.semanticTypesNames = chunk.semanticTypesNames

        WITH a, chunk
        MATCH (p:Project {applicationId: chunk.applicationId})
        MERGE (p)-[:has_annotation]->(a)
    '''

    '''
    Read new grant annotations directly from grant_project_annotation. Rows
    without application_id or umls_cui cannot be connected to Project or keyed
    as Annotation nodes, so the SQL filters them before batch conversion.
    '''
    FETCH_NEW_ANNOTATIONS_QUERY = '''
        SELECT
            id,
            application_id,
            umls_cui,
            umls_concept,
            semantic_types,
            semantic_type_names
        FROM grant_project_annotation
        WHERE
            is_new = 1
            AND application_id IS NOT NULL
            AND umls_cui IS NOT NULL
        ORDER BY id
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewGrantAnnotationGraphTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Fetch current new grant annotations in batches and upsert them into Memgraph."""

        fetch_cursor = None
        summary = {
            "batches_seen": 0,
            "batches_failed": 0,
            "rows_seen": 0,
            "rows_skipped": 0,
            "annotations_submitted": 0,
        }

        try:
            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            if self.memgraph is None:
                self.logger.error("Unable to create Memgraph connection.")
                return

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_ANNOTATIONS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    break

                summary["batches_seen"] += 1
                summary["rows_seen"] += len(rows)

                chunks = self._build_annotation_chunks(rows)
                summary["rows_skipped"] += len(rows) - len(chunks)

                if not chunks:
                    self.logger.info(f"Grant annotation graph batch {summary['batches_seen']} had no valid rows.")
                    continue

                try:
                    self.memgraph.execute(self.UPSERT_ANNOTATIONS_CYPHER, {"chunks": chunks})

                    summary["annotations_submitted"] += len(chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} grant Annotation rows to Memgraph. "
                        f"Total submitted={summary['annotations_submitted']}."
                    )

                except Exception:
                    summary["batches_failed"] += 1
                    self.logger.exception(f"Grant annotation graph batch {summary['batches_seen']} failed. Continuing with next batch.")
                    continue

            self.logger.info(f"Completed grant Annotation graph load. Summary={summary}")

        except Exception:
            self.logger.exception(f"NewGrantAnnotationGraphTask failed. Summary={summary}")
            return

        finally:
            if fetch_cursor is not None:
                fetch_cursor.close()

            self.close()


    def _build_annotation_chunks(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert MySQL rows into Memgraph Annotation payload dictionaries."""

        chunks = []

        for row in rows:
            chunk = self._create_annotation_chunk(row)

            if chunk is None:
                continue

            chunks.append(chunk)

        return chunks


    def _create_annotation_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build one Annotation payload, returning None when required keys are missing."""

        application_id = row.get("application_id")
        umls_cui = _val(row.get("umls_cui"))

        if application_id is None:
            self.logger.error(f"Skipping grant Annotation row without application_id. id={row.get('id')}")
            return None

        if not umls_cui:
            self.logger.error(f"Skipping grant Annotation row without umls_cui. id={row.get('id')}")
            return None

        return {
            "applicationId": application_id,
            "umlsCui": umls_cui,
            "umlsConcept": _val(row.get("umls_concept")),
            "semanticTypes": _arr(row.get("semantic_types")),
            "semanticTypesNames": _arr(row.get("semantic_type_names")),
        }

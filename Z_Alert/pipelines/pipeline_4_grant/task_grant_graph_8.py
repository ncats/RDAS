"""
Create Memgraph CoreProject-ClinicalTrial relationships for new grant clinical-study rows.

This alert-pipeline graph task is based on
`D_grant/initializer/core_project_clinical_trail_relation.py`. The historical
initializer reads unprocessed rows from `grant_clinical_study`. This task
narrows the graph update to current alert rows by reading
`grant_clinical_study` directly where `is_new = 1`.

Relationship direction:
    The initializer creates the relationship from CoreProject to ClinicalTrial:

        (CoreProject)-[:has_clinical_trial]->(ClinicalTrial)

    This task keeps the same direction and relationship type.

Processing flow:
    1. Read distinct current new `(core_project_num, nctid)` pairs from `grant_clinical_study`.
    2. Match the existing ClinicalTrial node by `nctId`.
    3. Match the existing CoreProject node by `coreProjectNumber`.
    4. MERGE the CoreProject -> ClinicalTrial `has_clinical_trial` relationship.

Notes:
    This task expects ClinicalTrial and CoreProject nodes to already exist in
    Memgraph. Run the clinical-trial graph task and task_grant_graph_3.py before
    this task so both MATCH clauses can create the relationship.
"""

# Reference: D_grant/initializer/core_project_clinical_trail_relation.py

import os
import sys
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase


class NewCoreProjectClinicalTrialRelationshipGraphTask(PipelineBase):
    """Upsert current alert-run CoreProject-to-ClinicalTrial relationships into Memgraph."""

    BATCH_SIZE = 200

    '''
    Relationship-only alert task: match existing nodes and merge the edge. This
    mirrors the initializer without creating partial ClinicalTrial or
    CoreProject nodes.
    '''
    UPSERT_RELATIONSHIPS_CYPHER = '''
        UNWIND $chunks AS chunk
        MATCH (ct:ClinicalTrial {nctId: chunk.nctId})
        MATCH (cp:CoreProject {coreProjectNumber: chunk.coreProjectNumber})
        MERGE (cp)-[:has_clinical_trial]->(ct)
    '''

    '''
    Use only current grant clinical-study rows. Trimming avoids sending blank
    keys into Cypher, where they would fail to match useful graph nodes.
    '''
    FETCH_NEW_RELATIONSHIPS_QUERY = '''
        SELECT DISTINCT
            core_project_num,
            nctid
        FROM grant_clinical_study
        WHERE
            is_new = 1
            AND core_project_num IS NOT NULL
            AND TRIM(core_project_num) <> ''
            AND nctid IS NOT NULL
            AND TRIM(nctid) <> ''
        ORDER BY
            core_project_num,
            nctid
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewCoreProjectClinicalTrialRelationshipGraphTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Fetch current new CoreProject-ClinicalTrial pairs and upsert them into Memgraph."""

        fetch_cursor = None
        summary = {
            "batches_seen": 0,
            "batches_failed": 0,
            "rows_seen": 0,
            "rows_skipped": 0,
            "relationships_submitted": 0,
        }

        try:
            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            if self.memgraph is None:
                self.logger.error("Unable to create Memgraph connection.")
                return

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_RELATIONSHIPS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    break

                summary["batches_seen"] += 1
                summary["rows_seen"] += len(rows)

                chunks = self._build_relationship_chunks(rows)
                summary["rows_skipped"] += len(rows) - len(chunks)

                if not chunks:
                    self.logger.info(f"CoreProject-ClinicalTrial graph batch {summary['batches_seen']} had no valid rows.")
                    continue

                try:
                    self.memgraph.execute(self.UPSERT_RELATIONSHIPS_CYPHER, {"chunks": chunks})

                    summary["relationships_submitted"] += len(chunks)
                    self.logger.info(
                        f"Submitted {len(chunks)} CoreProject-ClinicalTrial relationships to Memgraph. "
                        f"Total submitted={summary['relationships_submitted']}."
                    )

                except Exception:
                    summary["batches_failed"] += 1
                    self.logger.exception(f"CoreProject-ClinicalTrial graph batch {summary['batches_seen']} failed. Continuing with next batch.")
                    continue

            self.logger.info(f"Completed CoreProject-ClinicalTrial relationship graph load. Summary={summary}")

        except Exception:
            self.logger.exception(f"NewCoreProjectClinicalTrialRelationshipGraphTask failed. Summary={summary}")
            return

        finally:
            if fetch_cursor is not None:
                fetch_cursor.close()

            self.close()


    def _build_relationship_chunks(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert MySQL rows into Memgraph relationship payload dictionaries."""

        chunks = []

        for row in rows:
            chunk = self._create_relationship_chunk(row)

            if chunk is None:
                continue

            chunks.append(chunk)

        return chunks


    def _create_relationship_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Build one relationship payload, returning None when required keys are missing."""

        core_project_num = row.get("core_project_num")
        nctid = row.get("nctid")

        if not core_project_num:
            self.logger.error(f"Skipping CoreProject-ClinicalTrial relation without core_project_num. nctid={nctid}")
            return None

        if not nctid:
            self.logger.error(f"Skipping CoreProject-ClinicalTrial relation without nctid. core_project_num={core_project_num}")
            return None

        return {
            "coreProjectNumber": str(core_project_num),
            "nctId": str(nctid),
        }

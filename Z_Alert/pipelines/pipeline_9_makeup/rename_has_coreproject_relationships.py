"""
Rename existing Memgraph GARD-to-CoreProject relationships.

Memgraph relationship types cannot be renamed in place, so this makeup task
creates the replacement edge and then deletes the old edge:

    (GARD)-[:has_coreproject]->(CoreProject)
    (GARD)-[:has_core_project]->(CoreProject)

Run from the repository root:

    conda run -n rdas python Z_Alert/pipelines/pipeline_9_makeup/rename_has_coreproject_relationships.py
"""

import os
import sys
import time


_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..", "..")),
    os.path.abspath(os.path.join(_dir, "..", "..", "..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _time_hms


class RenameHasCoreProjectRelationshipsTask(PipelineBase):
    """Move old CoreProject relationship edges to the corrected type."""

    BATCH_SIZE = 1000

    COUNT_OLD_RELATIONSHIPS_CYPHER = '''
        MATCH (:GARD)-[r:has_coreproject]->(:CoreProject)
        RETURN count(r) AS relationship_count
    '''

    COUNT_NEW_RELATIONSHIPS_CYPHER = '''
        MATCH (:GARD)-[r:has_core_project]->(:CoreProject)
        RETURN count(r) AS relationship_count
    '''

    RENAME_RELATIONSHIPS_CYPHER = '''
        MATCH (gard:GARD)-[old_rel:has_coreproject]->(cp:CoreProject)
        WITH gard, old_rel, cp
        LIMIT $batch_size
        MERGE (gard)-[:has_core_project]->(cp)
        DELETE old_rel
        RETURN count(*) AS relationships_renamed
    '''

    def __init__(self):

        super().__init__(init_mysql=False, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:

        self.logger.info("RenameHasCoreProjectRelationshipsTask does not use find_new_data().")


    def process_new_data(self) -> None:

        start_time = time.time()
        summary = {
            "old_relationships_before": 0,
            "new_relationships_before": 0,
            "relationships_renamed": 0,
            "old_relationships_after": 0,
            "new_relationships_after": 0,
            "batches_seen": 0,
        }

        try:
            if self.memgraph is None:
                self._log_progress("Unable to create Memgraph connection.")
                return

            summary["old_relationships_before"] = self._count_relationships(self.COUNT_OLD_RELATIONSHIPS_CYPHER)
            summary["new_relationships_before"] = self._count_relationships(self.COUNT_NEW_RELATIONSHIPS_CYPHER)
            self._log_progress(
                f"Starting has_coreproject rename. "
                f"old_before={summary['old_relationships_before']}, "
                f"new_before={summary['new_relationships_before']}."
            )

            while True:
                result = list(self.memgraph.execute_and_fetch(self.RENAME_RELATIONSHIPS_CYPHER, {"batch_size": self.BATCH_SIZE}))
                renamed_count = int(result[0].get("relationships_renamed", 0)) if result else 0

                if renamed_count == 0:
                    break

                summary["batches_seen"] += 1
                summary["relationships_renamed"] += renamed_count
                self._log_progress(
                    f"Renamed batch {summary['batches_seen']}: {renamed_count} relationships. "
                    f"Total renamed={summary['relationships_renamed']}."
                )

            summary["old_relationships_after"] = self._count_relationships(self.COUNT_OLD_RELATIONSHIPS_CYPHER)
            summary["new_relationships_after"] = self._count_relationships(self.COUNT_NEW_RELATIONSHIPS_CYPHER)
            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self._log_progress(
                f"Completed has_coreproject rename. Summary={summary}. "
                f"Total time={hours} hours, {minutes} minutes, {seconds} seconds."
            )

        except Exception:
            self.logger.exception(f"RenameHasCoreProjectRelationshipsTask failed. Summary={summary}")
            print(f"RenameHasCoreProjectRelationshipsTask failed. Summary={summary}", flush=True)

        finally:
            self.close()


    def _count_relationships(self, cypher: str) -> int:

        result = list(self.memgraph.execute_and_fetch(cypher))

        if not result:
            return 0

        return int(result[0].get("relationship_count", 0) or 0)


    def _log_progress(self, message: str) -> None:

        self.logger.info(message)
        print(message, flush=True)


if __name__ == "__main__":

    RenameHasCoreProjectRelationshipsTask().process_new_data()

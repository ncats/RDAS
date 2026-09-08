"""
Step 6: initialize GARD-to-GARD hierarchy relationships.

This concrete graph task reads `GARD_classification.csv` and creates
`:subclass_of` relationships between existing `:GARD` nodes.
"""

import os
import sys
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.gard_update.gard_common import GARD_HIERARCHY_FILE, clean_optional_text, create_memgraph_indexes_if_missing, iter_csv_dict_rows, log_csv_file_summary, resolve_data_file, validate_batch_size
from pipelines.pipeline_base import PipelineBase


UPSERT_PARENT_RELATIONSHIPS_CYPHER = """
    UNWIND $chunks AS chunk
    WITH chunk
    WHERE chunk.hasParent = true
    MATCH (parent:GARD {gardId: chunk.parent})
    MATCH (current:GARD {gardId: chunk.current})
    MERGE (current)-[:subclass_of]->(parent)
"""

UPSERT_CHILD_RELATIONSHIPS_CYPHER = """
    UNWIND $chunks AS chunk
    WITH chunk
    WHERE chunk.hasChild = true
    MATCH (current:GARD {gardId: chunk.current})
    MATCH (child:GARD {gardId: chunk.child})
    MERGE (child)-[:subclass_of]->(current)
"""


class GardGraphHierarchyRelationshipInitializationTask(PipelineBase):

    """Create `:subclass_of` relationships between existing GARD nodes."""

    def __init__(self, data_file: Any = GARD_HIERARCHY_FILE, batch_size: int = 200):

        super().__init__(init_mysql=False, init_memgraph=True)
        self.data_file = resolve_data_file(data_file)
        self.batch_size = validate_batch_size(batch_size)


    def find_new_data(self, gard_node) -> None:

        raise NotImplementedError("GardGraphHierarchyRelationshipInitializationTask does not implement find_new_data().")


    def process_new_data(self) -> None:

        try:
            log_csv_file_summary(self.logger, "GARD hierarchy", self.data_file)
            create_memgraph_indexes_if_missing(self.memgraph, {"GARD": ["gardId"]}, self.logger)

            chunks: List[Dict[str, Any]] = []
            total_submitted = 0
            batch_number = 0

            for row_index, row in enumerate(iter_csv_dict_rows(self.data_file), start=2):
                try:
                    chunk = self._build_hierarchy_chunk(row)

                except KeyError as e:
                    self.logger.error(f"Skipping hierarchy CSV row {row_index}; missing required column: {e}")
                    continue

                if not chunk:
                    continue

                chunks.append(chunk)

                if len(chunks) >= self.batch_size:
                    batch_number += 1
                    total_submitted = self._submit_batch(chunks, batch_number, total_submitted)
                    chunks = []

            if chunks:
                batch_number += 1
                total_submitted = self._submit_batch(chunks, batch_number, total_submitted)

            self.logger.info(f"Completed GARD hierarchy graph initialization. Submitted rows={total_submitted}.")

        except Exception:
            self.logger.exception("GardGraphHierarchyRelationshipInitializationTask failed.")
            raise

        finally:
            self.close()


    def _submit_batch(self, chunks: List[Dict[str, Any]], batch_number: int, total_submitted: int) -> int:

        """Submit parent and child hierarchy relationships for one batch."""

        self.memgraph.execute(UPSERT_PARENT_RELATIONSHIPS_CYPHER, {"chunks": chunks})
        self.memgraph.execute(UPSERT_CHILD_RELATIONSHIPS_CYPHER, {"chunks": chunks})
        total_submitted += len(chunks)
        self.logger.info(f"Upserted GARD hierarchy batch={batch_number}, rows={len(chunks)}, total={total_submitted}.")
        return total_submitted


    @staticmethod
    def _build_hierarchy_chunk(row: Dict[str, Any]) -> Dict[str, Any]:

        """Convert one hierarchy CSV row into parent/child relationship inputs."""

        current = clean_optional_text(row["GardID"])
        parent = clean_optional_text(row.get("Parent"))
        child = clean_optional_text(row.get("Child"))

        if not current:
            return {}

        return {
            "current": current,
            "parent": parent,
            "child": child,
            "hasParent": bool(parent),
            "hasChild": bool(child),
        }

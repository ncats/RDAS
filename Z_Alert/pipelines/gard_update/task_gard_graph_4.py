"""
Step 5: initialize Phenotype nodes and GARD-to-Phenotype relationships.

This concrete graph task reads the local phenotype association CSV and writes
`:Phenotype` nodes plus `:has_phenotype` relationships directly.
"""

import os
import sys
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.gard_update.gard_common import GARD_PHENOTYPE_FILE, clean_optional_text, create_memgraph_indexes_if_missing, iter_csv_dict_rows, log_csv_file_summary, resolve_data_file, split_bracketed_values, validate_batch_size
from pipelines.pipeline_base import PipelineBase


UPSERT_GARD_PHENOTYPE_CYPHER = """
    UNWIND $chunks AS chunk
    MATCH (g:GARD {gardId: chunk.gardId})
    WITH g, chunk
    WHERE chunk.hpoId <> ''
    MERGE (d:Phenotype {hpoId: chunk.hpoId})
    SET d.hpoTerm = chunk.hpoTerm
    MERGE (g)-[r:has_phenotype]->(d)
    SET
        r.evidence = chunk.evidence,
        r.references = chunk.references,
        r.hpoTermFrequency = chunk.hpoTermFrequency
"""


class GardGraphPhenotypeRelationshipInitializationTask(PipelineBase):
    """Create Phenotype nodes and connect them to existing GARD nodes."""

    def __init__(self, data_file: Any = GARD_PHENOTYPE_FILE, batch_size: int = 200):

        super().__init__(init_mysql=False, init_memgraph=True)
        self.data_file = resolve_data_file(data_file)
        self.batch_size = validate_batch_size(batch_size)


    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("GardGraphPhenotypeRelationshipInitializationTask does not implement find_new_data().")


    def process_new_data(self) -> None:

        try:
            log_csv_file_summary(self.logger, "GARD phenotype", self.data_file)
            create_memgraph_indexes_if_missing(
                self.memgraph,
                {
                    "GARD": ["gardId"],
                    "Phenotype": ["hpoId"],
                },
                self.logger,
            )

            chunks: List[Dict[str, Any]] = []
            total_submitted = 0
            batch_number = 0

            for row_index, row in enumerate(iter_csv_dict_rows(self.data_file), start=2):
                try:
                    chunk = self._build_phenotype_chunk(row)

                except KeyError as e:
                    self.logger.error(f"Skipping phenotype CSV row {row_index}; missing required column: {e}")
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

            self.logger.info(f"Completed GARD phenotype graph initialization. Submitted rows={total_submitted}.")

        except Exception:
            self.logger.exception("GardGraphPhenotypeRelationshipInitializationTask failed.")
            raise

        finally:
            self.close()


    def _submit_batch(self, chunks: List[Dict[str, Any]], batch_number: int, total_submitted: int) -> int:
        """Submit one Phenotype relationship batch and return the new submitted count."""

        self.memgraph.execute(UPSERT_GARD_PHENOTYPE_CYPHER, {"chunks": chunks})
        total_submitted += len(chunks)
        self.logger.info(f"Upserted GARD phenotype batch={batch_number}, rows={len(chunks)}, total={total_submitted}.")
        return total_submitted


    @staticmethod
    def _build_phenotype_chunk(row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert one phenotype CSV row into graph-ready properties."""

        gard_id = clean_optional_text(row["GardID"])
        hpo_id = clean_optional_text(row["HPO_ID"])

        if not gard_id or not hpo_id:
            return {}

        return {
            "gardId": gard_id,
            "hpoId": hpo_id,
            "hpoTerm": clean_optional_text(row.get("HPO_Term")),
            "hpoTermFrequency": clean_optional_text(row.get("HPOTerm_Frequency")),
            "evidence": clean_optional_text(row.get("Evidence")),
            "references": split_bracketed_values(row.get("References")),
        }

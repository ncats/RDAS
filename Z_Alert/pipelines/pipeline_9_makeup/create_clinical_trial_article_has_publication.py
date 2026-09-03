"""
Create ClinicalTrial-to-Article `has_publication` relationships in Memgraph.

This makeup script uses MySQL table `clinical_trial_nctid_pmids_mapping` as the
source of truth:

    clinical_trial_nctid_pmids_mapping.nctid -> ClinicalTrial.nctId
    clinical_trial_nctid_pmids_mapping.pmid  -> Article.pubmedId

Run from the repository root:

    conda run -n rdas python Z_Alert/pipelines/pipeline_9_makeup/x_create_clinical_trial_article_has_publication.py

Or, if the `rdas` conda environment is already active:

    python Z_Alert/pipelines/pipeline_9_makeup/x_create_clinical_trial_article_has_publication.py
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple


_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..", "..")),
    os.path.abspath(os.path.join(_dir, "..", "..", "..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _time_hms, _to_int


class ClinicalTrialArticlePublicationRelationshipMakeupTask(PipelineBase):
    """Create missing ClinicalTrial-to-Article publication relationships."""

    BATCH_SIZE = 500

    '''
    Keyset pagination keeps the MySQL read stable and restart-friendly without
    asking MySQL to buffer the whole mapping table. The unique key on
    (nctid, pmid) should keep one source row per pair, but this query still
    reads by id so it can safely move forward if older databases contain bad
    historical rows.
    '''
    FETCH_MAPPING_ROWS_QUERY = '''
        SELECT
            id,
            nctid,
            pmid
        FROM clinical_trial_nctid_pmids_mapping
        WHERE id > %s
        AND nctid IS NOT NULL
        AND TRIM(nctid) <> ''
        AND pmid IS NOT NULL
        ORDER BY id
        LIMIT %s
    '''

    '''
    Relationship-only repair: MATCH existing nodes and MERGE the edge. This
    avoids creating partial Article nodes that only have a PMID, and avoids
    creating partial ClinicalTrial nodes that only have an NCT ID.

    Current pipeline ClinicalTrial nodes use `nctId`, and Article nodes are
    keyed by integer `pubmedId`.
    '''
    UPSERT_RELATIONSHIPS_CYPHER = '''
        UNWIND $chunks AS chunk
        MATCH (ct:ClinicalTrial {nctId: chunk.nctId})
        MATCH (a:Article {pubmedId: chunk.pubmedId})
        MERGE (ct)-[:has_publication]->(a)
        RETURN count(*) AS relationships_matched
    '''

    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:

        self.logger.info("ClinicalTrialArticlePublicationRelationshipMakeupTask does not use find_new_data().")


    def process_new_data(self) -> None:

        """Read NCT ID/PMID mappings and merge ClinicalTrial-to-Article edges."""

        fetch_cursor = None
        last_seen_id = 0
        start_time = time.time()
        summary = {
            "batches_seen": 0,
            "batches_failed": 0,
            "rows_seen": 0,
            "rows_skipped": 0,
            "relationships_submitted": 0,
            "relationships_matched": 0,
        }

        try:
            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            if self.memgraph is None:
                self.logger.error("Unable to create Memgraph connection.")
                return

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            while True:
                fetch_cursor.execute(self.FETCH_MAPPING_ROWS_QUERY, (last_seen_id, self.BATCH_SIZE))
                rows = fetch_cursor.fetchall()

                if not rows:
                    self.logger.info("No more clinical trial PMID mapping rows to fetch.")
                    break

                summary["batches_seen"] += 1
                summary["rows_seen"] += len(rows)
                last_seen_id = max(row.get("id") or last_seen_id for row in rows)

                chunks = self._build_relationship_chunks(rows)
                summary["rows_skipped"] += len(rows) - len(chunks)

                if not chunks:
                    self.logger.info(f"ClinicalTrial-Article batch {summary['batches_seen']} had no valid rows.")
                    continue

                try:
                    relationships_matched = self._upsert_relationships(chunks)
                    summary["relationships_submitted"] += len(chunks)
                    summary["relationships_matched"] += relationships_matched

                    self.logger.info(
                        f"Submitted {len(chunks)} ClinicalTrial-to-Article has_publication relationships. "
                        f"Matched existing graph nodes for {relationships_matched}. "
                        f"Total submitted={summary['relationships_submitted']}; "
                        f"total matched={summary['relationships_matched']}."
                    )

                except Exception:
                    summary["batches_failed"] += 1
                    self.logger.exception(f"ClinicalTrial-Article batch {summary['batches_seen']} failed. Continuing with next batch.")
                    continue

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(
                f"Completed ClinicalTrial-to-Article has_publication makeup load. "
                f"Summary={summary}. Total time={hours} hours, {minutes} minutes, {seconds} seconds."
            )

        except Exception:
            self.logger.exception(f"ClinicalTrialArticlePublicationRelationshipMakeupTask failed. Summary={summary}")

        finally:
            if fetch_cursor is not None:
                fetch_cursor.close()

            self.close()


    def _build_relationship_chunks(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

        """Convert MySQL mapping rows into Memgraph relationship payloads."""

        chunks = []
        seen_pairs: Set[Tuple[str, int]] = set()

        for row in rows:
            chunk = self._create_relationship_chunk(row)

            if chunk is None:
                continue

            pair = (chunk["nctId"], chunk["pubmedId"])
            if pair in seen_pairs:
                continue

            seen_pairs.add(pair)
            chunks.append(chunk)

        return chunks


    def _create_relationship_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:

        """Build one relationship payload, returning None for unusable keys."""

        nctid = str(row.get("nctid") or "").strip()
        pubmed_id = _to_int(row.get("pmid"))

        if not nctid:
            self.logger.error(f"Skipping ClinicalTrial-Article relation without nctid. mapping_id={row.get('id')}")
            return None

        if pubmed_id is None:
            self.logger.error(f"Skipping ClinicalTrial-Article relation with invalid pmid={row.get('pmid')!r}. mapping_id={row.get('id')}")
            return None

        return {
            "nctId": nctid,
            "pubmedId": pubmed_id,
        }


    def _upsert_relationships(self, chunks: List[Dict[str, Any]]) -> int:

        """Merge one batch into Memgraph and return how many source pairs matched both nodes."""

        if not chunks:
            return 0

        rows = list(self.memgraph.execute_and_fetch(self.UPSERT_RELATIONSHIPS_CYPHER, {"chunks": chunks}))
        return int(rows[0].get("relationships_matched") or 0) if rows else 0


if __name__ == "__main__":

    ClinicalTrialArticlePublicationRelationshipMakeupTask().process_new_data()

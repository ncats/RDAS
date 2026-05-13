import os
import sys
from collections import defaultdict

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Update GARD nodes with EPI/NHS article counts.

For each GARD ID that has new publication articles, count the new
EPI/NHS rows and write those counts to the existing GARD node
countEpiArticles and countNhsArticles properties.
"""

# Reference: C_publication/initializer/x_epi_nhs_count.py


class GardPublicationEpiNhsCountUpdateTask(PipelineBase):
    """Update GARD EPI/NHS publication counters from publication articles."""

    BATCH_SIZE = 50

    ''' Write the computed count values to the GARD node counters. '''
    BATCH_UPDATE = '''
        UNWIND $chunks AS chunk
        MATCH (d:GARD {gardId: chunk.gardId})
        SET
            d.countEpiArticles = chunk.countEpiArticles,
            d.countNhsArticles = chunk.countNhsArticles
    '''

    # Start from new publication rows, then find the touched GARD IDs through
    # the mapping table.
    FETCH_GARD_IDS_QUERY = '''
        SELECT DISTINCT pgspm.gard_id, pgspm.pubmed_id
        FROM publication_article AS pa
        STRAIGHT_JOIN publication_gard_searchterm_pubmed_mapping AS pgspm
            ON pa.pubmed_id = pgspm.pubmed_id
        WHERE pa.is_new = 1
        AND pgspm.gard_id IS NOT NULL
        AND pgspm.pubmed_id IS NOT NULL
        ORDER BY pgspm.gard_id, pgspm.pubmed_id
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("GardPublicationEpiNhsCountUpdateTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Find affected GARD IDs, calculate new article counts, and update graph nodes."""

        fetch_pair_cursor = None
        count_cursor = None
        total = 0
        batch_num = 0

        try:
            fetch_pair_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            count_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            fetch_pair_cursor.execute(self.FETCH_GARD_IDS_QUERY)

            while True:
                rows = fetch_pair_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break
                
                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                gard_pubmed_objects = self._build_gard_pubmed_objects(rows)

                if not gard_pubmed_objects:
                    self.logger.info("No valid GARD/PubMed pairs found in this batch.")
                    continue

                # Count EPI/NHS flags for this batch of GARD IDs before sending
                # the compact counter deltas to Memgraph.
                chunks = self._get_epi_nhs_count_chunks(count_cursor, gard_pubmed_objects)
                print(f'# of chunks = {len(chunks)}')
                if not chunks:
                    self.logger.info("No EPI/NHS count chunks to update in Memgraph.")
                    continue

                try:
                    self.memgraph.execute(self.BATCH_UPDATE, {"chunks": chunks})

                    total += len(chunks)
                    self.logger.info(f"Submitted {len(chunks)} GARD EPI/NHS count updates to Memgraph. Total = {total}")

                except Exception as e:
                    self.logger.error(f"Error executing GARD EPI/NHS count batch update: {e}")

        except Exception as e:
            self.logger.error(f"Error updating GARD EPI/NHS article counts in Memgraph: {e}")

        finally:
            if fetch_pair_cursor:
                fetch_pair_cursor.close()

            if count_cursor:
                count_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _build_gard_pubmed_objects(self, rows):
        """Group fetched GARD/PubMed pairs into one object per GARD ID."""

        gard_pubmed_map = defaultdict(set)

        for row in rows:
            gard_id = row.get("gard_id")
            pubmed_id = row.get("pubmed_id")

            if not gard_id or pubmed_id is None:
                continue

            try:
                pubmed_id = int(pubmed_id)
            except (TypeError, ValueError):
                self.logger.warning(f"Invalid PubMed ID skipped for gard_id={gard_id}: {pubmed_id}")
                continue

            gard_pubmed_map[str(gard_id)].add(pubmed_id)

        return [
            {
                "gardId": gard_id,
                "pubmed_id_list": sorted(pubmed_id_set),
            }
            for gard_id, pubmed_id_set in gard_pubmed_map.items()
        ]


    def _get_epi_nhs_count_chunks(self, cursor, gard_pubmed_objects):
        """Aggregate new EPI/NHS article counts for a batch of GARD IDs."""

        pubmed_ids = sorted({
            pubmed_id
            for obj in gard_pubmed_objects
            for pubmed_id in obj.get("pubmed_id_list", [])
        })

        if not pubmed_ids:
            return []

        placeholders = ",".join(["%s"] * len(pubmed_ids))
        # The GARD/PubMed mapping rows were already fetched and grouped. This
        # query only reads the article flags for those PMIDs, avoiding another
        # join against the large mapping table.
        article_query = f'''
            SELECT
                pubmed_id,
                MAX(LOWER(COALESCE(is_EPI, '')) IN ('1', 'true')) AS is_epi,
                MAX(LOWER(COALESCE(is_NHS, '')) IN ('1', 'true')) AS is_nhs
            FROM publication_article
            WHERE pubmed_id IN ({placeholders})
            GROUP BY pubmed_id
        '''

        cursor.execute(article_query, pubmed_ids)
        results = cursor.fetchall()
        
        article_flags = {
            int(result["pubmed_id"]): {
                "is_epi": int(result.get("is_epi") or 0),
                "is_nhs": int(result.get("is_nhs") or 0),
            }
            for result in results
            if result.get("pubmed_id") is not None
        }

        chunks = []

        for obj in gard_pubmed_objects:
            # Each chunk contains the count values that BATCH_UPDATE writes to
            # the existing GARD node counters.
            gard_id = obj.get("gardId")
            present_pubmed_ids = [
                pubmed_id
                for pubmed_id in obj.get("pubmed_id_list", [])
                if pubmed_id in article_flags
            ]

            total_articles = len(present_pubmed_ids)
            count_epi_articles = sum(article_flags[pubmed_id]["is_epi"] for pubmed_id in present_pubmed_ids)
            count_nhs_articles = sum(article_flags[pubmed_id]["is_nhs"] for pubmed_id in present_pubmed_ids)

            if total_articles == 0:
                continue

            chunks.append({
                "gardId": gard_id,
                "countEpiArticles": count_epi_articles,
                "countNhsArticles": count_nhs_articles,
            })

            self.logger.info(
                f"gard_id: {gard_id}, total_new_articles: {total_articles}, "
                f"countEpiArticles: {count_epi_articles}, countNhsArticles: {count_nhs_articles}"
            )

        return chunks

import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Update GARD nodes with new EPI/NHS article counts.

For each GARD ID that has new publication articles, count the new
EPI/NHS rows from publication_article (is_new = 1) and add those counts
to the existing GARD node countEpiArticles and countNhsArticles properties.
"""

# Reference: C_publication/initializer/x_epi_nhs_count.py


class GardPublicationEpiNhsCountUpdateTask(PipelineBase):
    """Increment GARD EPI/NHS publication counters from new publication articles."""

    BATCH_SIZE = 100

    ''' Implements an incremental update so it adds new counts to existing GARD node totals instead of overwriting them'''
    BATCH_UPDATE = '''
        UNWIND $chunks AS chunk
        MATCH (d:GARD {gardId: chunk.gardId})
        SET
            d.countEpiArticles = coalesce(d.countEpiArticles, 0) + chunk.countEpiArticles,
            d.countNhsArticles = coalesce(d.countNhsArticles, 0) + chunk.countNhsArticles
    '''

    # Start from new publication rows, then find the touched GARD IDs through
    # the mapping table.
    FETCH_GARD_IDS_QUERY = '''
        SELECT DISTINCT pgspm.gard_id
        FROM publication_article AS pa
        INNER JOIN publication_gard_searchterm_pubmed_mapping AS pgspm
            ON pa.pubmed_id = pgspm.pubmed_id
        WHERE pa.is_new = 1
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("GardPublicationEpiNhsCountUpdateTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Find affected GARD IDs, calculate new article counts, and update graph nodes."""

        gard_cursor = None
        count_cursor = None
        total = 0
        batch_num = 0

        try:
            gard_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            count_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            gard_cursor.execute(self.FETCH_GARD_IDS_QUERY)

            while True:
                rows = gard_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break
                
                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                gard_ids = [row["gard_id"] for row in rows if row.get("gard_id")]

                if not gard_ids:
                    self.logger.info("No valid GARD IDs found in this batch.")
                    continue

                # Count EPI/NHS flags for this batch of GARD IDs before sending
                # the compact counter deltas to Memgraph.
                chunks = self._get_epi_nhs_count_chunks(count_cursor, gard_ids)
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
            if gard_cursor:
                gard_cursor.close()

            if count_cursor:
                count_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _get_epi_nhs_count_chunks(self, cursor, gard_ids):
        """Aggregate new EPI/NHS article counts for a batch of GARD IDs."""

        placeholders = ",".join(["%s"] * len(gard_ids))

        # The mapping table can contain multiple rows for the same
        # gard_id/pubmed_id through different search terms. Count distinct
        # article mappings first, then join to new articles so each PMID only
        # contributes once per GARD ID.
        count_query = f'''
        
            SELECT
                mapping.gard_id,
                COUNT(mapping.pubmed_id) AS total_articles,
                SUM(article.is_epi) AS countEpiArticles,
                SUM(article.is_nhs) AS countNhsArticles

            FROM                 
                (
                    SELECT DISTINCT gard_id, pubmed_id
                    FROM publication_gard_searchterm_pubmed_mapping
                    WHERE gard_id IN ({placeholders})
                    AND pubmed_id IS NOT NULL 
                ) AS mapping

                INNER JOIN 
                
                (
                    SELECT
                        pubmed_id,
                        MAX(LOWER(COALESCE(is_EPI, '')) IN ('1', 'true')) AS is_epi,
                        MAX(LOWER(COALESCE(is_NHS, '')) IN ('1', 'true')) AS is_nhs
                    FROM publication_article
                    WHERE is_new = 1
                    GROUP BY pubmed_id 
                ) AS article

                ON article.pubmed_id = mapping.pubmed_id
                
            GROUP BY mapping.gard_id
        '''

        cursor.execute(count_query, gard_ids)
        results = cursor.fetchall()

        chunks = []

        for result in results:
            # Each chunk is an incremental delta that BATCH_UPDATE adds to the
            # existing GARD node counters.
            gard_id = result.get("gard_id")
            total_articles = result.get("total_articles")
            count_epi_articles = int(result.get("countEpiArticles") or 0)
            count_nhs_articles = int(result.get("countNhsArticles") or 0)

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

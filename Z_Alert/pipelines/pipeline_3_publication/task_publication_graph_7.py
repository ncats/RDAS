import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Update GARD nodes with EPI/NHS article counts.

This follows the same mechanism as C_publication/initializer/x_epi_nhs_count.py:
batch GARD IDs from publication_gard_searchterm_pubmed_mapping, calculate the
current full MySQL totals, and write those totals to the GARD node properties.
"""

# Reference: C_publication/initializer/x_epi_nhs_count.py


class GardPublicationEpiNhsCountUpdateTask(PipelineBase):
    """Refresh GARD EPI/NHS publication counters from publication articles."""

    BATCH_SIZE = 300

    ''' Write the computed count values to the GARD node counters. '''
    BATCH_UPDATE = '''
        UNWIND $chunks AS chunk
        MATCH (d:GARD {gardId: chunk.gardId})
        SET
            d.countEpiArticles = chunk.countEpiArticles,
            d.countNhsArticles = chunk.countNhsArticles
    '''

    # Match the initializer by refreshing every mapped GARD ID. The grouped
    # count query below recalculates the full current totals for each batch.
    FETCH_GARD_IDS_QUERY = '''
        SELECT DISTINCT gard_id
        FROM publication_gard_searchterm_pubmed_mapping
        WHERE gard_id IS NOT NULL
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("GardPublicationEpiNhsCountUpdateTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Recalculate current EPI/NHS article totals and update graph nodes."""

        get_gard_ids_cursor = None
        count_cursor = None
        total = 0
        batch_num = 0

        try:
            get_gard_ids_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            count_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            get_gard_ids_cursor.execute(self.FETCH_GARD_IDS_QUERY)

            while True:
                rows = get_gard_ids_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more rows to fetch.")
                    break
                
                batch_num += 1
                self.logger.info(f"--- batch# = {batch_num} ---")

                gard_ids = [row["gard_id"] for row in rows if row.get("gard_id")]

                if not gard_ids:
                    self.logger.info("No valid GARD IDs found in this batch.")
                    continue

                placeholders = ",".join(["%s"] * len(gard_ids))
                get_counts_query = f'''
                    SELECT
                        pgspm.gard_id,
                        COUNT(a.pubmed_id) AS total_articles,
                        SUM(a.is_EPI = 1) AS countEpiArticles,
                        SUM(a.is_NHS = 1) AS countNhsArticles
                    FROM publication_gard_searchterm_pubmed_mapping pgspm
                    LEFT JOIN publication_article a
                        ON pgspm.pubmed_id = a.pubmed_id
                    WHERE pgspm.gard_id IN ({placeholders})
                    GROUP BY pgspm.gard_id
                '''

                count_cursor.execute(get_counts_query, gard_ids)
                results = count_cursor.fetchall()
                chunks = []

                for result in results:
                    gard_id = result["gard_id"]
                    total_articles = result["total_articles"]
                    count_epi_articles = result["countEpiArticles"]
                    count_nhs_articles = result["countNhsArticles"]

                    chunks.append({
                        "gardId": gard_id,
                        "countEpiArticles": int(count_epi_articles or 0),
                        "countNhsArticles": int(count_nhs_articles or 0),
                    })

                    self.logger.info(
                        f"gard_id: {gard_id}, total_articles: {total_articles}, "
                        f"countEpiArticles: {count_epi_articles}, countNhsArticles: {count_nhs_articles}"
                    )

                if not chunks:
                    self.logger.info("No EPI/NHS count chunks to update in Memgraph.")
                    continue

                try:
                    self.memgraph.execute(self.BATCH_UPDATE, {"chunks": chunks})

                    total += len(chunks)
                    self.logger.info(f"Updated {len(chunks)} GARD EPI/NHS count totals in Memgraph. Total = {total}")

                except Exception as e:
                    self.logger.error(f"Error executing GARD EPI/NHS count batch update: {e}")

        except Exception as e:
            self.logger.error(f"Error updating GARD EPI/NHS article counts in Memgraph: {e}")

        finally:
            if get_gard_ids_cursor:
                get_gard_ids_cursor.close()

            if count_cursor:
                count_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()

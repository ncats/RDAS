import csv
import os
import sys
from io import StringIO
from typing import List

import requests

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Mark Article nodes that are GeneReviews.
This task should run after Article nodes have been written to Memgraph.
"""
# Reference: G_update/updater/article_is_gene_review_updater.py


class ArticleGeneReviewFlagUpdateTask(PipelineBase):

    GENE_REVIEWS_TITLE_URL = "https://ftp.ncbi.nih.gov/pub/GeneReviews/GRtitle_shortname_NBKid.txt"
    
    BATCH_SIZE = 100

    BATCH_UPDATE_GENE_REVIEW_SQL = '''
        UNWIND $pmids AS pmid
        MATCH (a:Article {pubmedId: pmid})
        SET a.isGeneReview = true
        RETURN count(a) AS updated_count
    '''

    def __init__(self):
        super().__init__(init_mysql=False, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        self.logger.info("ArticleGeneReviewFlagUpdateTask does not use find_new_data().")


    # implement
    def process_new_data(self) -> None:

        try:
            pmids = self.fetch_gene_review_pmids()

            if not pmids:
                self.logger.info("No GeneReviews PMIDs found. No Article nodes were updated.")
                return

            self.logger.info(f"Retrieved {len(pmids)} GeneReviews PMIDs from NCBI.")

            total_updated = 0
            total_batches = (len(pmids) + self.BATCH_SIZE - 1) // self.BATCH_SIZE

            for batch_num, start in enumerate(range(0, len(pmids), self.BATCH_SIZE), 1):
                batch = pmids[start:start + self.BATCH_SIZE]

                try:
                    updated_count = self.update_gene_review_articles(batch)
                    total_updated += updated_count

                    self.logger.info(
                        f"Batch #{batch_num}/{total_batches}: "
                        f"updated {updated_count} Article nodes. Total updated = {total_updated}"
                    )

                except Exception as e:
                    self.logger.error(f"Error updating GeneReview Article nodes in batch #{batch_num}: {e}")

            self.logger.info(f"Completed GeneReview Article update. Total Article nodes updated = {total_updated}")

        except Exception as e:
            self.logger.error(f"ArticleGeneReviewFlagUpdateTask failed: {e}")

        finally:
            ''' Explicitly close all db connections. '''
            self.close()


    def fetch_gene_review_pmids(self) -> List[int]:

        try:
            response = requests.get(self.GENE_REVIEWS_TITLE_URL, timeout=30)

            if response.status_code >= 400:
                self.logger.error(
                    f"Error fetching GeneReviews title file: HTTP {response.status_code}"
                )
                return []

        except requests.RequestException as e:
            self.logger.error(f"Error fetching GeneReviews title file: {e}")
            return []

        try:
            reader = csv.DictReader(StringIO(response.text), delimiter="\t")
            pmids = []

            for row in reader:
                normalized_row = {
                    (key or "").lstrip("#"): value
                    for key, value in row.items()
                }

                pmid = normalized_row.get("PMID")

                if pmid and pmid.isdigit():
                    pmids.append(int(pmid))

            return pmids

        except Exception as e:
            self.logger.error(f"Error parsing GeneReviews title file: {e}")
            return []


    def update_gene_review_articles(self, pmids: List[int]) -> int:

        if not pmids:
            return 0

        result = self.memgraph.execute_and_fetch(self.BATCH_UPDATE_GENE_REVIEW_SQL, {"pmids": pmids})

        updated_count = 0
        for row in result:
            updated_count += row.get("updated_count", 0)

        return updated_count

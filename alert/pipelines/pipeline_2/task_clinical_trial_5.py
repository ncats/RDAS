import os
import sys
import json
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])


from pipelines.pipeline_base import PipelineBase
from utils.publication_worker import PublicationWorker
from utils.tools import _clean

"""
Find clinical-trial PMIDs that exist in clinical_trial_nctid_pmids_mapping but
are not present in PUBLICATION_ARTICLE table.

For each missing PMID, download the publication metadata from Europe PMC and
store the article row in UPDATE_publication_article for the alert workflow.
"""
# Reference: B_clinical_trial/init_6_clinical_trial_pmids_not_in_Article_umlti.py

class ClinicalTrialPmidArticleImportTask(PipelineBase):

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)

        self.publication_worker = PublicationWorker()


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("ClinicalTrialPmidArticleImportTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:

        ''' 1 '''
        query = '''
            SELECT DISTINCT ctnp.pmid
            FROM  rdas_db.clinical_trial_nctid_pmids_mapping ctnp

            LEFT JOIN rdas_db.publication_article pa
            ON ctnp.pmid = pa.pubmed_id

            WHERE ctnp.is_new = 1
            AND pa.pubmed_id IS NULL
        '''

        ''' 2 '''
        insert_new_article_sql = self.publication_worker.get_insert_sql("update_publication_article")

        count = 0
        batch_num = 0
        batch_size = 100

        try:
            insert_article_cursor = self.mysql.cursor(buffered=True)

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(query)

            while True:
                rows = fetch_cursor.fetchmany(batch_size)

                if not rows:
                    self.logger.info(f"No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f'\n--- batch# = {batch_num} ---')

                pubmed_ids = [row['pmid'] for row in rows]

                count += len(pubmed_ids)
                self.logger.info(f'Total count = {count}')

                for pubmed_id in pubmed_ids:

                    '''the pubmed_id is NOT in publication_article, download article '''
                    article_val = self.publication_worker.download_by_pmid(pubmed_id)

                    if not article_val:
                        self.logger.error(f"Unable to download Article of pubmed_id = {pubmed_id}" )
                        continue

                    ''' save the new article into update_publication_article table '''
                    insert_article_cursor.execute(insert_new_article_sql, article_val)

                    self.mysql.commit()
                    self.logger.info(f"Insert into update_publication_article :: pubmed_id: {pubmed_id}")

        except Exception as err:
            self.logger.error(f"Error: {err}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            # Explicitly close the all the db connections
            self.close()

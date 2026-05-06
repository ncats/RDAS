import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Copy new publication rows from update_publication_article into publication_article.
"""


class NewPublicationArticleImportTask(PipelineBase):

    '''
    publication_article does not have is_new or alert_sent, so only copy the
    columns that exist in both tables.
    '''
    INSERT_NEW_PUBLICATIONS_SQL = '''
        INSERT INTO publication_article (
            pubmed_id,
            doi,
            title,
            abstract_text,
            affiliation,
            first_publication_date,
            publication_year,
            cited_by_count,
            is_open_access,
            in_EPMC,
            in_PMC,
            is_EPI,
            epi_probability,
            is_NHS,
            has_PDF,
            pub_type,
            source,
            source_json,
            epi_extract,
            processed,
            created
        )
        SELECT
            pubmed_id,
            doi,
            title,
            abstract_text,
            affiliation,
            first_publication_date,
            publication_year,
            cited_by_count,
            is_open_access,
            in_EPMC,
            in_PMC,
            is_EPI,
            epi_probability,
            is_NHS,
            has_PDF,
            pub_type,
            source,
            source_json,
            epi_extract,
            processed,
            created
        FROM update_publication_article
        WHERE is_new = 1
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("NewPublicationArticleImportTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:

        cursor = None

        try:
            cursor = self.mysql.cursor()

            cursor.execute(self.INSERT_NEW_PUBLICATIONS_SQL)
            self.mysql.commit()

            self.logger.info(
                f"Copied {cursor.rowcount} rows from update_publication_article "
                "to publication_article where is_new = 1."
            )

        except Exception as e:
            self.logger.error(f"Error copying new publications into publication_article: {e}")

            if self.mysql:
                self.mysql.rollback()

            raise

        finally:
            if cursor:
                cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()

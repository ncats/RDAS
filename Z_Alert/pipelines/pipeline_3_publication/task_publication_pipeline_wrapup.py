import os
import sys
from typing import Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Final publication pipeline step.

After the publication MySQL and Memgraph update tasks finish processing new
publication data, reset the publication-related is_new flags so the same rows
are not processed again in the next alert run.
"""


class PublicationPipelineWrapUpTask(PipelineBase):

    '''
    Publication pipeline tables that are expected to participate in the
    is_new workflow.

    publication_article:
        Main publication table. Rows discovered during the current alert run
        are marked is_new = 1 until the publication pipeline finishes.

    publication_gard_omim_mapping:
        GARD/OMIM pairs introduced during the current alert run and consumed by
        the OMIM publication retrieval task.

    publication_omim:
        Newly retrieved OMIM entry_json rows used to create OMIMRef graph data.
    '''
    PUBLICATION_TABLES_WITH_IS_NEW_COLUMN: Tuple[str, ...] = (
        'publication_omim',
        'publication_article',
        'publication_gard_omim_mapping',
    )

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("PublicationPipelineWrapUpTask does not use find_new_data().")


    def process_new_data(self) -> None:

        cursor = None

        try:
            cursor = self.mysql.cursor()

            for table_name in self.PUBLICATION_TABLES_WITH_IS_NEW_COLUMN:

                '''
                Check the column first so schema drift does not break the whole
                pipeline. Only tables with an is_new column are updated.
                '''
                if not self._table_has_is_new_column(cursor, table_name):
                    self.logger.info(f"Skipped {table_name}; no is_new column found.")
                    continue

                try:
                    '''
                    Reset only rows still marked as new. Rows already at 0 are
                    left untouched, which keeps the update small and clear.
                    '''
                    update_sql = f'''
                        UPDATE {table_name}
                        SET is_new = 0
                        WHERE is_new = 1
                    '''

                    cursor.execute(update_sql)
                    self.mysql.commit()

                    self.logger.info(f"Updated {cursor.rowcount} rows in {table_name}; set is_new = 0.")

                except Exception as e:
                    self.logger.error(f"Error resetting is_new for {table_name}: {e}")

                    if self.mysql:
                        self.mysql.rollback()

            self.logger.info("Completed publication final is_new reset.")

        except Exception as e:
            self.logger.error(f"Error resetting publication is_new flags: {e}")

            if self.mysql:
                self.mysql.rollback()

        finally:
            if cursor:
                cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def _table_has_is_new_column(self, cursor, table_name: str) -> bool:

        '''
        Confirm the target table has an is_new column in the selected database.
        The table names are from the class constant, so the later UPDATE remains
        constrained to known publication pipeline tables.
        '''
        column_check_sql = '''
            SELECT COUNT(*)
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = %s
            AND COLUMN_NAME = 'is_new'
        '''

        cursor.execute(column_check_sql, (table_name,))
        row = cursor.fetchone()

        return bool(row and row[0])

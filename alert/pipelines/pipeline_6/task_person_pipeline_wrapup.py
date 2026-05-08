import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Final person pipeline step.

After the person extraction, grouping, and graph update tasks finish processing
new person rows, reset person_of_all_sources.is_new so the same people are not
processed again in the next alert run.
"""


class PersonPipelineWrapUpTask(PipelineBase):

    RESET_PERSON_IS_NEW_SQL = '''
        UPDATE person_of_all_sources
        SET is_new = 0
        WHERE is_new = 1
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("PersonPipelineWrapUpTask does not use find_new_data().")


    def process_new_data(self) -> None:

        cursor = None

        try:
            cursor = self.mysql.cursor()
            cursor.execute(self.RESET_PERSON_IS_NEW_SQL)
            self.mysql.commit()

            self.logger.info(
                f"Updated {cursor.rowcount} rows in person_of_all_sources; set is_new = 0."
            )

        except Exception as e:
            self.logger.error(f"Error resetting person_of_all_sources.is_new: {e}")

            if self.mysql:
                self.mysql.rollback()

        finally:
            if cursor:
                cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()

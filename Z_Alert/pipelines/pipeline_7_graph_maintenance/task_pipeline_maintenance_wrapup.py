import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Final graph maintenance pipeline step.

After organization location ROR lookup, graph sync, and source tracking finish,
reset organization_location.is_new so later maintenance runs do not replay the
same staged organization-location rows.
"""


class OrganizationLocationMaintenanceWrapUpTask(PipelineBase):
    """Clear organization_location staging flags after maintenance consumes them."""

    RESET_ORGANIZATION_LOCATION_IS_NEW_SQL = '''
        UPDATE organization_location
        SET is_new = 0
        WHERE is_new = 1
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("OrganizationLocationMaintenanceWrapUpTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Reset organization_location.is_new after graph maintenance completes."""

        cursor = None

        try:
            cursor = self.mysql.cursor()
            cursor.execute(self.RESET_ORGANIZATION_LOCATION_IS_NEW_SQL)
            self.mysql.commit()

            self.logger.info(
                f"Updated {cursor.rowcount} rows in organization_location; set is_new = 0."
            )

        except Exception as e:
            self.logger.error(f"Error resetting organization_location.is_new: {e}")

            if self.mysql:
                self.mysql.rollback()

            raise

        finally:
            if cursor:
                cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()

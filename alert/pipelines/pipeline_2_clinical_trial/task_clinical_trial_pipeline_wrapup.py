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
Final clinical-trial pipeline step.

After the MySQL and Memgraph update tasks finish processing new clinical-trial data,
reset the clinical-trial related is_new flags so the same rows are not processed
again in the next alert run.
"""

class ClinicalTrialPipelineWrapUpTask(PipelineBase):

    '''
    Tables from rdas_db_schema.sql that are clinical-trial related and have
    an `is_new` column.
    '''
    CLINICAL_TRIAL_TABLES_WITH_S_NEW_COLUMN: Tuple[str, ...] = (        
        'clinical_trial',
        'clinical_trial_unique',
        'clinical_trial_intervention_drug',
        'clinical_trial_nctid_pmids_mapping',
        'clinical_trial_annotation',
    )

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("ClinicalTrialPipelineWrapUpTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:

        cursor = None

        try:
            cursor = self.mysql.cursor()

            for table_name in self.CLINICAL_TRIAL_TABLES_WITH_S_NEW_COLUMN:

                update_sql = f'''
                    UPDATE {table_name}
                    SET is_new = 0
                    WHERE is_new = 1
                '''

                cursor.execute(update_sql)
                self.mysql.commit()

                self.logger.info(f"Updated {cursor.rowcount} rows in {table_name}; set is_new = 0.")
            
            self.logger.info("Completed clinical trial final is_new reset.")

        except Exception as e:
            self.logger.error(f"Error resetting clinical trial is_new flags: {e}")

            if self.mysql:
                self.mysql.rollback()

        finally:
            if cursor:
                cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()

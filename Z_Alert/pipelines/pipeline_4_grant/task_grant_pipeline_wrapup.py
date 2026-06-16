"""
Final grant pipeline step: reset grant-related `is_new` flags.

Earlier grant alert tasks mark rows with `is_new = 1` so downstream MySQL,
Memgraph, and follow-up tasks can process only the current alert batch. After
those tasks finish, this wrap-up resets the flags to `0` so the same rows are
not processed again in the next grant alert run.

This task only changes the `is_new` column. It does not reset `processed`,
`project_annotation_processed`, or `pmid_processed`, because those columns track
different stage-specific work queues.

Tables reset here:
    `grant_project`
        Project rows loaded or refreshed from RePORTER project exports.
    `grant_publication`
        Grant publication export rows loaded or refreshed this run.
    `grant_abstract`
        Grant abstract rows loaded or refreshed this run.
    `grant_linktable`
        Publication-to-project link rows loaded or refreshed this run.
    `grant_clinical_study`
        Grant clinical study rows loaded or refreshed this run.
    `grant_patent`
        Patent/project rows loaded or refreshed this run.
    `grant_gard_project_relation`
        Derived GARD-to-project relationship matches from task_grant_10.
    `grant_gard_project_relation_unique_application_id`
        Work table for current GARD-related application IDs.
    `grant_project_annotation`
        Derived UMLS annotation rows from task_grant_11.
    `publication_article`
        Non-grant table touched by task_grant_12 when it downloads grant-linked
        PMIDs that are missing from `publication_article`.
"""

import os
import sys
from typing import Dict, Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from pipelines.pipeline_base import PipelineBase


class GrantPipelineWrapUpTask(PipelineBase):
    """Reset `is_new` flags for tables used by the grant alert pipeline."""

    GRANT_TABLES_WITH_IS_NEW_COLUMN: Tuple[str, ...] = (
        "grant_project",
        "grant_publication",
        "grant_abstract",
        "grant_linktable",
        "grant_clinical_study",
        "grant_patent",
        "grant_gard_project_relation",
        "grant_gard_project_relation_unique_application_id",
        "grant_project_annotation",
        #"publication_article",
    )

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("GrantPipelineWrapUpTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Reset every whitelisted grant-related table from `is_new = 1` to `0`."""

        cursor = None
        summary: Dict[str, int] = {
            "tables_seen": 0,
            "tables_updated": 0,
            "tables_skipped": 0,
            "tables_failed": 0,
            "rows_reset": 0,
        }

        try:
            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            cursor = self.mysql.cursor()

            for table_name in self.GRANT_TABLES_WITH_IS_NEW_COLUMN:
                summary["tables_seen"] += 1

                # Check the column before updating. This keeps a schema mismatch
                # in one table from breaking the rest of the grant wrap-up.
                if not self._table_has_is_new_column(cursor, table_name):
                    summary["tables_skipped"] += 1
                    self.logger.info(f"Skipped {table_name}; no is_new column found.")
                    continue

                try:
                    reset_count = self._reset_table_is_new(cursor, table_name)
                    self.mysql.commit()

                    summary["tables_updated"] += 1
                    summary["rows_reset"] += reset_count
                    self.logger.info(f"Updated {reset_count} row(s) in {table_name}; set is_new = 0.")

                except Exception:
                    summary["tables_failed"] += 1
                    self.mysql.rollback()
                    self.logger.exception(f"Error resetting is_new for {table_name}. Continuing with next table.")
                    continue

            self.logger.info(f"Completed grant final is_new reset. Summary={summary}")

        except Exception:
            if self.mysql:
                self.mysql.rollback()

            self.logger.exception(f"Error resetting grant is_new flags. Summary={summary}")
            return

        finally:
            if cursor:
                cursor.close()

            self.close()


    def _table_has_is_new_column(self, cursor, table_name: str) -> bool:
        """
        Confirm the target table has an `is_new` column in the selected database.

        Table names come from the class whitelist, so the dynamic UPDATE remains
        constrained to known grant-pipeline tables.
        """

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


    def _reset_table_is_new(self, cursor, table_name: str) -> int:
        """Reset one whitelisted table and return the number of changed rows."""

        update_sql = f'''
            UPDATE `{table_name}`
            SET is_new = 0
            WHERE is_new = 1
        '''
        cursor.execute(update_sql)
        return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

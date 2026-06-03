import os
import sys
import time
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

load_dotenv(os.path.abspath(os.path.join(_dir, "..", ".env")))

from baseclass.conn import DBConnection as db
from utils.applogger import AppLogger
from utils.organization_name_extractor import OrganizationNameExtractor
from utils.tools import _time_hms

# *** Update the existing data in organization_location table ***
"""
Extract a cleaner organization name for organization_location rows that do not have a ROR match yet.
fetch_sql = f'''
            SELECT id, original_name_in_graph_db
            FROM {self.TABLE_NAME}
            WHERE ror_id IS NULL
            AND processed IS NULL
            ORDER BY id
            LIMIT %s
        '''
The raw graph value is stored in original_name_in_graph_db. It may contain a department, address, person text, or other affiliation text. 
This task sends that raw value to the configured local model and stores the returned organization name in model_extracted_name
and a deterministic hash key in model_extracted_name_hash_key. If the model request fails, the row is marked with
ror_id = 'N/A' so the same timeout/error row is not fetched forever.
"""

class OrganizationNameExtractionTask:
    
    TABLE_NAME = "organization_location"
    PROCESSED_FLAG = "llama3.1_org_name_extracted"
    ROR_NOT_AVAILABLE = "N/A"
    BATCH_SIZE = 200


    def __init__(self):

        self.mysql = db().mysql_conn()

        '''
        Use the same alert log directory convention as the alert pipeline, but
        keep this updater independent from PipelineBase.
        '''
        self.log_dir = os.path.expanduser(os.getenv("ALERT_LOG_DIR", "logs"))
        os.makedirs(self.log_dir, exist_ok=True)

        class_name = type(self).__name__
        self.log_file = f"{self.log_dir}/alert-{class_name}.log"
        self.logger = AppLogger(class_name, self.log_file).get_logger()
        self.logger.info(f'\n\n{"*" * 20} The {class_name} is initialized. {"*" * 20}\n')

        self.org_name_extractor = OrganizationNameExtractor(logger=self.logger)
        self.processed_with = self.org_name_extractor.model_name


    def find_new_data(self, gard_node) -> None:
        self.logger.info("OrganizationNameExtractionTask does not use find_new_data().")


    def close(self) -> None:
        """Close this updater's MySQL connection and logger handlers."""

        if getattr(self, "org_name_extractor", None) is not None:
            close_extractor = getattr(self.org_name_extractor, "close", None)

            if callable(close_extractor):
                close_extractor()

        if self.mysql is not None and self.mysql.is_connected():
            self.mysql.close()

        self.mysql = None

        if hasattr(self, "logger") and self.logger is not None:
            for handler in list(self.logger.handlers):
                handler.flush()
                handler.close()
                self.logger.removeHandler(handler)

            self.logger = None


    def process_new_data(self) -> None:
        """
        Load organization_location rows with ror_id IS NULL, ask the configured model for a
        clean organization name, and update model_extracted_name for each
        processed row.
        """

        total_fetched = 0
        total_updated = 0
        batch_num = 0
        start_time = time.time()
        update_cursor = None

        try:
            '''
            Create the update cursor once for the whole task. The old code
            created one update cursor per batch. That is not as expensive as
            creating one cursor per row, but reusing one cursor keeps the batch
            loop lighter and simpler.
            '''
            update_cursor = self.mysql.cursor()

            '''
            Step 1:
            Fetch only one small batch each time. This avoids creating one huge
            buffered MySQL result set when organization_location has many rows.
            Each saved row gets processed filled in, so the next SELECT returns
            the next unprocessed batch.
            '''
            while True:
                rows = self.fetch_organization_batch()

                if not rows:
                    self.logger.info("No more organization_location rows need model-based name extraction.")
                    break

                batch_num += 1
                self.logger.info(f'\n\n------ batch #: {batch_num} ------\n')

                batch_start = time.time()
                total_fetched += len(rows)
                self.logger.info(f'total_fetched: {total_fetched}')

                '''
                Step 2:
                Extract names row by row. This is the simpler path that avoids
                large JSON prompts and response parsing overhead from the
                previous model sub-batch version.
                '''
                update_tuples = self.build_organization_name_update_tuples(rows)

                '''
                Step 3:
                Save rows that can be marked processed. Rows with blank source
                text are marked with null extracted fields. Rows where the
                model request failed are marked with ror_id = 'N/A' and
                processed, which prevents repeated model calls for rows that
                keep timing out.
                '''
                if not update_tuples:
                    self.logger.error( f"Batch #{batch_num}: no rows could be updated. Stopping to avoid repeatedly fetching the same unprocessed rows.")
                    break

                updated_count = self.update_organization_names(update_cursor, update_tuples)

                total_updated += updated_count

                hours, minutes, seconds = _time_hms(time.time() - batch_start)
                self.logger.info(
                    f"Batch #{batch_num}: fetched={len(rows)}, updated={updated_count}, "
                    f"total_updated={total_updated}, "
                    f"time={hours} hours, {minutes} minutes, {seconds} seconds."
                )

            total_hours, total_minutes, total_seconds = _time_hms(time.time() - start_time)
            self.logger.info(
                "Completed OrganizationNameExtractionTask: "
                f"fetched={total_fetched}, updated={total_updated}, "
                f"time={total_hours} hours, {total_minutes} minutes, {total_seconds} seconds."
            )

        except Exception as e:
            self.logger.error(f"OrganizationNameExtractionTask failed: {e}")

        finally:
            if update_cursor:
                update_cursor.close()

            ''' Explicitly close the MySQL connection and logger handlers. '''
            self.close()
 

    def fetch_organization_batch(self) -> List[Dict[str, Any]]:
        """
        Fetch one small batch of unprocessed organization_location rows.

        LIMIT keeps each database round trip small. ORDER BY id gives stable
        progress, and the processed flag removes saved rows from the next batch.
        FORCE INDEX is intentional: MariaDB otherwise chooses PRIMARY to satisfy
        ORDER BY id, which makes later batches rescan millions of already
        processed rows before it finds the next pending slice.
        """

        fetch_sql = f'''
            SELECT id, original_name_in_graph_db
            FROM {self.TABLE_NAME} FORCE INDEX (gl_ror_processed_idx)
            WHERE ror_id IS NULL
            AND processed IS NULL
            ORDER BY id DESC
            LIMIT %s
        '''

        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            cursor.execute(fetch_sql, (self.BATCH_SIZE,))
            return cursor.fetchall()

        finally:
            if cursor:
                cursor.close()



    def build_organization_name_update_tuples(self, rows: List[Dict[str, Any]]) -> List[Tuple[Any, Any, Any, str, Any, Any]]:
        """Create the SQL update tuples for one fetched batch."""

        update_tuples = []

        for row in rows:

            row_id = row.get("id")
            original_name = row.get("original_name_in_graph_db")
            self.logger.info(f"[id] ={row_id}, [original_name]: {original_name}") 

            if not original_name:
                update_tuples.append((None, None, None, self.PROCESSED_FLAG, self.processed_with, row_id))
                continue

            extracted_name = self.org_name_extractor.extract_organization_name(original_name)

            if extracted_name is None:
                '''
                A None return means the local model request failed, for example
                a localhost Ollama timeout. Mark the row terminal with
                ror_id='N/A' so the same bad request does not block every
                future batch.
                '''
                self.logger.info(f"Marking id={row_id} with ror_id='N/A'; model request failed.")
                update_tuples.append((self.ROR_NOT_AVAILABLE, None, None, self.PROCESSED_FLAG, self.processed_with, row_id))
                continue

            extracted_name = self.org_name_extractor.normalize_extracted_name(extracted_name)

            if not extracted_name:
                update_tuples.append((None, None, None, self.PROCESSED_FLAG, self.processed_with, row_id))
                continue

            self.logger.info(f"[id] ={row_id}, [extracted_name]: {extracted_name}\n") 

            extracted_name_hash_key = self.org_name_extractor.make_extracted_name_hash_key(extracted_name)
            update_tuples.append((None, extracted_name, extracted_name_hash_key, self.PROCESSED_FLAG, self.processed_with, row_id))

        return update_tuples


    def update_organization_names(self, cursor: Any, update_tuples: List[Tuple[Any, Any, Any, str, Any, Any]]) -> int:
        """
        Save extracted names and terminal model failures to organization_location.

        ror_id stays NULL for successful name extraction so the ROR lookup step
        can process the row next. ror_id is set to 'N/A' only when the model
        request itself failed and the row should stop retrying this step.
        """

        if not update_tuples:
            return 0

        update_sql = f'''
            UPDATE {self.TABLE_NAME}
            SET ror_id = %s,
                model_extracted_name = %s,
                model_extracted_name_hash_key = %s,
                processed = %s,
                processed_with = %s
            WHERE id = %s
        '''

        try:
            cursor.executemany(update_sql, update_tuples)
            self.mysql.commit()

            return cursor.rowcount

        except Exception as e:
            self.logger.error(f"Error updating extracted organization names: {e}")

            if self.mysql:
                self.mysql.rollback()

            return 0

if __name__ == "__main__":
    OrganizationNameExtractionTask().process_new_data()

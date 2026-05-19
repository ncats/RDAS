import os
import sys
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
    os.path.abspath(os.path.join(_dir, '../..')),
])

try:
    from colorama import init, Fore, Style
except ModuleNotFoundError:
    class _NoColor:
        RED = GREEN = YELLOW = BLUE = RESET_ALL = ""

    def init():
        pass

    Fore = _NoColor()
    Style = _NoColor()

init()

from baseclass.conn import DBConnection as db
from utils.file_appender import FileAppender
from utils.tools import _date_string


BAD_COST_VALUE = object()


"""
Change Project.totalCost from the formatted dollar string to a numeric value.

ProjectInitializer currently reads grant_project.TOTAL_COST and writes:

    totalCost: _format_dollars(total_cost)

This updater reads the MySQL project cost values, uses DIRECT_COST_AMT + INDIRECT_COST_AMT
when that sum is available, falls back to TOTAL_COST when the sum is NULL, converts the
chosen value to a Python number, and updates only the existing Memgraph Project.totalCost property.
If both cost sources are NULL, Project.totalCost is set to an empty string.
"""


class ProjectTotalCostNumberUpdater:
    """Update existing Project nodes so totalCost stores a number, or blank when cost is missing."""

    BATCH_SIZE = 1000
    MAX_RETRIES = 3
    RETRY_SECONDS = 5

    PROJECT_SOURCE_TABLE = "grant_gard_project_relation_unique_application_id"
    GRANT_PROJECT_TABLE = "grant_project"

    FETCH_PROJECT_COST_QUERY = f'''
        SELECT
            gpru.application_id,
            p.TOTAL_COST AS total_cost_1,
            p.DIRECT_COST_AMT + p.INDIRECT_COST_AMT AS total_cost_2
        FROM {PROJECT_SOURCE_TABLE} AS gpru
        LEFT JOIN {GRANT_PROJECT_TABLE} AS p
            ON gpru.application_id = p.application_id
        WHERE gpru.application_id IS NOT NULL
        ORDER BY gpru.application_id
    '''

    UPDATE_PROJECT_TOTAL_COST_CYPHER = '''
        UNWIND $chunks AS chunk

        // Step 1: Match the existing Project node using the same applicationId used by ProjectInitializer.
        MATCH (p:Project {applicationId: chunk.applicationId})

        // Step 2: Update only Project.totalCost. Do not replace the node map or change any other Project property.
        SET p.totalCost = chunk.totalCost

        // Step 3: Return the number of Project nodes changed in this batch for logging.
        RETURN count(p) AS updated_count
    '''

    def __init__(self):

        # Step 1: Open the shared RDAS MySQL and Memgraph connections from baseclass.conn.
        self.mysql = db().mysql_conn()
        self.memgraph = db().memgraph_conn()

        # Step 2: Create a migration log file so each batch count is written to disk and stdout.
        class_name = type(self).__name__
        self.log_file = f'logs/G-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    def update(self) -> None:
        """Read MySQL project costs and update only Project.totalCost in Memgraph."""

        batch_num = 0
        total_rows_read = 0
        total_chunks_sent = 0
        total_updated = 0
        total_blank_cost = 0
        total_skipped_bad_cost = 0
        fetch_cursor = None

        self.appender.log_stdout(f"Starting Project.totalCost numeric update. Batch size = {self.BATCH_SIZE}.")

        try:
            # Step 1: Read the same MySQL project source used by D_grant/initializer/project.py.
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_PROJECT_COST_QUERY)

            while True:
                # Step 2: Pull one MySQL batch so the updater does not load all projects into memory.
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    break

                batch_num += 1
                total_rows_read += len(rows)

                # Step 3: Convert MySQL rows into the small payload Memgraph needs.
                chunks, blank_cost, skipped_bad_cost = self._build_chunks(rows)

                total_blank_cost += blank_cost
                total_skipped_bad_cost += skipped_bad_cost

                if not chunks:
                    self.appender.log_stdout(
                        f"Batch #{batch_num}: no Project rows to update. "
                        f"Missing costs set blank: {blank_cost}; "
                        f"skipped bad cost: {skipped_bad_cost}."
                    )
                    continue

                # Step 4: Write only the totalCost property to existing Project nodes in Memgraph.
                updated_count = self._update_batch_with_retry(chunks, batch_num)
                total_chunks_sent += len(chunks)
                total_updated += updated_count


            self.appender.log_stdout(
                f"{Fore.GREEN}Finished Project.totalCost numeric update. "
                f"MySQL rows read: {total_rows_read}; "
                f"cost values sent: {total_chunks_sent}; "
                f"Project nodes updated: {total_updated}; "
                f"missing costs set blank: {total_blank_cost}; "
                f"bad costs skipped: {total_skipped_bad_cost}.{Style.RESET_ALL}"
            )

        finally:
            # Step 5: Close the MySQL cursor and both database connections even if a batch fails.
            if fetch_cursor:
                fetch_cursor.close()
 
            self.close()


    def _build_chunks(self, rows: List[Dict[str, Any]]):
        """Convert MySQL rows into Memgraph update chunks."""

        chunks = []
        blank_cost = 0
        skipped_bad_cost = 0

        for row in rows:
            print(row)

            # Step 1: Keep applicationId in the same numeric form used by ProjectInitializer.
            application_id = row.get("application_id")

            if application_id is None:
                continue

            # Step 2: Prefer total_cost_2 because it is calculated from DIRECT_COST_AMT + INDIRECT_COST_AMT.
            # If that sum is NULL, use total_cost_1 from TOTAL_COST as the fallback value.
            selected_total_cost = row.get("total_cost_2")

            if selected_total_cost is None:
                selected_total_cost = row.get("total_cost_1")

            # Step 3: Convert the selected cost to a number without calling _format_dollars.
            total_cost = self._to_number(selected_total_cost)


            # Step 4: If MySQL has no cost value, set Project.totalCost to an empty string.
            if total_cost is None:
                blank_cost += 1
                total_cost = ''

            # Step 5: Skip rows where the cost value exists but cannot be parsed as a number.
            if total_cost is BAD_COST_VALUE:
                skipped_bad_cost += 1
                continue

            # Step 6: Send only applicationId and totalCost, so Cypher cannot modify other Project properties.
            chunks.append({
                "applicationId": application_id,
                "totalCost": total_cost,
            })

        return chunks, blank_cost, skipped_bad_cost


    def _update_batch_with_retry(self, chunks: List[Dict[str, Any]], batch_num: int) -> int:
        """Run one Memgraph update batch with a small retry loop."""

        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                # Step 1: Execute the single-property Project.totalCost update.
                rows = list(self.memgraph.execute_and_fetch(
                    self.UPDATE_PROJECT_TOTAL_COST_CYPHER,
                    {"chunks": chunks}
                ))

                # Step 2: Read the returned count so the log reflects matched Project nodes.
                if not rows:
                    return 0

                return int(rows[0].get("updated_count") or 0)

            except Exception as e:
                last_error = e

                if attempt >= self.MAX_RETRIES:
                    self.appender.log_stdout(
                        f"{Fore.RED}Batch #{batch_num} failed after {attempt} attempts: "
                        f"{e}{Style.RESET_ALL}"
                    )
                    raise

                # Step 3: Wait briefly before retrying transient Memgraph failures.
                self.appender.log_stdout(
                    f"{Fore.YELLOW}Batch #{batch_num} attempt {attempt}/{self.MAX_RETRIES} "
                    f"failed: {e}. Retrying in {self.RETRY_SECONDS} seconds..."
                    f"{Style.RESET_ALL}"
                )
                time.sleep(self.RETRY_SECONDS)

        raise last_error


    @staticmethod
    def _to_number(value: Any) -> Optional[Any]:
        """Convert MySQL TOTAL_COST to int or float, returning None for blank values."""

        # Step 1: Treat NULL and blank strings as missing costs.
        if value is None:
            return None

        if isinstance(value, str) and value.strip() == "":
            return None

        # Step 2: Preserve numeric database values as numbers.
        if isinstance(value, int) and not isinstance(value, bool):
            return value

        if isinstance(value, float):
            return int(value) if value.is_integer() else value

        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral_value() else float(value)

        # Step 3: Parse text-like numeric values without adding dollar formatting.
        try:
            number = Decimal(str(value).strip().replace(",", ""))
        except (InvalidOperation, ValueError):
            return BAD_COST_VALUE

        # Step 4: Store whole-dollar project costs as int; keep decimal costs as float if they appear.
        return int(number) if number == number.to_integral_value() else float(number)


    def close(self) -> None:
        """Close open connections and the log appender."""

        # Step 1: Close the MySQL connection opened by this updater.
        if self.mysql:
            self.mysql.close()

        # Step 2: Close Memgraph only if the gqlalchemy client exposes a close method.
        close_memgraph = getattr(self.memgraph, "close", None)

        if callable(close_memgraph):
            close_memgraph()

        # Step 3: Close the log file last so final status messages are flushed.
        self.appender.close()

 

if __name__ == "__main__":
    # Step 1: Run the updater directly with the built-in constants; no CLI arguments are required.
    ProjectTotalCostNumberUpdater().update()

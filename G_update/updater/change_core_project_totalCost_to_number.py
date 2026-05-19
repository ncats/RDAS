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
Add numeric totalCost values to existing CoreProject nodes.

CoreProjectInitializer creates CoreProject nodes by coreProjectNumber and originally
writes totalCost using _format_dollars. This updater recalculates the CoreProject
cost from MySQL grant_project rows, stores it as a number, and updates only the
CoreProject.totalCost property in Memgraph.
"""


class CoreProjectTotalCostUpdater:
    """Update existing CoreProject nodes so totalCost stores a numeric summed cost."""

    BATCH_SIZE = 1000
    MAX_RETRIES = 3
    RETRY_SECONDS = 5

    FETCH_CORE_PROJECT_COST_QUERY = '''
        SELECT
            p.core_project_num,
            SUM(p.TOTAL_COST) AS total_cost_1,
            SUM(p.DIRECT_COST_AMT + p.INDIRECT_COST_AMT) AS total_cost_2
        FROM rdas_db.grant_project AS p
        WHERE p.core_project_num IS NOT NULL
        GROUP BY p.core_project_num
    '''

    UPDATE_CORE_PROJECT_TOTAL_COST_CYPHER = '''
        UNWIND $chunks AS chunk

        // Step 1: Match the existing CoreProject node using the same key used by CoreProjectInitializer.
        MATCH (cp:CoreProject {coreProjectNumber: chunk.coreProjectNumber})

        // Step 2: Update only CoreProject.totalCost. Do not change any other CoreProject property.
        SET cp.totalCost = chunk.totalCost

        // Step 3: Return the number of CoreProject nodes changed in this batch for logging.
        RETURN count(cp) AS updated_count
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
        """Read summed MySQL costs and update only CoreProject.totalCost in Memgraph."""

        start_time = time.time()
        batch_num = 0
        total_rows_read = 0
        total_chunks_sent = 0
        total_updated = 0
        total_skipped_missing_cost = 0
        total_skipped_bad_cost = 0
        fetch_cursor = None

        self.appender.log_stdout(
            f"Starting CoreProject.totalCost numeric update. Batch size = {self.BATCH_SIZE}."
        )

        try:
            # Step 1: Read one row per core_project_num from MySQL grant_project.
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_CORE_PROJECT_COST_QUERY)

            while True:
                # Step 2: Pull one MySQL batch so the updater does not load all CoreProject rows into memory.
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    break

                batch_num += 1
                batch_start = time.time()
                total_rows_read += len(rows)

                # Step 3: Convert MySQL rows into the small payload Memgraph needs.
                chunks, skipped_missing_cost, skipped_bad_cost = self._build_chunks(rows)
                total_skipped_missing_cost += skipped_missing_cost
                total_skipped_bad_cost += skipped_bad_cost

                if not chunks:
                    self.appender.log_stdout(
                        f"Batch #{batch_num}: no rows with numeric totalCost. "
                        f"Skipped missing cost: {skipped_missing_cost}; "
                        f"skipped bad cost: {skipped_bad_cost}."
                    )
                    continue

                # Step 4: Write only the totalCost property to existing CoreProject nodes in Memgraph.
                updated_count = self._update_batch_with_retry(chunks, batch_num)
                total_chunks_sent += len(chunks)
                total_updated += updated_count

                hours, minutes, seconds = self.elapsed_time(batch_start, time.time())
                self.appender.log_stdout(
                    f"Batch #{batch_num}: read {len(rows)} MySQL rows, "
                    f"sent {len(chunks)} numeric costs, updated {updated_count} CoreProject nodes | "
                    f"Batch time: {hours}:{minutes}:{seconds}"
                )

            self.appender.log_stdout(
                f"{Fore.GREEN}Finished CoreProject.totalCost numeric update. "
                f"MySQL rows read: {total_rows_read}; "
                f"numeric costs sent: {total_chunks_sent}; "
                f"CoreProject nodes updated: {total_updated}; "
                f"missing costs skipped: {total_skipped_missing_cost}; "
                f"bad costs skipped: {total_skipped_bad_cost}.{Style.RESET_ALL}"
            )

        finally:
            # Step 5: Close the MySQL cursor and both database connections even if a batch fails.
            if fetch_cursor:
                fetch_cursor.close()

            hours, minutes, seconds = self.elapsed_time(start_time, time.time())
            self.appender.log_stdout(
                f"\n{Fore.BLUE}{'=' * 30} Done. Total updated: {total_updated}. "
                f"Total time: {hours}:{minutes}:{seconds} {'=' * 30}{Style.RESET_ALL}\n"
            )
            self.close()


    def _build_chunks(self, rows: List[Dict[str, Any]]):
        """Convert MySQL core_project_num cost rows into Memgraph update chunks."""

        chunks = []
        skipped_missing_cost = 0
        skipped_bad_cost = 0

        for row in rows:
            # Step 1: Read the CoreProject key used by CoreProjectInitializer.
            core_project_num = row.get("core_project_num")

            if core_project_num is None or str(core_project_num).strip() == "":
                continue

            # Step 2: Prefer total_cost_2 because it is calculated from DIRECT_COST_AMT + INDIRECT_COST_AMT.
            # If that sum is NULL, use total_cost_1 from TOTAL_COST as the fallback value.
            selected_total_cost = row.get("total_cost_2")

            if selected_total_cost is None:
                selected_total_cost = row.get("total_cost_1")

            # Step 3: Convert the selected cost to a number without calling _format_dollars.
            total_cost = self._to_number(selected_total_cost)

            # Step 4: Skip rows where MySQL has no cost value to convert.
            if total_cost is None:
                skipped_missing_cost += 1
                continue

            # Step 5: Skip rows where the cost value exists but cannot be parsed as a number.
            if total_cost is BAD_COST_VALUE:
                skipped_bad_cost += 1
                continue

            # Step 6: Send only coreProjectNumber and totalCost, so Cypher cannot modify other CoreProject properties.
            chunks.append({
                "coreProjectNumber": core_project_num,
                "totalCost": total_cost,
            })

        return chunks, skipped_missing_cost, skipped_bad_cost


    def _update_batch_with_retry(self, chunks: List[Dict[str, Any]], batch_num: int) -> int:
        """Run one Memgraph update batch with a small retry loop."""

        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                # Step 1: Execute the single-property CoreProject.totalCost update.
                rows = list(self.memgraph.execute_and_fetch(
                    self.UPDATE_CORE_PROJECT_TOTAL_COST_CYPHER,
                    {"chunks": chunks}
                ))

                # Step 2: Read the returned count so the log reflects matched CoreProject nodes.
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
        """Convert a MySQL cost value to int or float, returning None for blank values."""

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

        # Step 4: Store whole-dollar costs as int; keep decimal costs as float if they appear.
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


    @staticmethod
    def elapsed_time(start_time: float, end_time: float):
        """Return elapsed time as hours, minutes, seconds."""

        time_diff = end_time - start_time
        hours = int(time_diff // 3600)
        minutes = int((time_diff % 3600) // 60)
        seconds = int(time_diff % 60)

        return hours, minutes, seconds


if __name__ == "__main__":
    # Step 1: Run the updater directly with the built-in constants; no CLI arguments are required.
    CoreProjectTotalCostUpdater().update()

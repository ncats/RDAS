import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
])

from ast import literal_eval
from colorama import init, Fore, Style
init()

import time

from baseclass.init_base import InitBase
from utils.file_appender import FileAppender
from utils.tools import _clean, _date_string, _make_hash_key, elapsed_time


"""
Backfill _composite_key on existing PrimaryOutcome nodes.

New alert pipeline writes PrimaryOutcome nodes with:

    _make_hash_key(f"{measure}|{time_frame}|{description}")

Existing PrimaryOutcome nodes were created by the original initializer without
that key. This updater reads PrimaryOutcome nodes directly, computes the same
key in Python, and writes it back in batches.
"""


class PrimaryOutcomeCompositeKeyUpdater(InitBase):
    """Add _composite_key to existing PrimaryOutcome nodes in Memgraph."""

    DEFAULT_BATCH_SIZE = 5000
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_SECONDS = 10

    FETCH_BATCH = '''
        MATCH (po:PrimaryOutcome)
        WHERE po._composite_key IS NULL
        WITH po
        LIMIT $limit
        RETURN
            id(po) AS node_id,
            coalesce(po.primaryOutcomeMeasure, po.PrimaryOutcomeMeasure, "") AS measure,
            coalesce(po.primaryOutcomeTimeFrame, po.PrimaryOutcomeTimeFrame, "") AS timeFrame,
            coalesce(po.primaryOutcomeDescription, po.PrimaryOutcomeDescription, "") AS description
    '''

    UPDATE_BATCH = '''
        UNWIND $chunks AS chunk
        MATCH (po:PrimaryOutcome)
        WHERE id(po) = chunk.node_id
        AND po._composite_key IS NULL
        SET po._composite_key = chunk._composite_key
        RETURN count(po) AS updated_count
    '''

    def __init__(self):
        super().__init__('', 'PrimaryOutcome')

        self.batch_size = self.DEFAULT_BATCH_SIZE
        self.max_retries = self.DEFAULT_MAX_RETRIES
        self.retry_seconds = self.DEFAULT_RETRY_SECONDS

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/G-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    def init_nodes(self):
        """Override abstract method from InitBase."""

        self.update()


    def update(self):
        """Backfill _composite_key until no more eligible nodes are found."""

        start_time = time.time()
        total_updated = 0
        batch_num = 0

        self.appender.log_stdout(
            f"Starting PrimaryOutcome _composite_key backfill. "
            f"Batch size = {self.batch_size}"
        )

        try:
            self._ensure_composite_key_index()

            while True:
                batch_num += 1
                batch_start = time.time()

                chunks = self._fetch_batch()

                if not chunks:
                    self.appender.log_stdout(
                        f"No more PrimaryOutcome nodes need _composite_key after "
                        f"{batch_num - 1} completed batches."
                    )
                    break

                updated_count = self._update_batch_with_retry(chunks, batch_num)
                total_updated += updated_count

                hours, minutes, seconds = elapsed_time(batch_start, time.time())
                self.appender.log_stdout(
                    f"Batch #{batch_num}: updated {updated_count} PrimaryOutcome nodes | "
                    f"Total updated: {total_updated} | "
                    f"Batch time: {hours}:{minutes}:{seconds}"
                )

            remaining = self._has_remaining_nodes_without_key()
            if remaining:
                self.appender.log_stdout(
                    f"{Fore.YELLOW}Warning: at least one PrimaryOutcome node still has no "
                    f"_composite_key.{Style.RESET_ALL}"
                )
            else:
                self.appender.log_stdout(
                    f"{Fore.GREEN}Verified: all PrimaryOutcome nodes have _composite_key."
                    f"{Style.RESET_ALL}"
                )

        finally:
            hours, minutes, seconds = elapsed_time(start_time, time.time())
            self.appender.log_stdout(
                f"\n{Fore.BLUE}{'=' * 30} Done. Total updated: {total_updated}. "
                f"Total time: {hours}:{minutes}:{seconds} {'=' * 30}{Style.RESET_ALL}\n"
            )
            self.appender.close()
            self.close_mysql_conn()


    def _fetch_batch(self):
        """
        Read one batch of PrimaryOutcome nodes missing _composite_key.

        PrimaryOutcome nodes do not store the owning trial id, so the key is
        based only on the outcome fields available on the node.
        """

        rows = list(self.memgraph.execute_and_fetch(self.FETCH_BATCH, {"limit": self.batch_size}))
        chunks = []
        seen_node_ids = set()

        for row in rows:
            node_id = row.get("node_id")

            if node_id in seen_node_ids:
                continue

            if node_id is None:
                continue

            measure = _clean(row.get("measure"))
            time_frame = _clean(row.get("timeFrame"))
            description = _clean(row.get("description"))
            composite_key = _make_hash_key(f"{measure}|{time_frame}|{description}")

            chunks.append({
                "node_id": node_id,
                "_composite_key": composite_key,
            })
            seen_node_ids.add(node_id)

        return chunks


    def _update_batch_with_retry(self, chunks, batch_num):
        """Write one batch with retry for transient Memgraph conflicts."""

        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return self._update_batch(chunks)
            except Exception as e:
                last_error = e

                if attempt >= self.max_retries:
                    self.appender.log_stdout(
                        f"{Fore.RED}Batch #{batch_num} failed after {attempt} attempts: {e}"
                        f"{Style.RESET_ALL}"
                    )
                    raise

                self.appender.log_stdout(
                    f"{Fore.YELLOW}Batch #{batch_num} attempt {attempt}/{self.max_retries} "
                    f"failed: {e}. Retrying in {self.retry_seconds} seconds..."
                    f"{Style.RESET_ALL}"
                )
                time.sleep(self.retry_seconds)

        raise last_error


    def _update_batch(self, chunks):
        """Set _composite_key on one batch and return the updated count."""

        rows = list(self.memgraph.execute_and_fetch(self.UPDATE_BATCH, {"chunks": chunks}))

        if not rows:
            return 0

        return int(rows[0].get("updated_count") or 0)


    def _ensure_composite_key_index(self):
        """Create the PrimaryOutcome._composite_key index if it is missing."""

        if self._is_index_field_exists_compatible("PrimaryOutcome", "_composite_key"):
            self.appender.log_stdout("Index already exists: :PrimaryOutcome(_composite_key)")
            return

        command = "CREATE INDEX ON :PrimaryOutcome(_composite_key);"
        self.memgraph.execute(command)
        self.appender.log_stdout(f"Created index: {command}")


    def _is_index_field_exists_compatible(self, label_name, field):
        """Handle SHOW INDEX INFO property shapes from old and new Memgraph."""

        rows = list(self.memgraph.execute_and_fetch("SHOW INDEX INFO"))

        for row in rows:
            if row.get("label") != label_name:
                continue

            properties = self._property_list(row.get("property"))
            if properties == [field]:
                return True

        return False


    @staticmethod
    def _property_list(value):
        """Normalize SHOW INDEX INFO property values to a list."""

        if value is None or value == "":
            return []

        if isinstance(value, list):
            return value

        if isinstance(value, tuple):
            return list(value)

        if isinstance(value, str):
            text = value.strip()

            if text.startswith("[") and text.endswith("]"):
                try:
                    parsed = literal_eval(text)
                    return parsed if isinstance(parsed, list) else [text]
                except (SyntaxError, ValueError):
                    return [text]

            return [text]

        return [str(value)]


    def _has_remaining_nodes_without_key(self):
        """Check whether any PrimaryOutcome node still lacks _composite_key."""

        query = '''
            MATCH (po:PrimaryOutcome)
            WHERE po._composite_key IS NULL
            RETURN 1 AS has_remaining
            LIMIT 1
        '''

        rows = list(self.memgraph.execute_and_fetch(query))
        return bool(rows)


if __name__ == '__main__':
    updater = PrimaryOutcomeCompositeKeyUpdater()
    updater.update()

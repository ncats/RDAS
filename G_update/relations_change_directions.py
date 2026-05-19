import os
import sys
import re
import time

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
])

try:
    from colorama import init, Fore, Style
except ModuleNotFoundError:
    class _NoColor:
        RED = GREEN = YELLOW = BLUE = CYAN = RESET_ALL = ""

    def init():
        pass

    Fore = _NoColor()
    Style = _NoColor()

init()

from baseclass.conn import DBConnection as db
from utils.file_appender import FileAppender
from utils.tools import _date_string


"""
Change Memgraph relationship directions in batches.

Add more direction changes by adding rows to RELATION_DIRECTION_CHANGE_SPECS.
Each row describes the old directed relationship and the desired new directed
relationship between the same two endpoint nodes.

The updater uses copy/delete:

    MATCH (old_from:OldFrom)-[old_r:old_type]->(old_to:OldTo)
    MERGE (old_to)-[new_r:new_type]->(old_from)
    SET new_r += properties(old_r)
    DELETE old_r

Relationship properties are preserved. Nodes are not modified.
"""


VALID_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# (old_from_label, old_relation, old_to_label, new_from_label, new_relation, new_to_label)
RELATION_DIRECTION_CHANGE_SPECS = (
    ("GARD", "has_researched_disease", "Project", "Project", "has_researched_disease", "GARD"),
)


class RelationDirectionChangeUpdater:
    """Batch reverse configured relationship directions in Memgraph."""

    BATCH_SIZE = 10000
    MAX_RETRIES = 3
    RETRY_SECONDS = 10

    def __init__(self):

        self.batch_size = self.BATCH_SIZE
        self.max_retries = self.MAX_RETRIES
        self.retry_seconds = self.RETRY_SECONDS

        self.memgraph = db().memgraph_conn()

        class_name = type(self).__name__
        self.log_file = f'logs/G-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self._validate_specs()


    def update(self) -> None:
        """Run all configured relationship direction changes."""

        start_time = time.time()
        total_changed = 0

        self.appender.log_stdout(
            f"Starting relationship direction migration. "
            f"Mappings = {len(RELATION_DIRECTION_CHANGE_SPECS)}. "
            f"Batch size = {self.batch_size}."
        )

        try:
            for spec in RELATION_DIRECTION_CHANGE_SPECS:
                total_changed += self._update_one_spec(*spec)

        finally:
            hours, minutes, seconds = self.elapsed_time(start_time, time.time())
            self.appender.log_stdout(
                f"\n{Fore.BLUE}{'=' * 30} Done. Total changed: {total_changed}. "
                f"Total time: {hours}:{minutes}:{seconds} {'=' * 30}{Style.RESET_ALL}\n"
            )
            self.close()


    def _update_one_spec(
            self,
            old_from_label: str,
            old_relation: str,
            old_to_label: str,
            new_from_label: str,
            new_relation: str,
            new_to_label: str) -> int:
        """Change all relationships for one configured direction mapping."""

        total_changed = 0
        batch_num = 0
        spec_name = (
            f"({old_from_label})-[:{old_relation}]->({old_to_label}) "
            f"to ({new_from_label})-[:{new_relation}]->({new_to_label})"
        )

        self.appender.log_stdout(f"\n{Fore.CYAN}Changing direction: {spec_name}{Style.RESET_ALL}")

        while True:
            batch_num += 1
            batch_start = time.time()

            relationships_changed = self._change_one_batch_with_retry(
                old_from_label,
                old_relation,
                old_to_label,
                new_relation,
                spec_name,
                batch_num
            )

            if relationships_changed == 0:
                self.appender.log_stdout(
                    f"No more old-direction relationships found for {spec_name} "
                    f"after {batch_num - 1} completed batches."
                )
                break

            total_changed += relationships_changed
            hours, minutes, seconds = self.elapsed_time(batch_start, time.time())
            self.appender.log_stdout(
                f"{spec_name} batch #{batch_num}: changed {relationships_changed} "
                f"relationships | Spec total: {total_changed} | "
                f"Batch time: {hours}:{minutes}:{seconds}"
            )

        self._log_final_verification(
            old_from_label,
            old_relation,
            old_to_label,
            new_from_label,
            new_relation,
            new_to_label,
            spec_name
        )

        return total_changed


    def _change_one_batch_with_retry(
            self,
            old_from_label: str,
            old_relation: str,
            old_to_label: str,
            new_relation: str,
            spec_name: str,
            batch_num: int) -> int:
        """Run one direction-change batch, retrying transient Memgraph failures."""

        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return self._change_one_batch(
                    old_from_label,
                    old_relation,
                    old_to_label,
                    new_relation
                )
            except Exception as e:
                last_error = e

                if attempt >= self.max_retries:
                    self.appender.log_stdout(
                        f"{Fore.RED}{spec_name} batch #{batch_num} failed after "
                        f"{attempt} attempts: {e}{Style.RESET_ALL}"
                    )
                    raise

                self.appender.log_stdout(
                    f"{Fore.YELLOW}{spec_name} batch #{batch_num} attempt "
                    f"{attempt}/{self.max_retries} failed: {e}. "
                    f"Retrying in {self.retry_seconds} seconds...{Style.RESET_ALL}"
                )
                time.sleep(self.retry_seconds)

        raise last_error


    def _change_one_batch(
            self,
            old_from_label: str,
            old_relation: str,
            old_to_label: str,
            new_relation: str) -> int:
        """
        Reverse one limited set of relationships.

        Relationship types and labels cannot be parameterized in Cypher, so the
        validated spec values are interpolated into the query.
        """

        batch_change_query = f'''
            MATCH (old_from:{old_from_label})-[old_r:{old_relation}]->(old_to:{old_to_label})
            WITH old_from, old_to, old_r
            LIMIT {self.batch_size}
            MERGE (old_to)-[new_r:{new_relation}]->(old_from)
            SET new_r += properties(old_r)
            DELETE old_r
            RETURN count(new_r) AS relationships_changed
        '''

        rows = list(self.memgraph.execute_and_fetch(batch_change_query))

        if not rows:
            return 0

        return int(rows[0].get("relationships_changed") or 0)


    def _log_final_verification(
            self,
            old_from_label: str,
            old_relation: str,
            old_to_label: str,
            new_from_label: str,
            new_relation: str,
            new_to_label: str,
            spec_name: str) -> None:
        """Log old and new direction counts after one configured mapping finishes."""

        old_count = self._count_relationships(old_from_label, old_relation, old_to_label)
        new_count = self._count_relationships(new_from_label, new_relation, new_to_label)

        if old_count:
            self.appender.log_stdout(
                f"{Fore.YELLOW}Warning: {old_count} old-direction relationships "
                f"still remain for {spec_name}.{Style.RESET_ALL}"
            )
        else:
            self.appender.log_stdout(
                f"{Fore.GREEN}Verified: no old-direction relationships remain for "
                f"{spec_name}.{Style.RESET_ALL}"
            )

        self.appender.log_stdout(
            f"Final counts for {spec_name}: old direction = {old_count}; "
            f"new direction = {new_count}."
        )


    def _count_relationships(self, from_label: str, relation: str, to_label: str) -> int:
        """Count a directed relationship pattern for logging and verification."""

        count_query = f'''
            MATCH (:{from_label})-[r:{relation}]->(:{to_label})
            RETURN count(r) AS relationship_count
        '''

        rows = list(self.memgraph.execute_and_fetch(count_query))

        if not rows:
            return 0

        return int(rows[0].get("relationship_count") or 0)


    def _validate_specs(self) -> None:
        """
        Validate labels and relationship types before interpolating Cypher.

        This updater reverses relationships between the same two endpoint nodes.
        Therefore the new from-label must be the old to-label, and the new
        to-label must be the old from-label.
        """

        for spec in RELATION_DIRECTION_CHANGE_SPECS:
            if len(spec) != 6:
                raise ValueError(f"Relation direction config must have 6 values: {spec}")

            old_from_label, old_relation, old_to_label, new_from_label, new_relation, new_to_label = spec

            identifiers = [
                old_from_label,
                old_relation,
                old_to_label,
                new_from_label,
                new_relation,
                new_to_label,
            ]

            for identifier in identifiers:
                if not VALID_IDENTIFIER_RE.match(identifier):
                    raise ValueError(f"Invalid Cypher identifier in relation direction config: {identifier}")

            if new_from_label != old_to_label or new_to_label != old_from_label:
                raise ValueError(
                    "Relation direction config must reverse the same endpoint labels: "
                    f"{spec}"
                )


    def close(self) -> None:
        """Close the Memgraph connection and log file."""

        close_memgraph = getattr(self.memgraph, "close", None)

        if callable(close_memgraph):
            close_memgraph()

        self.appender.close()


    @staticmethod
    def elapsed_time(start_time: float, end_time: float):
        """Return elapsed time as hours, minutes, seconds."""

        time_diff = end_time - start_time
        hours = int(time_diff // 3600)
        minutes = int((time_diff % 3600) // 60)
        seconds = int(time_diff % 60)

        return hours, minutes, seconds


if __name__ == '__main__':
    RelationDirectionChangeUpdater().update()

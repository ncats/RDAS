import os
import sys
import re
import time
from datetime import datetime

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
    os.path.abspath(os.path.join(_dir, '../..')),
])

try:
    from colorama import init, Fore, Style
except ModuleNotFoundError:
    class _NoColor:
        BLACK = RED = GREEN = YELLOW = BLUE = MAGENTA = CYAN = WHITE = RESET = RESET_ALL = ""

    def init():
        pass

    Fore = _NoColor()
    Style = _NoColor()

init()

from utils.file_appender import FileAppender
from baseclass.conn import DBConnection as db


"""
Rename legacy Memgraph relationship types in batches.

The mappings are scoped by endpoint labels. This is important because some old
relationship names are reused in different areas of the graph. For example,
`mapped_to_gard` can connect Disease/Condition and Disease/ClinicalTrial, but
those pairs need different new relationship names.

This updater uses copy/delete instead of refactor.rename_type so each mapping
can be label-scoped and processed in small transactions:

    MATCH (a:OldFrom)-[old_r:old_type]->(b:OldTo)
    MERGE (a)-[new_r:new_type]->(b)
    SET new_r += properties(old_r)
    DELETE old_r

By default it checks both directions for each label pair and preserves the
direction of each old relationship it finds. That makes the migration tolerant
of older graph loads that may have created the same legacy relationship in the
reverse direction.
"""


VALID_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _date_string(format_string: str = "%Y%m%d") -> str:
    """Return the current date string for the migration log file name."""

    return datetime.now().strftime(format_string)


def elapsed_time(start_time: float, end_time: float):
    """Return elapsed time as hours, minutes, seconds."""

    time_diff = end_time - start_time
    hours = int(time_diff // 3600)
    minutes = int((time_diff % 3600) // 60)
    seconds = int(time_diff % 60)

    return hours, minutes, seconds


# (from_label, old_relation, to_label, new_relation)
RELATION_RENAME_SPECS = (
    ("CoreProject", "funded_by", "Organization", "has_funding_organization"),
    ("CoreProject", "published", "Article", "has_publication"),
    ("CoreProject", "patented", "Patent", "has_patent"),
    ("CoreProject", "studied", "ClinicalTrial", "has_clinical_trial"),
    ("Project", "annotated", "Annotation", "has_annotation"),
    ("Article", "mesh_term_for", "MeshTerm", "has_mesh_term"),
    ("Article", "published_in", "Journal", "has_journal"),
    ("ClinicalTrial", "investigates_condition", "Condition", "has_investigated_condition"),
    ("Disease", "mapped_to_gard", "Condition", "has_mapped_condition"),
    ("Disease", "mapped_to_gard", "ClinicalTrial", "has_clinical_trial"),
    ("Disease", "mentioned_in", "Article", "has_mention_in"),
    ("Disease", "condition_associated_with_gene", "Gene", "has_associated_gene"),
    ("ClinicalTrial", "annotated", "Annotation", "has_annotation"),
    ("ClinicalTrial", "associated_with", "Organization", "has_associated_organization"),
    ("Project", "has_mention_under", "Disease", "has_researched_disease"),
    ("Intervention", "mapped_to_rxnorm", "Drug", "has_rxnorm_mapping"),
)


class RelationRenameUpdater:
    """Batch rename legacy Memgraph relationships for all configured mappings."""

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
        """Run all relationship rename mappings."""

        start_time = time.time()
        total_changed = 0

        self.appender.log_stdout(
            f"Starting relationship rename migration. "
            f"Mappings = {len(RELATION_RENAME_SPECS)}. Batch size = {self.batch_size}."
        )

        try:
            for from_label, old_relation, to_label, new_relation in RELATION_RENAME_SPECS:
                total_changed += self._update_one_spec(
                    from_label,
                    old_relation,
                    to_label,
                    new_relation
                )

        finally:
            hours, minutes, seconds = elapsed_time(start_time, time.time())
            self.appender.log_stdout(
                f"\n{Fore.BLUE}{'=' * 30} Done. Total renamed: {total_changed}. "
                f"Total time: {hours}:{minutes}:{seconds} {'=' * 30}{Style.RESET_ALL}\n"
            )
            self.appender.close()
            self._close_memgraph()


    def _update_one_spec(self, from_label: str, old_relation: str, to_label: str, new_relation: str) -> int:
        """Rename all relationships for one label-scoped mapping."""

        total_changed = 0
        batch_num = 0
        spec_name = f"{from_label}-{to_label}"

        self.appender.log_stdout(
            f"\n{Fore.CYAN}Renaming {spec_name}: "
            f"{old_relation} -> {new_relation}{Style.RESET_ALL}"
        )

        while True:
            batch_num += 1
            batch_start = time.time()

            relationships_changed = self._rename_one_batch_with_retry(
                from_label,
                old_relation,
                to_label,
                new_relation,
                batch_num
            )

            if relationships_changed == 0:
                self.appender.log_stdout(
                    f"No more {old_relation} relationships found for "
                    f"{spec_name} after {batch_num - 1} completed batches."
                )
                break

            total_changed += relationships_changed
            hours, minutes, seconds = elapsed_time(batch_start, time.time())
            self.appender.log_stdout(
                f"{spec_name} batch #{batch_num}: renamed {relationships_changed} "
                f"relationships | Spec total: {total_changed} | "
                f"Batch time: {hours}:{minutes}:{seconds}"
            )

        if self._has_remaining_old_relationships(from_label, old_relation, to_label):
            self.appender.log_stdout(
                f"{Fore.YELLOW}Warning: legacy {old_relation} relationships "
                f"still exist for {spec_name}.{Style.RESET_ALL}"
            )
        else:
            self.appender.log_stdout(
                f"{Fore.GREEN}Verified: no legacy {old_relation} relationships "
                f"remain for {spec_name}.{Style.RESET_ALL}"
            )

        return total_changed


    def _rename_one_batch_with_retry(
            self,
            from_label: str,
            old_relation: str,
            to_label: str,
            new_relation: str,
            batch_num: int) -> int:
        """Run one rename batch, retrying transient Memgraph failures."""

        last_error = None
        spec_name = f"{from_label}-{to_label}"

        for attempt in range(1, self.max_retries + 1):
            try:
                return self._rename_one_batch(from_label, old_relation, to_label, new_relation)
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


    def _rename_one_batch(self, from_label: str, old_relation: str, to_label: str, new_relation: str) -> int:
        """
        Rename one limited set of relationships.

        Relationship types and labels cannot be parameterized in Cypher, so the
        configured constants are validated once and interpolated into the query.
        """

        changed = self._copy_delete_one_direction(
            from_label=from_label,
            to_label=to_label,
            old_relation=old_relation,
            new_relation=new_relation,
        )

        if changed:
            return changed

        return self._copy_delete_one_direction(
            from_label=to_label,
            to_label=from_label,
            old_relation=old_relation,
            new_relation=new_relation,
        )


    def _copy_delete_one_direction(
            self,
            from_label: str,
            to_label: str,
            old_relation: str,
            new_relation: str) -> int:
        """Create the new relation type, copy properties, and delete the old relation."""

        batch_rename_query = f'''
            MATCH (from_node:{from_label})-[old_r:{old_relation}]->(to_node:{to_label})
            WITH from_node, to_node, old_r
            LIMIT {self.batch_size}
            MERGE (from_node)-[new_r:{new_relation}]->(to_node)
            SET new_r += properties(old_r)
            DELETE old_r
            RETURN count(new_r) AS relationships_changed
        '''

        rows = list(self.memgraph.execute_and_fetch(batch_rename_query))

        if not rows:
            return 0

        return int(rows[0].get("relationships_changed") or 0)


    def _has_remaining_old_relationships(self, from_label: str, old_relation: str, to_label: str) -> bool:
        """Check for any old relationship in either direction without a full count."""

        check_query = f'''
            MATCH (:{from_label})-[r:{old_relation}]-(:{to_label})
            RETURN 1 AS has_remaining
            LIMIT 1
        '''

        return bool(list(self.memgraph.execute_and_fetch(check_query)))


    def _validate_specs(self) -> None:
        """Validate labels and relationship types before interpolating Cypher."""

        for from_label, old_relation, to_label, new_relation in RELATION_RENAME_SPECS:
            identifiers = [
                from_label,
                to_label,
                old_relation,
                new_relation,
            ]

            for identifier in identifiers:
                if not VALID_IDENTIFIER_RE.match(identifier):
                    raise ValueError(f"Invalid Cypher identifier in relation rename config: {identifier}")


    def _close_memgraph(self) -> None:
        """Close the Memgraph connection if the client exposes a close method."""

        close = getattr(self.memgraph, "close", None)
        if callable(close):
            close()

if __name__ == '__main__':

    updater = RelationRenameUpdater()
    updater.update()

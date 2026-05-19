import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
])

from colorama import init, Fore, Style
init()

import argparse
import time

from baseclass.init_base import InitBase
from utils.file_appender import FileAppender
from utils.tools import _date_string, ask_to_continue, elapsed_time


"""
Rename existing Article/MeshTerm relationships in Memgraph.

The original one-shot Cypher collects all matching relationships before calling
refactor.rename_type(). That is risky for this migration because production has
millions of Article/MeshTerm relationships:

    MATCH (d:Article)-[r:mesh_term_for]-(g:MeshTerm)
    WITH COLLECT(distinct(r)) AS rels
    CALL refactor.rename_type("mesh_term_for", "has_mesh_term", rels)
    YIELD relationships_changed
    RETURN relationships_changed;

This updater performs the same migration in smaller transactions. By default it
does not call refactor.rename_type because that procedure can hit serialization
errors on very large relationship sets. Instead, each batch creates the new
relationship type, copies the old relationship properties, deletes the old
relationship, logs progress, and repeats until no `mesh_term_for` relationships
remain between Article and MeshTerm nodes.
"""


class ArticleMeshTermRelationRenameUpdater(InitBase):
    """Batch rename Article/MeshTerm relationship type in Memgraph."""

    OLD_RELATION_NAME = "mesh_term_for"
    NEW_RELATION_NAME = "has_mesh_term"

    DEFAULT_BATCH_SIZE = 20000
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_SECONDS = 10
    DEFAULT_MODE = "copy-delete"

    def __init__(
            self,
            batch_size=DEFAULT_BATCH_SIZE,
            max_retries=DEFAULT_MAX_RETRIES,
            retry_seconds=DEFAULT_RETRY_SECONDS,
            mode=DEFAULT_MODE):
        super().__init__('', 'Article')

        self.batch_size = int(batch_size)
        self.max_retries = int(max_retries)
        self.retry_seconds = int(retry_seconds)
        self.mode = mode

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/G-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

    def init_nodes(self):
        """Override abstract method from InitBase."""

        self.update()


    def update(self):
        """Rename old Article/MeshTerm relationship types in repeatable batches."""

        start_time = time.time()
        total_changed = 0
        batch_num = 0

        self.appender.log_stdout(
            f"Starting Article/MeshTerm relationship rename: "
            f"{self.OLD_RELATION_NAME} -> {self.NEW_RELATION_NAME}. "
            f"Batch size = {self.batch_size}. Mode = {self.mode}"
        )

        try:
            while True:
                batch_num += 1
                batch_start = time.time()

                relationships_changed = self._rename_one_batch_with_retry(batch_num)

                if relationships_changed == 0:
                    self.appender.log_stdout(
                        f"No more {self.OLD_RELATION_NAME} relationships found after "
                        f"{batch_num - 1} completed batches."
                    )
                    break

                total_changed += relationships_changed
                hours, minutes, seconds = elapsed_time(batch_start, time.time())

                self.appender.log_stdout(
                    f"Batch #{batch_num}: renamed {relationships_changed} relationships | "
                    f"Total renamed: {total_changed} | "
                    f"Batch time: {hours}:{minutes}:{seconds}"
                )

            has_remaining = self._has_remaining_old_relationships()
            if has_remaining:
                self.appender.log_stdout(
                    f"{Fore.YELLOW}Warning: at least one {self.OLD_RELATION_NAME} relationship "
                    f"still exists between Article and MeshTerm nodes.{Style.RESET_ALL}"
                )
            else:
                self.appender.log_stdout(
                    f"{Fore.GREEN}Verified: no {self.OLD_RELATION_NAME} relationships remain "
                    f"between Article and MeshTerm nodes.{Style.RESET_ALL}"
                )

        finally:
            hours, minutes, seconds = elapsed_time(start_time, time.time())
            self.appender.log_stdout(
                f"\n{Fore.BLUE}{'=' * 30} Done. Total renamed: {total_changed}. "
                f"Total time: {hours}:{minutes}:{seconds} {'=' * 30}{Style.RESET_ALL}\n"
            )
            self.appender.close()
            self.close_mysql_conn()



    def _rename_one_batch_with_retry(self, batch_num):
        """Run one rename batch, retrying transient Memgraph conflicts."""

        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return self._rename_one_batch()
            except Exception as e:
                last_error = e

                if attempt >= self.max_retries:
                    self.appender.log_stdout(
                        f"{Fore.RED}Batch #{batch_num} failed after {attempt} attempts: {e}{Style.RESET_ALL}"
                    )
                    raise

                self.appender.log_stdout(
                    f"{Fore.YELLOW}Batch #{batch_num} attempt {attempt}/{self.max_retries} failed: {e}. "
                    f"Retrying in {self.retry_seconds} seconds...{Style.RESET_ALL}"
                )
                time.sleep(self.retry_seconds)

        raise last_error
    


    def _rename_one_batch(self):
        """
        Rename one limited set of relationships.

        The relationship type in MATCH cannot be parameterized, so the fixed
        project constants are interpolated into the query. The batch size is
        sanitized as an integer before it is interpolated into LIMIT.
        """

        if self.mode == "refactor":
            return self._rename_one_batch_with_refactor()

        return self._rename_one_batch_with_copy_delete()

    def _rename_one_batch_with_copy_delete(self):
        """
        Rename one batch without refactor.rename_type.

        Current graph writers create MeshTerm -> Article relationships, so the
        migration uses that direction first. This avoids the slower undirected
        match and removes the need for DISTINCT. If older data exists in the
        reverse direction, the fallback query handles Article -> MeshTerm too.
        """

        changed = self._copy_delete_one_direction(
            match_pattern=f"(m:MeshTerm)-[r:{self.OLD_RELATION_NAME}]->(a:Article)",
            merge_pattern=f"(m)-[new_r:{self.NEW_RELATION_NAME}]->(a)"
        )

        if changed:
            return changed

        return self._copy_delete_one_direction(
            match_pattern=f"(a:Article)-[r:{self.OLD_RELATION_NAME}]->(m:MeshTerm)",
            merge_pattern=f"(a)-[new_r:{self.NEW_RELATION_NAME}]->(m)"
        )

    def _copy_delete_one_direction(self, match_pattern, merge_pattern):
        """Create the new relation, copy properties, and delete the old relation."""

        batch_size = max(1, int(self.batch_size))

        batch_rename_query = f'''
            MATCH {match_pattern}
            WITH m, a, r
            LIMIT {batch_size}
            MERGE {merge_pattern}
            SET new_r += properties(r)
            DELETE r
            RETURN count(new_r) AS relationships_changed
        '''

        rows = list(self.memgraph.execute_and_fetch(batch_rename_query))

        if not rows:
            return 0

        return int(rows[0].get("relationships_changed") or 0)

    def _rename_one_batch_with_refactor(self):
        """Rename one batch with refactor.rename_type for compatibility/testing."""

        batch_size = max(1, int(self.batch_size))

        batch_rename_query = f'''
            MATCH (m:MeshTerm)-[r:{self.OLD_RELATION_NAME}]->(a:Article)
            WITH r
            LIMIT {batch_size}
            WITH collect(r) AS rels
            CALL refactor.rename_type($old_relation_name, $new_relation_name, rels)
            YIELD relationships_changed
            RETURN relationships_changed
        '''

        rows = list(self.memgraph.execute_and_fetch(
            batch_rename_query,
            {
                "old_relation_name": self.OLD_RELATION_NAME,
                "new_relation_name": self.NEW_RELATION_NAME,
            }
        ))

        if not rows:
            return 0

        return int(rows[0].get("relationships_changed") or 0)
    

    def _has_remaining_old_relationships(self):
        """
        Check for any remaining old relationships without doing a full count.

        A full count over tens of millions of relationships can be slow. LIMIT 1
        is enough to verify whether another batch might still be needed.
        """

        check_query = f'''
            MATCH (:Article)-[r:{self.OLD_RELATION_NAME}]-(:MeshTerm)
            RETURN 1 AS has_remaining
            LIMIT 1
        '''

        rows = list(self.memgraph.execute_and_fetch(check_query))
        return bool(rows)



if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description="Batch rename Article/MeshTerm Memgraph relationships from mesh_term_for to has_mesh_term."
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=ArticleMeshTermRelationRenameUpdater.DEFAULT_BATCH_SIZE,
        help="Number of relationships to rename per Memgraph transaction."
    )

    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the interactive confirmation prompt."
    )

    parser.add_argument(
        "--mode",
        choices=["copy-delete", "refactor"],
        default=ArticleMeshTermRelationRenameUpdater.DEFAULT_MODE,
        help="Rename implementation. copy-delete avoids refactor.rename_type serialization errors."
    )

    args = parser.parse_args()

    if not args.yes:
        prompt = (
            "This will rename Article/MeshTerm relationships in Memgraph from "
            "mesh_term_for to has_mesh_term. Continue?"
        )
        if not ask_to_continue(prompt):
            sys.exit('------Stopped------')


    updater = ArticleMeshTermRelationRenameUpdater(batch_size=args.batch_size, mode=args.mode)

    updater.update()

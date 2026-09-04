"""
Repair missing Project-to-GARD roll-up relationships in MySQL and Memgraph.

The grant relationship matcher can keep a more specific GARD label such as
`adult glioblastoma` while dropping the shorter/base label `glioblastoma`.
This makeup task repairs that class of issue by inferring base-disease
relationships from existing specific-disease relationships.

The rule is intentionally phrase-based:
    If an existing source GARD disease has a longer name/search term that
    contains a shorter target GARD primary name as a whole phrase, then each
    Project linked to the source GARD should also be linked to the target GARD.

Example:
    Project -> adult glioblastoma
    adult glioblastoma contains glioblastoma
    Therefore add Project -> glioblastoma when the generic link is missing.

Run from the repository root:

    conda run -n rdas python Z_Alert/pipelines/pipeline_9_makeup/fix_gard_project_rollup_relationships.py

Dry-run is the default. To apply the repair:

    conda run -n rdas python Z_Alert/pipelines/pipeline_9_makeup/fix_gard_project_rollup_relationships.py --apply

Useful scoped check for the known glioblastoma issue:

    conda run -n rdas python Z_Alert/pipelines/pipeline_9_makeup/fix_gard_project_rollup_relationships.py --base-gard-id GARD:0002491
"""

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..", "..")),
    os.path.abspath(os.path.join(_dir, "..", "..", "..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _normalize_txt, _time_hms


TERM_SEPARATOR = "$$$"
DEFAULT_BATCH_SIZE = 5000
DEFAULT_MIN_BASE_NAME_CHARS = 5
MAX_SOURCE_TYPE_LENGTH = 45
MAX_RAW_RESULT_LENGTH = 4000
MAX_LOGGED_PAIR_EXAMPLES = 20
EXCLUSION_CONTEXT_WORDS = {"exclude", "excluded", "excluding", "except", "without", "non", "not"}
EXCLUSION_CONTEXT_WINDOW = 3
MYSQL_LOCK_NAME = "rdas_gard_project_rollup_relationship_makeup"


@dataclass(frozen=True)
class GardName:
    """One normalized GARD name row used to infer source-to-base roll-up pairs."""

    gard_id: str
    name: str
    normalized_name: str
    words: Tuple[str, ...]
    source_terms: Tuple[str, ...]


@dataclass(frozen=True)
class RollupPair:
    """One inferred source GARD to target/base GARD repair rule."""

    source_gard_id: str
    source_gard_name: str
    target_gard_id: str
    target_gard_name: str
    rollup_rule: str


class GardProjectRollupRelationshipMakeupTask(PipelineBase):
    """Insert missing MySQL roll-up rows and sync them to Memgraph."""

    FETCH_GARD_NAMES_QUERY = """
        SELECT
            gardid,
            name,
            synonyms_sw
        FROM grant_gard_processed_names
        WHERE
            gardid IS NOT NULL
            AND TRIM(gardid) <> ''
            AND name IS NOT NULL
            AND TRIM(name) <> ''
    """

    DROP_TEMP_ROLLUP_PAIRS_QUERY = """
        DROP TEMPORARY TABLE IF EXISTS tmp_gard_project_rollup_pairs
    """

    CREATE_TEMP_ROLLUP_PAIRS_QUERY = """
        CREATE TEMPORARY TABLE tmp_gard_project_rollup_pairs (
            source_gard_id VARCHAR(45) NOT NULL,
            source_gard_name VARCHAR(300) NOT NULL,
            target_gard_id VARCHAR(45) NOT NULL,
            target_gard_name VARCHAR(300) NOT NULL,
            rollup_rule VARCHAR(80) NOT NULL,
            PRIMARY KEY (source_gard_id, target_gard_id),
            KEY idx_tmp_target_gard_id (target_gard_id)
        ) ENGINE=InnoDB
    """

    INSERT_TEMP_ROLLUP_PAIR_QUERY = """
        INSERT IGNORE INTO tmp_gard_project_rollup_pairs (
            source_gard_id,
            source_gard_name,
            target_gard_id,
            target_gard_name,
            rollup_rule
        )
        VALUES (%s, %s, %s, %s, %s)
    """

    DROP_TEMP_ROLLUP_CANDIDATES_QUERY = """
        DROP TEMPORARY TABLE IF EXISTS tmp_gard_project_rollup_candidates
    """

    CREATE_TEMP_ROLLUP_CANDIDATES_QUERY = """
        CREATE TEMPORARY TABLE tmp_gard_project_rollup_candidates (
            candidate_id BIGINT NOT NULL AUTO_INCREMENT,
            source_relation_id INT NOT NULL,
            source_gard_id VARCHAR(45) DEFAULT NULL,
            source_gard_name VARCHAR(300) DEFAULT NULL,
            target_gard_id VARCHAR(45) NOT NULL,
            target_gard_name VARCHAR(300) NOT NULL,
            rollup_rule VARCHAR(80) NOT NULL,
            application_id INT NOT NULL,
            core_project_num VARCHAR(45) DEFAULT NULL,
            source_type VARCHAR(45) DEFAULT NULL,
            confidence_score DECIMAL(19,18) DEFAULT NULL,
            semantic_similarity DECIMAL(19,18) DEFAULT NULL,
            PRIMARY KEY (candidate_id),
            UNIQUE KEY idx_tmp_rollup_target_application (target_gard_id, application_id),
            KEY idx_tmp_rollup_source_relation (source_relation_id)
        ) ENGINE=InnoDB
    """

    MATERIALIZE_MISSING_MYSQL_ROLLUPS_QUERY = """
        INSERT INTO tmp_gard_project_rollup_candidates (
            source_relation_id,
            source_gard_id,
            source_gard_name,
            target_gard_id,
            target_gard_name,
            rollup_rule,
            application_id,
            core_project_num,
            source_type,
            confidence_score,
            semantic_similarity
        )
        SELECT
            candidate.source_relation_id,
            source.gard_id AS source_gard_id,
            source.gard_name AS source_gard_name,
            candidate.target_gard_id,
            candidate.target_gard_name,
            candidate.rollup_rule,
            candidate.application_id,
            source.core_project_num,
            source.source_type,
            source.confidence_score,
            source.semantic_similarity
        FROM (
            SELECT
                MIN(source.id) AS source_relation_id,
                pair.target_gard_id,
                MIN(pair.target_gard_name) AS target_gard_name,
                MIN(pair.rollup_rule) AS rollup_rule,
                source.application_id
            FROM grant_gard_project_relation AS source
            INNER JOIN tmp_gard_project_rollup_pairs AS pair
                ON pair.source_gard_id = source.gard_id
            LEFT JOIN grant_gard_project_relation AS existing
                ON existing.application_id = source.application_id
                AND existing.gard_id = pair.target_gard_id
            WHERE
                source.application_id IS NOT NULL
                AND source.gard_id IS NOT NULL
                AND (source.source_type IS NULL OR source.source_type NOT LIKE 'rollup%%')
                AND existing.id IS NULL
            GROUP BY
                pair.target_gard_id,
                source.application_id
        ) AS candidate
        INNER JOIN grant_gard_project_relation AS source
            ON source.id = candidate.source_relation_id
    """

    COUNT_TEMP_ROLLUP_CANDIDATES_QUERY = """
        SELECT COUNT(*) AS missing_count
        FROM tmp_gard_project_rollup_candidates
    """

    FETCH_TEMP_ROLLUP_CANDIDATES_QUERY = """
        SELECT
            candidate_id,
            source_relation_id,
            source_gard_id,
            source_gard_name,
            target_gard_id,
            target_gard_name,
            rollup_rule,
            application_id,
            core_project_num,
            source_type,
            confidence_score,
            semantic_similarity
        FROM tmp_gard_project_rollup_candidates
        WHERE candidate_id > %s
        ORDER BY candidate_id
        LIMIT %s
    """

    INSERT_MYSQL_ROLLUP_RELATION_QUERY = """
        INSERT INTO grant_gard_project_relation (
            gard_id,
            application_id,
            gard_name,
            source_type,
            confidence_score,
            semantic_similarity,
            core_project_num,
            raw_result,
            is_new
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1)
    """

    FETCH_ROLLUP_GRAPH_ROWS_QUERY = """
        SELECT
            id,
            gard_id,
            application_id,
            gard_name,
            source_type,
            confidence_score,
            semantic_similarity,
            raw_result
        FROM grant_gard_project_relation
        WHERE
            id > %s
            AND application_id IS NOT NULL
            AND gard_id IS NOT NULL
            AND source_type LIKE 'rollup%%'
        ORDER BY id
        LIMIT %s
    """

    UPSERT_MEMGRAPH_RELATIONSHIPS_CYPHER = """
        UNWIND $chunks AS chunk
        MATCH (p:Project {applicationId: chunk.applicationId})
        MATCH (g:GARD {gardId: chunk.gardId})
        MERGE (p)-[r:has_researched_disease]->(g)
        SET
            r.confidenceScore = chunk.confidenceScore,
            r.semanticSimilarity = chunk.semanticSimilarity,
            r.sourceType = chunk.sourceType,
            r.rollupSourceGardId = chunk.rollupSourceGardId,
            r.rollupSourceGardName = chunk.rollupSourceGardName,
            r.rollupRule = chunk.rollupRule
        RETURN count(*) AS relationships_matched
    """

    def __init__(self, apply_changes: bool = False, batch_size: int = DEFAULT_BATCH_SIZE, min_base_name_chars: int = DEFAULT_MIN_BASE_NAME_CHARS, base_gard_ids: Optional[Set[str]] = None, source_gard_ids: Optional[Set[str]] = None, skip_memgraph: bool = False):

        super().__init__(init_mysql=True, init_memgraph=apply_changes and not skip_memgraph)
        self.apply_changes = apply_changes
        self.batch_size = max(1, batch_size)
        self.min_base_name_chars = max(1, min_base_name_chars)
        self.base_gard_ids = base_gard_ids or set()
        self.source_gard_ids = source_gard_ids or set()
        self.skip_memgraph = skip_memgraph


    def find_new_data(self, gard_node) -> None:

        self.logger.info("GardProjectRollupRelationshipMakeupTask does not use find_new_data().")


    def process_new_data(self) -> None:

        """Build roll-up rules, repair MySQL rows, and sync repair rows to Memgraph."""

        start_time = time.time()
        summary = {
            "gard_names_seen": 0,
            "rollup_pairs_created": 0,
            "missing_mysql_rollups": 0,
            "mysql_rollups_inserted": 0,
            "memgraph_rollups_submitted": 0,
            "memgraph_rollups_matched": 0,
        }
        lock_acquired = False

        try:
            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            if self.apply_changes:
                lock_acquired = self._acquire_mysql_lock()

                if not lock_acquired:
                    self.logger.error(
                        f"Another {type(self).__name__} run appears to be active. "
                        "Stop the other run or wait for it to finish before applying roll-up repairs."
                    )
                    return

            gard_names = self._fetch_gard_names()
            summary["gard_names_seen"] = len(gard_names)

            rollup_pairs = self._build_rollup_pairs(gard_names)
            summary["rollup_pairs_created"] = len(rollup_pairs)

            if not rollup_pairs:
                self.logger.info("No GARD roll-up pairs were inferred. Nothing to repair.")
                return

            self._log_rollup_pair_examples(rollup_pairs)
            self._create_temp_rollup_pair_table(rollup_pairs)
            self._create_temp_rollup_candidate_table()
            missing_count = self._count_temp_rollup_candidates()
            summary["missing_mysql_rollups"] = missing_count

            if not self.apply_changes:
                self.logger.info(
                    "Dry-run complete. "
                    f"Inferred rollup_pairs={len(rollup_pairs)} and missing_mysql_rollups={missing_count}. "
                    "Run again with --apply to write MySQL rows and sync Memgraph relationships."
                )
                return

            summary["mysql_rollups_inserted"] = self._insert_missing_mysql_rollups()

            if self.skip_memgraph:
                self.logger.info("Skipping Memgraph sync because --skip-memgraph was set.")

            else:
                memgraph_summary = self._sync_rollup_rows_to_memgraph()
                summary["memgraph_rollups_submitted"] = memgraph_summary["submitted"]
                summary["memgraph_rollups_matched"] = memgraph_summary["matched"]

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(
                f"Completed GARD Project roll-up relationship makeup task. "
                f"Summary={summary}. Total time={hours} hours, {minutes} minutes, {seconds} seconds."
            )

        except Exception:
            self.logger.exception(f"GardProjectRollupRelationshipMakeupTask failed. Summary={summary}")

        finally:
            if lock_acquired:
                self._release_mysql_lock()

            self.close()


    def _fetch_gard_names(self) -> List[GardName]:

        """Fetch and normalize GARD primary names plus exact processed search terms."""

        cursor = None
        gard_names: List[GardName] = []
        seen_rows: Set[Tuple[str, str]] = set()

        try:
            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            cursor.execute(self.FETCH_GARD_NAMES_QUERY)

            for row in cursor.fetchall():
                gard_id = str(row.get("gardid") or "").strip()
                name = str(row.get("name") or "").strip()
                normalized_name = normalize_phrase(name)

                if not gard_id or not name or not normalized_name:
                    continue

                row_key = (gard_id, normalized_name)

                if row_key in seen_rows:
                    continue

                seen_rows.add(row_key)
                source_terms = tuple(sorted(split_normalized_terms(row.get("synonyms_sw")) | {normalized_name}))
                gard_names.append(
                    GardName(
                        gard_id=gard_id,
                        name=name,
                        normalized_name=normalized_name,
                        words=tuple(normalized_name.split()),
                        source_terms=source_terms,
                    )
                )

            self.logger.info(f"Fetched {len(gard_names)} distinct GARD name rows for roll-up inference.")
            return gard_names

        finally:
            if cursor is not None:
                cursor.close()


    def _build_rollup_pairs(self, gard_names: List[GardName]) -> List[RollupPair]:

        """Infer specific-to-base GARD pairs from primary names and exact source terms."""

        names_by_phrase: Dict[str, List[GardName]] = {}

        for gard_name in gard_names:
            names_by_phrase.setdefault(gard_name.normalized_name, []).append(gard_name)

        pairs_by_key: Dict[Tuple[str, str], RollupPair] = {}

        for source in gard_names:
            if self.source_gard_ids and source.gard_id not in self.source_gard_ids:
                continue

            '''
            Exact source terms come from `Synonyms_sw`, which is the same
            normalized search-term column used by task_grant_10.py. If a more
            specific disease uses a base disease's primary name as one exact
            search term, the base relationship is a good repair candidate.
            '''
            for source_term in source.source_terms:
                self._add_rollup_pairs_for_phrase(
                    pairs_by_key,
                    names_by_phrase,
                    source,
                    source_term,
                    "target_name_in_source_terms",
                )

            '''
            Primary-name containment catches modifier/base patterns even when
            the source term list is incomplete, for example:
                childhood disease name -> disease name
                adult disease name -> disease name
            The phrase generator only creates whole-word contiguous phrases.
            '''
            for source_name_phrase in generate_contiguous_phrases(source.words):
                self._add_rollup_pairs_for_phrase(
                    pairs_by_key,
                    names_by_phrase,
                    source,
                    source_name_phrase,
                    "source_name_contains_target_name",
                )

        rollup_pairs = sorted(
            pairs_by_key.values(),
            key=lambda pair: (pair.target_gard_id, pair.source_gard_id),
        )
        self.logger.info(f"Inferred {len(rollup_pairs)} source-to-base GARD roll-up pairs.")
        return rollup_pairs


    def _add_rollup_pairs_for_phrase(self, pairs_by_key: Dict[Tuple[str, str], RollupPair], names_by_phrase: Dict[str, List[GardName]], source: GardName, phrase: str, rollup_rule: str) -> None:

        """Add target pairs whose primary disease name equals one source phrase."""

        if not phrase:
            return

        targets = names_by_phrase.get(phrase, [])

        for target in targets:
            if not self._is_valid_rollup_target(source, target):
                continue

            if has_exclusion_context(source.words, target.words):
                continue

            pair_key = (source.gard_id, target.gard_id)
            current_pair = pairs_by_key.get(pair_key)

            if current_pair and current_pair.rollup_rule == "target_name_in_source_terms":
                continue

            pairs_by_key[pair_key] = RollupPair(
                source_gard_id=source.gard_id,
                source_gard_name=source.name,
                target_gard_id=target.gard_id,
                target_gard_name=target.name,
                rollup_rule=rollup_rule,
            )


    def _is_valid_rollup_target(self, source: GardName, target: GardName) -> bool:

        """Return True when target is a shorter/base disease candidate for source."""

        if source.gard_id == target.gard_id:
            return False

        if source.normalized_name == target.normalized_name:
            return False

        if self.base_gard_ids and target.gard_id not in self.base_gard_ids:
            return False

        if len(target.normalized_name) < self.min_base_name_chars:
            return False

        return len(target.words) < len(source.words)


    def _log_rollup_pair_examples(self, rollup_pairs: List[RollupPair]) -> None:

        """Log a small sample of inferred rules so the operator can sanity-check them."""

        for pair in rollup_pairs[:MAX_LOGGED_PAIR_EXAMPLES]:
            self.logger.info(
                "Roll-up pair example: "
                f"{pair.source_gard_id} ({pair.source_gard_name}) -> "
                f"{pair.target_gard_id} ({pair.target_gard_name}); rule={pair.rollup_rule}"
            )

        if len(rollup_pairs) > MAX_LOGGED_PAIR_EXAMPLES:
            self.logger.info(f"Skipped logging {len(rollup_pairs) - MAX_LOGGED_PAIR_EXAMPLES} additional roll-up pair examples.")


    def _create_temp_rollup_pair_table(self, rollup_pairs: List[RollupPair]) -> None:

        """Create and populate a connection-local temporary table of roll-up rules."""

        cursor = None

        try:
            cursor = self.mysql.cursor()
            cursor.execute(self.DROP_TEMP_ROLLUP_PAIRS_QUERY)
            cursor.execute(self.CREATE_TEMP_ROLLUP_PAIRS_QUERY)
            cursor.executemany(
                self.INSERT_TEMP_ROLLUP_PAIR_QUERY,
                [
                    (
                        pair.source_gard_id,
                        pair.source_gard_name,
                        pair.target_gard_id,
                        pair.target_gard_name,
                        pair.rollup_rule,
                    )
                    for pair in rollup_pairs
                ],
            )
            self.mysql.commit()
            self.logger.info(f"Loaded {cursor.rowcount} roll-up pairs into temporary MySQL repair table.")

        finally:
            if cursor is not None:
                cursor.close()


    def _create_temp_rollup_candidate_table(self) -> None:

        """Materialize missing repair rows once so insertion does not rescan every batch."""

        cursor = None
        start_time = time.time()

        try:
            cursor = self.mysql.cursor()
            cursor.execute(self.DROP_TEMP_ROLLUP_CANDIDATES_QUERY)
            cursor.execute(self.CREATE_TEMP_ROLLUP_CANDIDATES_QUERY)

            '''
            This is the one intentionally heavy query in the task. The original
            implementation ran this relationship/pair/existing anti-join before
            every 500-row insert batch. Materializing the result once changes the
            expensive part from "one full scan per batch" to "one full scan per
            script run"; the later insert loop only pages through the temporary
            candidate table by its auto-increment key.
            '''
            cursor.execute(self.MATERIALIZE_MISSING_MYSQL_ROLLUPS_QUERY)
            self.mysql.commit()
            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(
                f"Materialized {cursor.rowcount} missing roll-up candidates into temporary MySQL table. "
                f"Candidate build time={hours} hours, {minutes} minutes, {seconds} seconds."
            )

        finally:
            if cursor is not None:
                cursor.close()


    def _count_temp_rollup_candidates(self) -> int:

        """Count materialized target Project-to-GARD rows missing in MySQL."""

        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            cursor.execute(self.COUNT_TEMP_ROLLUP_CANDIDATES_QUERY)
            row = cursor.fetchone() or {}
            return int(row.get("missing_count") or 0)

        finally:
            if cursor is not None:
                cursor.close()


    def _insert_missing_mysql_rollups(self) -> int:

        """Insert materialized Project-to-base-GARD repair rows in keyset batches."""

        fetch_cursor = None
        insert_cursor = None
        inserted_count = 0
        batch_number = 0
        last_seen_candidate_id = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            insert_cursor = self.mysql.cursor()

            while True:
                fetch_cursor.execute(self.FETCH_TEMP_ROLLUP_CANDIDATES_QUERY, (last_seen_candidate_id, self.batch_size))
                rows = fetch_cursor.fetchall()

                if not rows:
                    break

                batch_number += 1
                last_seen_candidate_id = max(int(row.get("candidate_id") or last_seen_candidate_id) for row in rows)
                insert_values = [self._build_mysql_insert_values(row) for row in rows]

                try:
                    insert_cursor.executemany(self.INSERT_MYSQL_ROLLUP_RELATION_QUERY, insert_values)
                    self.mysql.commit()
                    inserted_count += len(insert_values)
                    self.logger.info(
                        f"Inserted MySQL roll-up batch {batch_number}: "
                        f"rows={len(insert_values)}, last_candidate_id={last_seen_candidate_id}, "
                        f"total_inserted={inserted_count}."
                    )

                except Exception:
                    self.mysql.rollback()
                    self.logger.exception(f"MySQL roll-up insert batch {batch_number} failed. Stopping before Memgraph sync.")
                    raise

            self.logger.info(f"Inserted {inserted_count} missing MySQL GARD Project roll-up rows.")
            return inserted_count

        finally:
            if fetch_cursor is not None:
                fetch_cursor.close()

            if insert_cursor is not None:
                insert_cursor.close()


    def _acquire_mysql_lock(self) -> bool:

        """Acquire a connection-scoped lock so two optimized repair runs do not overlap."""

        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            cursor.execute("SELECT GET_LOCK(%s, 0) AS lock_acquired", (MYSQL_LOCK_NAME,))
            row = cursor.fetchone() or {}
            lock_acquired = int(row.get("lock_acquired") or 0) == 1

            if lock_acquired:
                self.logger.info(f"Acquired MySQL makeup lock: {MYSQL_LOCK_NAME}.")

            return lock_acquired

        finally:
            if cursor is not None:
                cursor.close()


    def _release_mysql_lock(self) -> None:

        """Release the connection-scoped MySQL makeup lock before closing."""

        cursor = None

        try:
            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            cursor.execute("SELECT RELEASE_LOCK(%s)", (MYSQL_LOCK_NAME,))
            row = cursor.fetchone() or {}
            release_result = list(row.values())[0] if row else None
            self.logger.info(f"Released MySQL makeup lock: {MYSQL_LOCK_NAME}. release_result={release_result}.")

        except Exception:
            self.logger.exception(f"Failed to release MySQL makeup lock: {MYSQL_LOCK_NAME}.")

        finally:
            if cursor is not None:
                cursor.close()


    def _build_mysql_insert_values(self, row: Dict[str, Any]) -> Tuple[Any, ...]:

        """Build one MySQL insert tuple for a repaired roll-up relationship."""

        raw_result = {
            "rollup_rule": row.get("rollup_rule"),
            "source_relation_id": row.get("source_relation_id"),
            "source_gard_id": row.get("source_gard_id"),
            "source_gard_name": row.get("source_gard_name"),
            "target_gard_id": row.get("target_gard_id"),
            "target_gard_name": row.get("target_gard_name"),
        }

        return (
            row.get("target_gard_id"),
            row.get("application_id"),
            row.get("target_gard_name"),
            build_rollup_source_type(row.get("source_type")),
            row.get("confidence_score"),
            row.get("semantic_similarity"),
            row.get("core_project_num"),
            json.dumps(raw_result, sort_keys=True, default=str)[:MAX_RAW_RESULT_LENGTH],
        )


    def _sync_rollup_rows_to_memgraph(self) -> Dict[str, int]:

        """Sync all MySQL roll-up rows to Memgraph so reruns can recover graph failures."""

        if self.memgraph is None:
            self.logger.error("Unable to create Memgraph connection.")
            return {"submitted": 0, "matched": 0}

        cursor = None
        last_seen_id = 0
        summary = {
            "submitted": 0,
            "matched": 0,
            "batches_failed": 0,
        }

        try:
            cursor = self.mysql.cursor(dictionary=True, buffered=True)

            while True:
                cursor.execute(self.FETCH_ROLLUP_GRAPH_ROWS_QUERY, (last_seen_id, self.batch_size))
                rows = cursor.fetchall()

                if not rows:
                    break

                last_seen_id = max(int(row.get("id") or last_seen_id) for row in rows)
                chunks = [self._build_memgraph_chunk(row) for row in rows]

                try:
                    matched_count = self._upsert_memgraph_relationships(chunks)
                    summary["submitted"] += len(chunks)
                    summary["matched"] += matched_count
                    self.logger.info(
                        f"Submitted Memgraph roll-up batch ending at MySQL id={last_seen_id}: "
                        f"rows={len(chunks)}, matched={matched_count}, total_submitted={summary['submitted']}."
                    )

                except Exception:
                    summary["batches_failed"] += 1
                    self.logger.exception(f"Memgraph roll-up sync batch ending at MySQL id={last_seen_id} failed. Continuing.")

            self.logger.info(f"Completed Memgraph roll-up sync. Summary={summary}")
            return summary

        finally:
            if cursor is not None:
                cursor.close()


    def _build_memgraph_chunk(self, row: Dict[str, Any]) -> Dict[str, Any]:

        """Convert one MySQL roll-up row into the existing graph relationship shape."""

        rollup_details = parse_rollup_raw_result(row.get("raw_result"))

        return {
            "gardId": row.get("gard_id"),
            "applicationId": row.get("application_id"),
            "gardName": empty_if_none(row.get("gard_name")),
            "sourceType": empty_if_none(row.get("source_type")),
            "confidenceScore": score_to_graph_value(row.get("confidence_score")),
            "semanticSimilarity": score_to_graph_value(row.get("semantic_similarity")),
            "rollupSourceGardId": empty_if_none(rollup_details.get("source_gard_id")),
            "rollupSourceGardName": empty_if_none(rollup_details.get("source_gard_name")),
            "rollupRule": empty_if_none(rollup_details.get("rollup_rule")),
        }


    def _upsert_memgraph_relationships(self, chunks: List[Dict[str, Any]]) -> int:

        """Merge one batch of Project-to-GARD relationships into Memgraph."""

        if not chunks:
            return 0

        rows = list(self.memgraph.execute_and_fetch(self.UPSERT_MEMGRAPH_RELATIONSHIPS_CYPHER, {"chunks": chunks}))
        return int(rows[0].get("relationships_matched") or 0) if rows else 0


def normalize_phrase(value: Any) -> str:

    """Normalize a disease phrase to lowercase ASCII words separated by one space."""

    if value is None:
        return ""

    normalized_value = _normalize_txt(str(value)).lower()
    words = re.findall(r"[a-z0-9]+", normalized_value)
    return " ".join(words)


def split_normalized_terms(value: Any) -> Set[str]:

    """Split a $$$-delimited processed-term field into normalized exact terms."""

    terms: Set[str] = set()

    if not value:
        return terms

    for raw_term in str(value).split(TERM_SEPARATOR):
        term = normalize_phrase(raw_term)

        if term:
            terms.add(term)

    return terms


def generate_contiguous_phrases(words: Tuple[str, ...]) -> Iterable[str]:

    """Yield every whole-word contiguous phrase from a normalized source name."""

    for start_index in range(len(words)):
        for end_index in range(start_index + 1, len(words) + 1):
            yield " ".join(words[start_index:end_index])


def has_exclusion_context(source_words: Tuple[str, ...], target_words: Tuple[str, ...]) -> bool:

    """Return True when a target phrase appears in an excluding/without context."""

    if not source_words or not target_words:
        return False

    target_length = len(target_words)

    for start_index in range(0, len(source_words) - target_length + 1):
        if source_words[start_index:start_index + target_length] != target_words:
            continue

        '''
        Labels such as "astrocytoma (excluding glioblastoma)" contain the base
        phrase textually, but they mean the opposite of a roll-up. Looking only
        a few words back catches these exclusion labels while leaving ordinary
        modifier/base disease names such as "adult glioblastoma" untouched.
        '''
        context_start_index = max(0, start_index - EXCLUSION_CONTEXT_WINDOW)
        context_words = set(source_words[context_start_index:start_index])

        if context_words & EXCLUSION_CONTEXT_WORDS:
            return True

    return False


def build_rollup_source_type(source_type: Any) -> str:

    """Create a source_type value that identifies this row as inferred roll-up data."""

    source_type_value = str(source_type or "").strip()

    if not source_type_value:
        return "rollup"

    return f"rollup:{source_type_value}"[:MAX_SOURCE_TYPE_LENGTH]


def parse_rollup_raw_result(value: Any) -> Dict[str, Any]:

    """Parse roll-up metadata from raw_result, returning an empty dict on older rows."""

    if not value:
        return {}

    try:
        parsed_value = json.loads(str(value))

    except (TypeError, ValueError, json.JSONDecodeError):
        return {}

    return parsed_value if isinstance(parsed_value, dict) else {}


def empty_if_none(value: Any) -> Any:

    """Return an empty string for None, otherwise preserve the original value."""

    return "" if value is None else value


def score_to_graph_value(value: Any) -> str:

    """Convert nullable MySQL score values to graph string properties."""

    if value is None:
        return ""

    return str(value)


def parse_gard_id_filters(values: Optional[List[str]]) -> Set[str]:

    """Normalize repeated --base-gard-id and --source-gard-id values."""

    if not values:
        return set()

    return {
        value.strip()
        for value in values
        if value and value.strip()
    }


def parse_args() -> argparse.Namespace:

    """Parse command-line options for dry-run, scoped repair, and apply mode."""

    parser = argparse.ArgumentParser(description="Repair missing Project-to-GARD base disease roll-up relationships.")
    parser.add_argument("--apply", action="store_true", help="Write missing MySQL rows and sync roll-up relationships to Memgraph. Without this flag, only dry-run counts are logged.")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("GARD_PROJECT_ROLLUP_BATCH_SIZE", DEFAULT_BATCH_SIZE)), help="Number of MySQL rows to insert or sync per batch.")
    parser.add_argument("--min-base-name-chars", type=int, default=int(os.getenv("GARD_PROJECT_ROLLUP_MIN_BASE_NAME_CHARS", DEFAULT_MIN_BASE_NAME_CHARS)), help="Ignore inferred target/base disease names shorter than this many normalized characters.")
    parser.add_argument("--base-gard-id", action="append", default=[], help="Limit repair to one target/base GARD ID. Repeat the option for multiple IDs.")
    parser.add_argument("--source-gard-id", action="append", default=[], help="Limit repair to one source/specific GARD ID. Repeat the option for multiple IDs.")
    parser.add_argument("--skip-memgraph", action="store_true", help="Only repair MySQL rows; do not connect to or sync Memgraph.")
    return parser.parse_args()


if __name__ == "__main__":

    args = parse_args()
    GardProjectRollupRelationshipMakeupTask(
        apply_changes=args.apply,
        batch_size=args.batch_size,
        min_base_name_chars=args.min_base_name_chars,
        base_gard_ids=parse_gard_id_filters(args.base_gard_id),
        source_gard_ids=parse_gard_id_filters(args.source_gard_id),
        skip_memgraph=args.skip_memgraph,
    ).process_new_data()

import json
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.conn import DBConnection as db
from utils.applogger import AppLogger
from utils.tools import _make_hash_key, _remove_parentheses, _time_hms


# Reference: B_clinical_trial/initializer/organization_location.py
# Reference: D_grant/initializer/funding_IC.py
# Reference: F_person/initializer/agent.py

SourceRow = Tuple[str, str, str, str]
InsertRow = Tuple[str, Optional[int], str, str, str]


class OrganizationSourceTracker:
    """
    Track where each Organization came from.

    organization_location_source stores one row per source Organization mapping:
    - ClinicalTrial: responsible organization, source id = nctid
    - CoreProject: funding IC organization, source id = coreProjectNumber
    - Person: affiliation organization, source id = person_of_all_sources.id

    When a matching organization_location row exists, each source row is linked
    to the newest matching organization_location.id by
    original_name_in_graph_db_idx_key. Rows without a matching
    organization_location row are still inserted with organization_location_id
    set to NULL.
    """

    SOURCE_TABLE_NAME = "organization_location_source"
    ORGANIZATION_LOCATION_TABLE_NAME = "organization_location"

    SOURCE_NODE_TYPE_CLINICAL_TRIAL = "ClinicalTrial"
    SOURCE_NODE_TYPE_CORE_PROJECT = "CoreProject"
    SOURCE_NODE_TYPE_PERSON = "Person"

    CLINICAL_TRIAL_BATCH_SIZE = 500
    CORE_PROJECT_BATCH_SIZE = 1000
    PERSON_BATCH_SIZE = 5000
    LOCATION_LOOKUP_BATCH_SIZE = 1000


    def __init__(self):
        self.mysql = db().mysql_conn()

        self.log_dir = os.path.expanduser(os.getenv("ALERT_LOG_DIR", "logs"))
        os.makedirs(self.log_dir, exist_ok=True)

        class_name = type(self).__name__
        self.log_file = f"{self.log_dir}/alert-{class_name}.log"
        self.logger = AppLogger(class_name, self.log_file).get_logger()
        self.logger.info(f'\n\n{"*" * 20} The {class_name} is initialized. {"*" * 20}\n')

        self.INSERT_SOURCE_SQL = f"""
            INSERT INTO {self.SOURCE_TABLE_NAME}
            (
                org_original_name,
                organization_location_id,
                original_name_in_graph_db_idx_key,
                node_type_name,
                node_type_id
            )
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                org_original_name = VALUES(org_original_name),
                organization_location_id = COALESCE(
                    VALUES(organization_location_id),
                    organization_location_source.organization_location_id
                ),
                updated = CURRENT_TIMESTAMP
        """


    def update(self) -> None:
        """Track ClinicalTrial, CoreProject, and Person organization sources."""

        start_time = time.time()

        try:
            self.clinical_trial_source_update()

            self.core_project_source_update()

            self.person_source_update()

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(
                "Completed OrganizationSourceTracker. "
                f"time={hours} hours, {minutes} minutes, {seconds} seconds."
            )

        except Exception as e:
            self.logger.error(f"OrganizationSourceTracker failed: {e}")
            raise



    def clinical_trial_source_update(self) -> None:

        self.logger.info(f'\n\n{"*" * 30} clinical_trial_source_update() {"*" * 30}\n\n')

        query = """
            SELECT id, nctid, studies
            FROM clinical_trial_unique
            WHERE nctid IS NOT NULL
            AND id > %s
            ORDER BY id ASC
            LIMIT %s
        """

        self._process_source_batches(
            source_name=self.SOURCE_NODE_TYPE_CLINICAL_TRIAL,
            query=query,
            batch_size=self.CLINICAL_TRIAL_BATCH_SIZE,
            row_builder=self._create_clinical_trial_source_row,
        )


    def core_project_source_update(self) -> None:

        self.logger.info(f'\n\n{"*" * 30} core_project_source_update() {"*" * 30}\n\n')

        query = """
            SELECT
                gpru.id,
                p.CORE_PROJECT_NUM AS core_project_num,
                p.FULL_PROJECT_NUM AS full_project_num,
                p.IC_NAME AS ic_name
            FROM grant_gard_project_relation_unique_application_id AS gpru
            LEFT JOIN grant_project AS p
                ON gpru.application_id = p.APPLICATION_ID
            WHERE gpru.id > %s
            ORDER BY gpru.id ASC
            LIMIT %s
        """

        self._process_source_batches(
            source_name=self.SOURCE_NODE_TYPE_CORE_PROJECT,
            query=query,
            batch_size=self.CORE_PROJECT_BATCH_SIZE,
            row_builder=self._create_core_project_source_row,
        )


    def person_source_update(self) -> None:

        self.logger.info(f'\n\n{"*" * 30} person_source_update() {"*" * 30}\n\n')

        query = """
            SELECT id, affiliation
            FROM person_of_all_sources
            WHERE id > %s
            AND affiliation IS NOT NULL
            AND TRIM(affiliation) <> ''
            ORDER BY id ASC
            LIMIT %s
        """

        self._process_source_batches(
            source_name=self.SOURCE_NODE_TYPE_PERSON,
            query=query,
            batch_size=self.PERSON_BATCH_SIZE,
            row_builder=self._create_person_source_row,
        )


    def _process_source_batches(self, source_name: str, query: str, batch_size: int, row_builder) -> None:

        last_max_id = 0
        total_source_rows = 0
        total_db_changes = 0
        total_without_location = 0

        fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
        update_cursor = self.mysql.cursor()

        try:
            while True:
                fetch_cursor.execute(query, (last_max_id, batch_size))
                rows = fetch_cursor.fetchall()

                if not rows:
                    self.logger.info(
                        f'\n\n{"*" * 20} Finished {source_name} source tracking. '
                        f'Total inserted_or_updated={total_source_rows}, '
                        f'db changes={total_db_changes}, '
                        f'without_organization_location={total_without_location}. '
                        f'{"*" * 20}\n\n'
                    )
                    break

                source_rows = []

                for row in rows:
                    last_max_id = row["id"]
                    source_row = row_builder(row)

                    if source_row:
                        source_rows.append(source_row)

                if not source_rows:
                    self.logger.info(f"{source_name} batch through id={last_max_id}: no valid organization rows found.")
                    continue

                try:
                    inserted_or_updated, db_changes, without_location = self._insert_source_rows(
                        update_cursor,
                        source_rows,
                    )
                    self.mysql.commit()

                    total_source_rows += inserted_or_updated
                    total_db_changes += db_changes
                    total_without_location += without_location

                    self.logger.info(
                        f"{source_name} batch through id={last_max_id}: "
                        f"source rows={len(source_rows)}, inserted_or_updated={inserted_or_updated}, "
                        f"db changes={db_changes}, without_organization_location={without_location}, "
                        f"total inserted_or_updated={total_source_rows}."
                    )

                except Exception as e:
                    self.mysql.rollback()
                    self.logger.error(f"{source_name} source tracking failed through id={last_max_id}: {e}")
                    raise

        finally:
            update_cursor.close()
            fetch_cursor.close()


    def _create_clinical_trial_source_row(self, row: Dict[str, Any]) -> Optional[SourceRow]:

        nctid = self._clean_value(row.get("nctid"))

        if not nctid:
            return None

        try:
            study = json.loads(row.get("studies") or "{}")
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Invalid clinical_trial_unique.studies JSON for nctid={nctid}: {e}")
            return None

        if not isinstance(study, dict):
            return None

        protocol = study.get("protocolSection", {})
        if not isinstance(protocol, dict):
            return None

        identification_module = protocol.get("identificationModule", {})
        if not isinstance(identification_module, dict):
            return None

        organization_data = identification_module.get("organization", {})
        if not isinstance(organization_data, dict):
            return None

        organization = self._normalize_org_name(
            organization_data.get("fullName"),
            remove_parentheses=True,
        )

        if not organization:
            return None

        return (
            organization[:300],
            _make_hash_key(organization),
            self.SOURCE_NODE_TYPE_CLINICAL_TRIAL,
            nctid,
        )
    

    def _create_core_project_source_row(self, row: Dict[str, Any]) -> Optional[SourceRow]:

        core_project_num = self._clean_value(row.get("core_project_num") or row.get("full_project_num"))
        ic_name = self._normalize_org_name(row.get("ic_name"), remove_parentheses=False)

        if not core_project_num or not ic_name:
            return None

        return (
            ic_name[:300],
            _make_hash_key(ic_name),
            self.SOURCE_NODE_TYPE_CORE_PROJECT,
            core_project_num,
        )


    def _create_person_source_row(self, row: Dict[str, Any]) -> Optional[SourceRow]:

        person_id = row.get("id")
        affiliation = row.get("affiliation")

        if not person_id or not affiliation:
            return None

        org_name = self._normalize_org_name(affiliation, remove_parentheses=True)

        if not org_name:
            return None

        return (
            str(affiliation).strip()[:300],
            _make_hash_key(org_name),
            self.SOURCE_NODE_TYPE_PERSON,
            str(person_id),
        )


    def _insert_source_rows(self, update_cursor, source_rows: Sequence[SourceRow]) -> Tuple[int, int, int]:

        unique_source_rows = self._dedupe_source_rows(source_rows)
        location_ids_by_idx_key = self._fetch_latest_organization_location_ids(
            [source_row[1] for source_row in unique_source_rows]
        )

        insert_rows = []
        without_location = 0

        for org_original_name, org_idx_key, node_type_name, node_type_id in unique_source_rows:
            organization_location_id = location_ids_by_idx_key.get(org_idx_key)

            if organization_location_id is None:
                without_location += 1

            insert_rows.append((
                org_original_name,
                organization_location_id,
                org_idx_key,
                node_type_name,
                node_type_id,
            ))

        if not insert_rows:
            return 0, 0, without_location

        update_cursor.executemany(self.INSERT_SOURCE_SQL, insert_rows)

        return len(insert_rows), update_cursor.rowcount, without_location
    

    def _fetch_latest_organization_location_ids(self, org_idx_keys: Iterable[str]) -> Dict[str, int]:

        unique_idx_keys = [idx_key for idx_key in dict.fromkeys(org_idx_keys) if idx_key]
        location_ids_by_idx_key = {}

        if not unique_idx_keys:
            return location_ids_by_idx_key

        cursor = self.mysql.cursor(dictionary=True, buffered=True)

        try:
            for start in range(0, len(unique_idx_keys), self.LOCATION_LOOKUP_BATCH_SIZE):
                batch_idx_keys = unique_idx_keys[start:start + self.LOCATION_LOOKUP_BATCH_SIZE]
                placeholders = ", ".join(["%s"] * len(batch_idx_keys))
                query = f"""
                    SELECT
                        original_name_in_graph_db_idx_key,
                        MAX(id) AS organization_location_id
                    FROM {self.ORGANIZATION_LOCATION_TABLE_NAME}
                    WHERE original_name_in_graph_db_idx_key IN ({placeholders})
                    GROUP BY original_name_in_graph_db_idx_key
                """

                cursor.execute(query, tuple(batch_idx_keys))

                for row in cursor.fetchall():
                    location_ids_by_idx_key[row["original_name_in_graph_db_idx_key"]] = row["organization_location_id"]

        finally:
            cursor.close()

        return location_ids_by_idx_key
    

    def _dedupe_source_rows(self, source_rows: Sequence[SourceRow]) -> List[SourceRow]:

        unique_source_rows = []
        seen_keys = set()

        for source_row in source_rows:
            _, org_idx_key, node_type_name, node_type_id = source_row
            source_key = (org_idx_key, node_type_name, node_type_id)

            if source_key in seen_keys:
                continue

            seen_keys.add(source_key)
            unique_source_rows.append(source_row)

        return unique_source_rows
    

    def _normalize_org_name(self, value: Any, remove_parentheses: bool) -> str:

        org_name = self._clean_value(value)

        if not org_name:
            return ""

        if remove_parentheses:
            org_name = _remove_parentheses(org_name)

        return org_name.strip()


    def _clean_value(self, value: Any) -> str:
        return str(value).strip() if value is not None else ""
    

    def close(self) -> None:
        if self.mysql is not None and self.mysql.is_connected():
            self.mysql.close()

        self.mysql = None

        if hasattr(self, "logger") and self.logger is not None:
            for handler in list(self.logger.handlers):
                handler.flush()
                handler.close()
                self.logger.removeHandler(handler)

            self.logger = None



if __name__ == "__main__":
    tracker = OrganizationSourceTracker()

    try:
        tracker.update()
    finally:
        tracker.close()

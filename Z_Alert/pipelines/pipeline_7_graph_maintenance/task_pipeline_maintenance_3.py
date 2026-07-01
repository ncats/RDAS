import json
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _make_hash_key, _remove_parentheses, _time_hms

"""
Track new Organization source mappings for the alert pipeline.

This task is the incremental alert-pipeline version of: 
G_update/track_organization_source.py(This standalone script can rebuild organization_location_source across all rows.)

This task only reads rows marked is_new = 1 so it can run during normal alert
updates without rescanning the full ClinicalTrial, grant, and person tables.

organization_location_source stores one row per source mapping:
    ClinicalTrial organization -> node_type_name = ClinicalTrial, node_type_id = nctid
    CoreProject funding IC     -> node_type_name = CoreProject,   node_type_id = coreProjectNumber
    Person affiliation         -> node_type_name = Person,        node_type_id = person_of_all_sources.id

organization_location_id is resolved from organization_location.id when a row with the same original_name_in_graph_db_idx_key already exists.
If no organization_location row exists yet, the source row is still inserted with organization_location_id = NULL.
A later rerun can fill that link once the organization_location row has been created.
"""

# Reference: G_update/track_organization_source.py

# SourceRow is the normalized source mapping before organization_location_id is
# resolved. Tuple order:
#   1. org_original_name: original Organization text from the source table/JSON
#   2. original_name_in_graph_db_idx_key: graph Organization._idx_key hash
#   3. node_type_name: source type, such as ClinicalTrial/CoreProject/Person
#   4. node_type_id: source id, such as nctid/coreProjectNumber/person row id
SourceRow = Tuple[str, str, str, str]

# InsertRow is the final organization_location_source insert payload after the
# optional organization_location.id lookup. Tuple order:
#   1. org_original_name
#   2. organization_location_id, or None when no organization_location row exists
#   3. original_name_in_graph_db_idx_key
#   4. node_type_name
#   5. node_type_id
InsertRow = Tuple[str, Optional[int], str, str, str]


class NewOrganizationSourceTrackingTask(PipelineBase):
    """Track new source records that created or referenced Organization nodes."""

    SOURCE_TABLE_NAME = "organization_location_source"
    ORGANIZATION_LOCATION_TABLE_NAME = "organization_location"

    SOURCE_NODE_TYPE_CLINICAL_TRIAL = "ClinicalTrial"
    SOURCE_NODE_TYPE_CORE_PROJECT = "CoreProject"
    SOURCE_NODE_TYPE_PERSON = "Person"

    CLINICAL_TRIAL_BATCH_SIZE = 500
    CORE_PROJECT_BATCH_SIZE = 1000
    PERSON_BATCH_SIZE = 5000
    LOCATION_LOOKUP_BATCH_SIZE = 1000

    '''
    Keep the insert idempotent. The unique key on organization_location_source
    is (original_name_in_graph_db_idx_key, node_type_name, node_type_id), so
    the same source mapping will not be duplicated on a rerun.

    COALESCE protects an existing organization_location_id from being replaced
    by NULL if a later rerun cannot resolve the location row for some reason.
    '''
    INSERT_SOURCE_SQL = f"""
        INSERT INTO {SOURCE_TABLE_NAME}
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

    '''
    ClinicalTrials.gov stores the responsible organization inside the study
    JSON under protocolSection.identificationModule.organization.fullName.
    '''
    FETCH_NEW_CLINICAL_TRIALS_QUERY = """
        SELECT id, nctid, studies
        FROM clinical_trial_unique
        WHERE is_new = 1
        AND nctid IS NOT NULL
        ORDER BY id ASC
    """

    '''
    The graph initializer D_grant/initializer/funding_IC.py creates
    Organization nodes from grant_project.IC_NAME and links them to
    CoreProject nodes. This incremental task follows the same rule, but reads
    only grant_project rows marked is_new = 1.
    '''
    FETCH_NEW_CORE_PROJECTS_QUERY = """
        SELECT
            id,
            CORE_PROJECT_NUM AS core_project_num,
            FULL_PROJECT_NUM AS full_project_num,
            IC_NAME AS ic_name
        FROM grant_project
        WHERE is_new = 1
        ORDER BY id ASC
    """

    '''
    The person graph task links Agent nodes to Organization nodes from each
    person_of_all_sources.affiliation. This tracker stores the source as
    Person with node_type_id = person_of_all_sources.id, because the source
    table is tracking the exact MySQL person row, not the merged Agent node.
    '''
    FETCH_NEW_PERSONS_QUERY = """
        SELECT id, affiliation
        FROM person_of_all_sources
        WHERE is_new = 1
        AND affiliation IS NOT NULL
        AND TRIM(affiliation) <> ''
        ORDER BY id ASC
    """

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewOrganizationSourceTrackingTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Track Organization sources from only new alert-pipeline rows."""

        start_time = time.time()

        try:
            self.track_clinical_trial_sources()
            self.track_core_project_sources()
            self.track_person_sources()

            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(
                "Completed NewOrganizationSourceTrackingTask. "
                f"Time={hours} hours, {minutes} minutes, {seconds} seconds."
            )

        except Exception as e:
            self.logger.error(f"NewOrganizationSourceTrackingTask failed: {e}")

        finally:
            self.close()


    def track_clinical_trial_sources(self) -> None:
        """Track organizations from clinical_trial_unique rows where is_new = 1."""

        self._process_query_batches(
            source_name=self.SOURCE_NODE_TYPE_CLINICAL_TRIAL,
            query=self.FETCH_NEW_CLINICAL_TRIALS_QUERY,
            batch_size=self.CLINICAL_TRIAL_BATCH_SIZE,
            row_builder=self._create_clinical_trial_source_row,
        )


    def track_core_project_sources(self) -> None:
        """Track funding IC organizations from grant_project rows where is_new = 1."""

        self._process_query_batches(
            source_name=self.SOURCE_NODE_TYPE_CORE_PROJECT,
            query=self.FETCH_NEW_CORE_PROJECTS_QUERY,
            batch_size=self.CORE_PROJECT_BATCH_SIZE,
            row_builder=self._create_core_project_source_row,
        )


    def track_person_sources(self) -> None:
        """Track affiliation organizations from person rows where is_new = 1."""

        self._process_query_batches(
            source_name=self.SOURCE_NODE_TYPE_PERSON,
            query=self.FETCH_NEW_PERSONS_QUERY,
            batch_size=self.PERSON_BATCH_SIZE,
            row_builder=self._create_person_source_row,
        )


    def _process_query_batches(self, source_name: str, query: str, batch_size: int, row_builder) -> None:
        """
        Fetch new source rows, transform them to organization source tuples, and
        insert them into organization_location_source.

        The cursor streams in fetchmany() batches. The SQL query already limits
        the input set to is_new = 1, so there is no keyset pagination here; this
        task is intended for the small incremental alert set.
        """

        fetch_cursor = None
        update_cursor = None
        total_source_rows = 0
        total_db_changes = 0
        total_without_location = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            update_cursor = self.mysql.cursor()
            fetch_cursor.execute(query)

            while True:
                rows = fetch_cursor.fetchmany(batch_size)

                if not rows:
                    self.logger.info(
                        f"Finished {source_name} source tracking. "
                        f"Total inserted_or_updated={total_source_rows}, "
                        f"db changes={total_db_changes}, "
                        f"without_organization_location={total_without_location}."
                    )
                    break

                batch_num += 1
                source_rows = []

                for row in rows:
                    source_row = row_builder(row)

                    if source_row:
                        source_rows.append(source_row)

                if not source_rows:
                    self.logger.info(f"{source_name} batch #{batch_num}: no valid organization source rows found.")
                    continue

                inserted_or_updated, db_changes, without_location = self._insert_source_rows(
                    update_cursor,
                    source_rows,
                )
                self.mysql.commit()

                total_source_rows += inserted_or_updated
                total_db_changes += db_changes
                total_without_location += without_location

                self.logger.info(
                    f"{source_name} batch #{batch_num}: source rows={len(source_rows)}, "
                    f"inserted_or_updated={inserted_or_updated}, db changes={db_changes}, "
                    f"without_organization_location={without_location}, "
                    f"total inserted_or_updated={total_source_rows}."
                )

        except Exception as e:
            self.mysql.rollback()
            self.logger.error(f"{source_name} source tracking failed: {e}")
            raise

        finally:
            if update_cursor:
                update_cursor.close()

            if fetch_cursor:
                fetch_cursor.close()


    def _create_clinical_trial_source_row(self, row: Dict[str, Any]) -> Optional[SourceRow]:
        """Create one source tuple from one clinical_trial_unique JSON row."""

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

        # Match B_clinical_trial/initializer/organization_location.py:
        # remove the parenthetical acronym before hashing.
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
        """Create one source tuple from one new grant_project row."""

        core_project_num = self._clean_value(row.get("core_project_num") or row.get("full_project_num"))

        # Match D_grant/initializer/funding_IC.py:
        # IC_NAME is hashed directly, without removing parenthetical text.
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
        """Create one source tuple from one new person_of_all_sources row."""

        person_id = row.get("id")
        affiliation = row.get("affiliation")

        if not person_id or not affiliation:
            return None

        # Match F_person/initializer/agent.py:
        # person affiliations become Organization nodes using
        # _make_hash_key(_remove_parentheses(affiliation)).
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
        """
        Insert source rows after resolving organization_location_id where possible.

        Rows with no matching organization_location record are kept with a NULL
        organization_location_id. This preserves the source trail even when ROR
        lookup has not yet created a corresponding organization_location row.
        """

        unique_source_rows = self._dedupe_source_rows(source_rows)
        location_ids_by_idx_key = self._fetch_latest_organization_location_ids(
            [source_row[1] for source_row in unique_source_rows]
        )

        insert_rows: List[InsertRow] = []
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
        """
        Return the newest organization_location.id for each Organization _idx_key.

        organization_location can contain multiple rows for the same graph
        Organization key from repeated lookup attempts or later enrichment runs.
        MAX(id) mirrors the standalone tracker and chooses the newest row.
        """

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
        """
        Remove duplicate source mappings inside one batch.

        This reduces duplicate-key work before MySQL handles idempotency at the
        table level. The unique identity is the same as the table's unique key.
        """

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
        """
        Normalize an organization name before deriving the graph _idx_key.

        Different graph initializers used slightly different rules. The
        remove_parentheses flag keeps those source-specific rules explicit.
        """

        org_name = self._clean_value(value)

        if not org_name:
            return ""

        if remove_parentheses:
            org_name = _remove_parentheses(org_name)

        return org_name.strip()


    def _clean_value(self, value: Any) -> str:
        """Convert None to an empty string and trim real values."""

        return str(value).strip() if value is not None else ""

import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _arr, _normalize_tuple

"""
Create person rows for new grant project investigators.

It reads GARD-related grant projects where both `grant_project.is_new = 1` and
`grant_gard_project_relation.is_new = 1`, extracts principal-investigator names
and IDs from `grant_project`, and inserts those investigators into
`person_of_all_sources` with `is_new = 1`.
"""

# Reference: F_person/3_generate_person_of_grant.py


def extract_name(value: Any) -> Tuple[Optional[str], Optional[str]]:
    """Parse NIH grant names shaped like `LAST, FIRST` into first and last names."""

    if not value:
        return None, None

    parts = [part.strip() for part in str(value).strip().split(",")]

    if not parts or not parts[0]:
        return None, None

    if len(parts) == 1:
        return None, parts[0].lower().capitalize()

    if len(parts) == 2:
        return parts[1].lower().capitalize() if parts[1] else None, parts[0].lower().capitalize()

    last_name = parts[0].lower().capitalize()
    suffix = parts[1] if parts[1].upper() in {"JR", "SR", "II", "III", "IV", "V"} else None
    first_name_parts = parts[2:] if suffix else parts[1:]
    first_name = " ".join(part.lower().capitalize() for part in " ".join(first_name_parts).split())

    return first_name or None, last_name


class NewGrantPersonTask(PipelineBase):
    """Extract investigator person records from newly staged grant projects."""

    BATCH_SIZE = 100
    PERSON_TABLE = "person_of_all_sources"
    GRANT_PROJECT_TABLE = "grant_project"
    GRANT_GARD_PROJECT_RELATION_TABLE = "grant_gard_project_relation"
    ASSOCIATE_TYPE = "Grant_PI"
    SOURCE = "GrantProject"
    PERSON_TYPE = "investigator"
    CONTACT_MARKER = "(contact)"

    '''
    Start from grant_gard_project_relation so the task only handles grant
    projects that were linked to GARD in the current grant alert run. Group by
    project fields to avoid duplicating investigators when one application has
    multiple GARD relationships.
    '''
    FETCH_NEW_GRANT_PROJECTS_QUERY = f'''
        SELECT
            MIN(ggpr.id) AS relation_id,
            gp.APPLICATION_ID AS application_id,
            gp.ORG_NAME AS org_name,
            gp.PI_IDS AS pi_ids,
            gp.`PI_NAMEs` AS pi_names,
            gp.ORG_CITY AS org_city,
            gp.ORG_STATE AS org_state,
            gp.ORG_COUNTRY AS org_country,
            gp.ORG_ZIPCODE AS org_zipcode
        FROM {GRANT_GARD_PROJECT_RELATION_TABLE} AS ggpr
        INNER JOIN {GRANT_PROJECT_TABLE} AS gp
            ON gp.APPLICATION_ID = ggpr.application_id
            AND gp.is_new = 1
        WHERE
            ggpr.is_new = 1
            AND ggpr.application_id IS NOT NULL
        GROUP BY
            gp.APPLICATION_ID,
            gp.ORG_NAME,
            gp.PI_IDS,
            gp.`PI_NAMEs`,
            gp.ORG_CITY,
            gp.ORG_STATE,
            gp.ORG_COUNTRY,
            gp.ORG_ZIPCODE
        ORDER BY relation_id
    '''

    # person_of_all_sources is the shared staging table that later grouping and
    # graph tasks use to build unified Agent records.
    INSERT_PERSON_SQL = f'''
        INSERT INTO {PERSON_TABLE}
        (
            associate_id,
            associate_id_int,
            associate_type,
            source,
            first_name,
            last_name,
            role,
            affiliation,
            PI_id,
            location,
            person_type,
            is_new
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewGrantPersonTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Read new GARD-related grants, extract investigators, and insert person rows."""

        fetch_cursor = None
        insert_cursor = None
        total_inserted = 0
        batch_num = 0

        try:
            if self.mysql is None:
                self.logger.error("Unable to create MySQL connection.")
                return

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            insert_cursor = self.mysql.cursor(buffered=True)
            fetch_cursor.execute(self.FETCH_NEW_GRANT_PROJECTS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more new grant rows for person extraction.")
                    break

                batch_num += 1
                person_rows = []

                for row in rows:
                    grant_person_rows = self.create_grant_person_rows(row)

                    if grant_person_rows:
                        person_rows.extend(grant_person_rows)

                person_rows = self._dedupe_person_rows(person_rows)

                if not person_rows:
                    self.logger.info(f"Batch #{batch_num}: no valid grant investigator rows found.")
                    continue

                try:
                    normalized_person_rows = [_normalize_tuple(person_row) for person_row in person_rows]
                    insert_cursor.executemany(self.INSERT_PERSON_SQL, normalized_person_rows)
                    self.mysql.commit()

                    total_inserted += len(normalized_person_rows)
                    self.logger.info(
                        f"Batch #{batch_num}: inserted {len(normalized_person_rows)} "
                        f"grant person rows. Total inserted={total_inserted}."
                    )

                except Exception as exc:
                    self.logger.error(f"Error inserting grant person rows in batch #{batch_num}: {exc}")

                    if self.mysql:
                        self.mysql.rollback()

            self.logger.info(f"Completed grant person extraction. Total inserted={total_inserted}.")

        except Exception as exc:
            self.logger.error(f"NewGrantPersonTask failed: {exc}")

            if self.mysql:
                self.mysql.rollback()

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            if insert_cursor:
                insert_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def create_grant_person_rows(self, row: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        """Extract all investigator insert tuples from one grant project row."""

        application_id = row.get("application_id")

        if application_id is None:
            return []

        pi_ids = self._clean_parts(_arr(row.get("pi_ids")))
        pi_names = self._clean_parts(_arr(row.get("pi_names")))

        if not pi_ids and not pi_names:
            return []

        location = self.create_location(row)
        affiliation = self._truncate(row.get("org_name"), 4000)
        person_rows = []

        if not pi_ids:
            for index, name in enumerate(pi_names):
                # Preserve the historical behavior: when there is only one
                # investigator name and no PI ID, treat it as the contact PI.
                role, cleaned_name = self._extract_role_and_clean_value(name, mark_single_as_contact=len(pi_names) == 1 and index == 0)
                person_row = self.create_person_row(application_id, cleaned_name, None, role, affiliation, location)

                if person_row:
                    person_rows.append(person_row)

            return person_rows

        for index, pi_id in enumerate(pi_ids):
            name = pi_names[index] if index < len(pi_names) else None
            role, cleaned_pi_id = self._extract_role_and_clean_value(pi_id, mark_single_as_contact=len(pi_ids) == 1 and index == 0)
            name_role, cleaned_name = self._extract_role_and_clean_value(name)

            if name_role == "contact":
                role = "contact"

            person_row = self.create_person_row(application_id, cleaned_name, cleaned_pi_id, role, affiliation, location)

            if person_row:
                person_rows.append(person_row)

        return person_rows


    def create_person_row(self, application_id: Any, name: Any, pi_id: Any, role: Optional[str], affiliation: Any, location: Any) -> Optional[Tuple[Any, ...]]:
        """Convert one grant investigator name/ID into the shared person insert tuple."""

        first_name, last_name = extract_name(name)

        if first_name is None and last_name is None:
            return None

        return (
            application_id,
            application_id,
            self.ASSOCIATE_TYPE,
            self.SOURCE,
            self._truncate(first_name, 250),
            self._truncate(last_name, 250),
            self._truncate(role, 145),
            affiliation,
            self._truncate(pi_id, 45),
            location,
            self.PERSON_TYPE,
        )


    def create_location(self, row: Dict[str, Any]) -> Optional[str]:
        """Build the grant organization location string used by the historical script."""

        city = row.get("org_city") or ""
        state = row.get("org_state") or ""
        country = row.get("org_country") or ""
        zipcode = row.get("org_zipcode") or ""
        location = f"{city}, {state} {country} {zipcode}".strip()

        if location == ",":
            return None

        return self._truncate(location, 200)


    def _extract_role_and_clean_value(self, value: Any, mark_single_as_contact: bool = False) -> Tuple[Optional[str], Optional[str]]:
        """Remove `(contact)` markers and return the derived role with the clean value."""

        if value is None:
            return "contact" if mark_single_as_contact else None, None

        value = str(value).strip()
        role = "contact" if mark_single_as_contact else None

        if self.CONTACT_MARKER in value.lower():
            role = "contact"
            value = re.sub(r"\(\s*contact\s*\)", "", value, flags=re.IGNORECASE)

        value = value.strip()
        return role, value or None


    def _clean_parts(self, values: List[Any]) -> List[str]:
        """Strip split PI values and remove empty strings."""

        return [str(value).strip() for value in values if value is not None and str(value).strip()]


    def _dedupe_person_rows(self, person_rows: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        """Remove duplicate investigator rows while preserving first-seen order."""

        seen = set()
        deduped_rows = []

        for person_row in person_rows:
            if person_row in seen:
                continue

            seen.add(person_row)
            deduped_rows.append(person_row)

        return deduped_rows


    def _truncate(self, value: Any, max_length: int) -> Optional[str]:
        """Return a stripped string truncated for the target database column."""

        if value is None:
            return None

        value = str(value).strip()

        if not value:
            return None

        return value[:max_length]

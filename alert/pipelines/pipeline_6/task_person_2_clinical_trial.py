import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _normalize_tuple, _normalize_txt

"""
Create person rows for newly staged clinical trials.

It reads clinical_trial_unique rows where is_new = 1, extracts responsible
parties, central contacts, and overall officials from studies JSON, and inserts
those people into person_of_all_sources with is_new = 1.
"""

# Reference: F_person/2_generate_person_of_clinical_trial.py


def extract_name_title(value: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Split names shaped like 'Susan M. O'Brien, MD' into first name, last name,
    and title. Returns empty values safely for blank or malformed names.
    """
    if not value:
        return None, None, None

    value = str(value).strip()

    if not value:
        return None, None, None

    parts = value.split(",", 1)
    name_part = parts[0].strip()
    title = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
    name_parts = name_part.split()

    if not name_parts:
        return None, None, title

    first_name = name_parts[0]
    last_name = name_parts[-1]

    return first_name, last_name, title


class NewClinicalTrialPersonTask(PipelineBase):
    """Extract person records from newly staged clinical trial JSON."""

    BATCH_SIZE = 100
    PERSON_TABLE = "person_of_all_sources"
    CLINICAL_TRIAL_TABLE = "clinical_trial_unique"
    SOURCE = "ClinicalTrial"
    ASSOCIATE_TYPE_PI = "PI"
    ASSOCIATE_TYPE_CONTACT = "contact"

    # Only process new clinical trials that do not already have ClinicalTrial
    # person rows, which keeps repeated alert runs from duplicating people.
    FETCH_NEW_CLINICAL_TRIALS_QUERY = f'''
        SELECT DISTINCT
            ctu.nctid,
            ctu.studies
        FROM {CLINICAL_TRIAL_TABLE} AS ctu
        LEFT JOIN {PERSON_TABLE} AS p
            ON p.associate_id = ctu.nctid
            AND p.source = 'ClinicalTrial'
        WHERE ctu.is_new = 1
        AND ctu.nctid IS NOT NULL
        AND ctu.studies IS NOT NULL
        AND p.associate_id IS NULL
    '''

    # person_of_all_sources is the shared staging table that later grouping and
    # graph tasks use to build unified Person records.
    INSERT_PERSON_SQL = f'''
        INSERT INTO {PERSON_TABLE}
        (
            associate_id,
            associate_type,
            source,
            title,
            first_name,
            last_name,
            role,
            affiliation,
            email,
            phone,
            person_type,
            is_new
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewClinicalTrialPersonTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Read new trial JSON, extract people, and insert staging rows."""

        fetch_cursor = None
        insert_cursor = None
        total_inserted = 0
        batch_num = 0

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            insert_cursor = self.mysql.cursor(buffered=True)

            fetch_cursor.execute(self.FETCH_NEW_CLINICAL_TRIALS_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info("No more new clinical trial rows for person extraction.")
                    break

                batch_num += 1
                person_rows = []

                for row in rows:
                    nctid = row.get("nctid")
                    studies = row.get("studies")

                    # One trial may produce responsible-party, contact, and
                    # official rows. Invalid study JSON returns an empty list.
                    clinical_trial_person_rows = self.create_clinical_trial_person_rows(nctid, studies)

                    if clinical_trial_person_rows:
                        person_rows.extend(clinical_trial_person_rows)

                if not person_rows:
                    self.logger.info(f"Batch #{batch_num}: no valid clinical trial person rows found.")
                    continue

                try:
                    # Normalize tuple values before inserting so empty strings
                    # and text fields match the original person pipeline style.
                    normalized_person_rows = [_normalize_tuple(person_row) for person_row in person_rows]

                    insert_cursor.executemany(self.INSERT_PERSON_SQL, normalized_person_rows)
                    self.mysql.commit()

                    total_inserted += len(normalized_person_rows)
                    self.logger.info(
                        f"Batch #{batch_num}: inserted {len(normalized_person_rows)} "
                        f"clinical trial person rows. Total inserted={total_inserted}."
                    )

                except Exception as e:
                    self.logger.error(f"Error inserting clinical trial person rows in batch #{batch_num}: {e}")

                    if self.mysql:
                        self.mysql.rollback()

            self.logger.info(f"Completed clinical trial person extraction. Total inserted={total_inserted}.")

        except Exception as e:
            self.logger.error(f"NewClinicalTrialPersonTask failed: {e}")

            if self.mysql:
                self.mysql.rollback()

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            if insert_cursor:
                insert_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def create_clinical_trial_person_rows(self, nctid: Any, studies: Any) -> List[Tuple[Any, ...]]:

        '''
        Extract person records from one ClinicalTrials.gov study JSON payload.
        Each output tuple matches INSERT_PERSON_SQL.
        '''
        if not nctid or not studies:
            return []

        try:
            study = json.loads(studies) if isinstance(studies, str) else studies
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Error parsing studies JSON for nctid={nctid}: {e}")
            return []

        if not isinstance(study, dict):
            return []

        protocol = study.get("protocolSection") or {}

        if not isinstance(protocol, dict):
            return []

        person_rows = []
        # ClinicalTrials.gov stores people in multiple protocol modules, so
        # combine each source into the shared insert tuple format.
        person_rows.extend(self.extract_responsible_party(nctid, protocol))
        person_rows.extend(self.extract_central_contacts(nctid, protocol))
        person_rows.extend(self.extract_overall_officials(nctid, protocol))

        return person_rows


    def extract_responsible_party(self, nctid: Any, protocol: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        """Extract the sponsor/responsible party investigator, when present."""

        sponsor_module = protocol.get("sponsorCollaboratorsModule") or {}

        if not isinstance(sponsor_module, dict):
            return []

        responsible_party = sponsor_module.get("responsibleParty") or {}

        if not isinstance(responsible_party, dict):
            return []

        pi_name = responsible_party.get("investigatorFullName")

        if not pi_name:
            return []

        # investigatorFullName can include title suffixes, while
        # investigatorTitle may provide a cleaner title field.
        first_name, last_name, parsed_title = extract_name_title(pi_name)
        title = responsible_party.get("investigatorTitle") or parsed_title

        return [
            (
                nctid,
                self.ASSOCIATE_TYPE_PI,
                self.SOURCE,
                self._truncate(title, 500),
                self._truncate(first_name, 250),
                self._truncate(last_name, 250),
                self._truncate(responsible_party.get("type"), 145),
                self._truncate(responsible_party.get("investigatorAffiliation"), 4000),
                None,
                None,
                "sponsor",
            )
        ]


    def extract_central_contacts(self, nctid: Any, protocol: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        """Extract central contacts from the contacts/locations module."""

        contacts_module = protocol.get("contactsLocationsModule") or {}

        if not isinstance(contacts_module, dict):
            return []

        central_contacts = self._as_list(contacts_module.get("centralContacts"))
        person_rows = []

        for contact in central_contacts:
            if not isinstance(contact, dict):
                continue

            first_name, last_name, title = extract_name_title(contact.get("name"))

            if first_name is None and last_name is None:
                continue

            person_rows.append(
                (
                    # Central contacts are stored as contact rows and keep the
                    # phone/email fields when ClinicalTrials.gov provides them.
                    nctid,
                    self.ASSOCIATE_TYPE_CONTACT,
                    self.SOURCE,
                    self._truncate(title, 500),
                    self._truncate(first_name, 250),
                    self._truncate(last_name, 250),
                    self._truncate(contact.get("role"), 145),
                    None,
                    self._truncate(contact.get("email"), 145),
                    self._truncate(contact.get("phone"), 45),
                    "contact",
                )
            )

        return person_rows


    def extract_overall_officials(self, nctid: Any, protocol: Dict[str, Any]) -> List[Tuple[Any, ...]]:
        """Extract study officials, such as principal investigators or chairs."""

        contacts_module = protocol.get("contactsLocationsModule") or {}

        if not isinstance(contacts_module, dict):
            return []

        overall_officials = self._as_list(contacts_module.get("overallOfficials"))
        person_rows = []

        for official in overall_officials:
            if not isinstance(official, dict):
                continue

            first_name, last_name, title = extract_name_title(official.get("name"))

            if first_name is None and last_name is None:
                continue

            person_rows.append(
                (
                    # Overall officials usually have affiliation/role data but
                    # no direct contact details in the source payload.
                    nctid,
                    self.ASSOCIATE_TYPE_CONTACT,
                    self.SOURCE,
                    self._truncate(title, 500),
                    self._truncate(first_name, 250),
                    self._truncate(last_name, 250),
                    self._truncate(official.get("role"), 145),
                    self._truncate(official.get("affiliation"), 4000),
                    None,
                    None,
                    "study_chair",
                )
            )

        return person_rows


    def _as_list(self, value: Any) -> List[Any]:
        """Normalize optional singleton/list JSON fields to a list."""

        if value is None:
            return []

        if isinstance(value, list):
            return value

        return [value]


    def _truncate(self, value: Any, max_length: int) -> Optional[str]:
        """Normalize text and cap it to the target MySQL column length."""

        if value is None:
            return None

        value = _normalize_txt(value)

        if value is None:
            return None

        value = str(value).strip()

        if not value:
            return None

        return value[:max_length]

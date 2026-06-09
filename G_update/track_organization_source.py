import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.conn import DBConnection as db
from utils.applogger import AppLogger
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _id_range_generator, _curr_timestamp, _date_string, _make_hash_key, _remove_parentheses
  

# Reference: B_clinical_trial/initializer/organization_location.py
# Reference: D_grant/initializer/funding_IC.py
# Reference: F_person/initializer/agent.py

class OrganizationLocationNodeTypeAndIdUpdater(): 

    SOURCE_TABLE_NAME = "organization_location_source"
    SOURCE_NODE_TYPE_CLINICAL_TRIAL = "ClinicalTrial"
    SOURCE_NODE_TYPE_CORE_PROJECT = "CoreProject"
    SOURCE_NODE_TYPE_PERSON = "Person"
    

    def __init__(self):

        self.mysql = db().mysql_conn()

        '''
        Use the same alert log directory convention as the alert pipeline, but
        keep this updater independent from PipelineBase.
        '''
        self.log_dir = os.path.expanduser(os.getenv("ALERT_LOG_DIR", "logs"))
        os.makedirs(self.log_dir, exist_ok=True)

        class_name = type(self).__name__
        self.log_file = f"{self.log_dir}/alert-{class_name}.log"
        self.logger = AppLogger(class_name, self.log_file).get_logger()
        self.logger.info(f'\n\n{"*" * 20} The {class_name} is initialized. {"*" * 20}\n')
 

        self.INSERT_SOURCE_SQL = f'''
            INSERT INTO {self.SOURCE_TABLE_NAME}
            (
                org_original_name,
                organization_location_id,
                original_name_in_graph_db_idx_key,
                node_type_name,
                node_type_id
            )
            VALUES
            (
                %s,
                (
                    SELECT ol.id
                    FROM organization_location AS ol
                    WHERE ol.original_name_in_graph_db_idx_key = %s
                    ORDER BY ol.id DESC
                    LIMIT 1
                ),
                %s,
                %s,
                %s
            )
            ON DUPLICATE KEY UPDATE
                org_original_name = VALUES(org_original_name),
                organization_location_id = VALUES(organization_location_id),
                updated = CURRENT_TIMESTAMP
        '''


    def update(self)->None:

        #self.clinical_trial_node_type_and_id_update()
        #self.grant_node_type_and_id_update()
        #self.person_node_type_and_id_update()
        pass



    def clinical_trial_node_type_and_id_update(self)->None:

        self.logger.info(f'\n\n{"*" * 30} clinical_trial_node_type_and_id_update() {"*" * 30}\n\n')

        last_max_id = 0

        query = f'''
            SELECT id, nctid, studies  
            FROM clinical_trial_unique
            WHERE nctid IS NOT NULL 
            AND id > %s
            ORDER BY id ASC
            LIMIT %s
        '''

        update_cursor = self.mysql.cursor()
        fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
        total_source_rows = 0
        total_db_changes = 0

        try:
            while True:

                fetch_cursor.execute(query, (last_max_id, 200))

                rows = fetch_cursor.fetchall()

                if not rows:
                    self.logger.info(
                        f'\n\n{"*" * 20} Finished clinical_trial_node_type_and_id_update(). '
                        f'Total source rows={total_source_rows}, db changes={total_db_changes}. '
                        f'{"*" * 20}\n\n'
                    )
                    break

                source_rows = []

                for row in rows:

                    last_max_id = row['id']
                    source_row = self._create_clinical_trial_source_row(row)

                    if source_row:
                        source_rows.append(source_row)

                if not source_rows:
                    self.logger.info(f'ClinicalTrial source batch through id={last_max_id}: no valid organization rows found.')
                    continue

                try:
                    update_cursor.executemany(self.INSERT_SOURCE_SQL, source_rows)
                    self.mysql.commit()

                    total_source_rows += len(source_rows)
                    total_db_changes += update_cursor.rowcount
                    self.logger.info(
                        f'ClinicalTrial source batch through id={last_max_id}: upserted source rows={len(source_rows)}, '
                        f'db changes={update_cursor.rowcount}, total source rows={total_source_rows}.'
                    )

                except Exception as e:
                    self.logger.error(f'clinical_trial_node_type_and_id_update() failed: {e}')
                    self.mysql.rollback()
                    break

        finally:
            update_cursor.close()
            fetch_cursor.close()


    def _create_clinical_trial_source_row(self, row):
        """Create one organization_location_source tuple from clinical_trial_unique."""

        nctid = row.get('nctid')

        if not nctid:
            return None

        try:
            study = json.loads(row.get('studies') or '{}')

        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Invalid clinical_trial_unique.studies JSON for nctid={nctid}: {e}")
            return None

        if not isinstance(study, dict):
            return None

        protocol = study.get('protocolSection', {})
        if not isinstance(protocol, dict):
            return None

        identification_module = protocol.get('identificationModule', {})
        if not isinstance(identification_module, dict):
            return None

        organization_data = identification_module.get('organization', {})
        if not isinstance(organization_data, dict):
            return None

        organization = organization_data.get('fullName')
        if not organization:
            return None

        # Match the Organization _idx_key generation used by
        # B_clinical_trial/initializer/organization_location.py.
        organization = _remove_parentheses(str(organization))
        if not organization:
            return None

        org_idx_key = _make_hash_key(organization)

        return (
            organization[:300],
            org_idx_key,
            org_idx_key,
            self.SOURCE_NODE_TYPE_CLINICAL_TRIAL,
            nctid,
        )


    def grant_node_type_and_id_update(self)->None:

        self.logger.info(f'\n\n{"*" * 30} grant_node_type_and_id_update() {"*" * 30}\n\n')

        last_max_id = 0

        query = f'''
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
        '''

        update_cursor = self.mysql.cursor()
        fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
        total_source_rows = 0
        total_db_changes = 0

        try:
            while True:

                fetch_cursor.execute(query, (last_max_id, 500))
                rows = fetch_cursor.fetchall()

                if not rows:
                    self.logger.info(f'\n\n{"*" * 20} Finished grant_node_type_and_id_update(). Total source rows={total_source_rows}, db changes={total_db_changes}. {"*" * 20}\n\n')
                    break

                source_rows = []

                for row in rows:

                    last_max_id = row['id']
                    source_row = self._create_grant_source_row(row)

                    if source_row:
                        source_rows.append(source_row)

                if not source_rows:
                    self.logger.info(f'Grant source batch through id={last_max_id}: no valid organization rows found.')
                    continue

                try:
                    update_cursor.executemany(self.INSERT_SOURCE_SQL, source_rows)
                    self.mysql.commit()

                    total_source_rows += len(source_rows)
                    total_db_changes += update_cursor.rowcount
                    self.logger.info(
                        f'Grant source batch through id={last_max_id}: upserted source rows={len(source_rows)}, '
                        f'db changes={update_cursor.rowcount}, total source rows={total_source_rows}.'
                    )

                except Exception as e:
                    self.logger.error(f'grant_node_type_and_id_update() failed: {e}')
                    self.mysql.rollback()
                    break

        finally:
            update_cursor.close()
            fetch_cursor.close()


    def _create_grant_source_row(self, row):
        """Create one organization_location_source tuple from grant funding IC data."""

        core_project_num = row.get('core_project_num') or row.get('full_project_num')
        ic_name = row.get('ic_name')

        if not core_project_num or not ic_name:
            return None

        # Match the Organization _idx_key generation used by
        # D_grant/initializer/funding_IC.py.
        ic_name = str(ic_name).strip()
        if not ic_name:
            return None

        org_idx_key = _make_hash_key(ic_name)

        return (
            ic_name[:300],
            org_idx_key,
            org_idx_key,
            self.SOURCE_NODE_TYPE_CORE_PROJECT,
            str(core_project_num),
        )
 

    def person_node_type_and_id_update(self)->None:

        self.logger.info(f'\n\n{"*" * 30} person_node_type_and_id_update() {"*" * 30}\n\n')

        last_max_id = 0

        query = f'''
            SELECT
                id,
                affiliation
            FROM person_of_all_sources
            WHERE id > %s
            AND affiliation IS NOT NULL
            AND TRIM(affiliation) <> ''
            ORDER BY id ASC
            LIMIT %s
        '''

        update_cursor = self.mysql.cursor()
        fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
        total_source_rows = 0
        total_db_changes = 0

        try:
            while True:

                fetch_cursor.execute(query, (last_max_id, 1000))
                rows = fetch_cursor.fetchall()

                if not rows:
                    self.logger.info(f'\n\n{"*" * 20} Finished person_node_type_and_id_update(). Total source rows={total_source_rows}, db changes={total_db_changes}. {"*" * 20}\n\n')
                    break

                source_rows = []
                source_row_keys = set()

                for row in rows:

                    last_max_id = row['id']
                    source_row = self._create_person_source_row(row)

                    if source_row and source_row[2:] not in source_row_keys:
                        source_row_keys.add(source_row[2:])
                        source_rows.append(source_row)

                if not source_rows:
                    self.logger.info(f'Person source batch through id={last_max_id}: no valid organization rows found.')
                    continue

                try:
                    update_cursor.executemany(self.INSERT_SOURCE_SQL, source_rows)
                    self.mysql.commit()

                    total_source_rows += len(source_rows)
                    total_db_changes += update_cursor.rowcount
                    self.logger.info(
                        f'Person source batch through id={last_max_id}: upserted source rows={len(source_rows)}, '
                        f'db changes={update_cursor.rowcount}, total source rows={total_source_rows}.'
                    )

                except Exception as e:
                    self.logger.error(f'person_node_type_and_id_update() failed: {e}')
                    self.mysql.rollback()
                    break

        finally:
            update_cursor.close()
            fetch_cursor.close()


    def _create_person_source_row(self, row):
        """Create one organization_location_source tuple from person source data."""

        person_id = row.get('id')
        affiliation = row.get('affiliation')

        if not person_id or not affiliation:
            return None

        # Match the Agent affiliation Organization _idx_key generation used by
        # F_person/initializer/agent.py.
        affiliation = str(affiliation).strip()

        if not affiliation:
            return None

        org_name = _remove_parentheses(affiliation)

        if not org_name:
            return None

        org_idx_key = _make_hash_key(org_name)

        return (
            affiliation[:300],
            org_idx_key,
            org_idx_key,
            self.SOURCE_NODE_TYPE_PERSON,
            str(person_id),
        )


    def close(self)->None:
        self.mysql.close()

 

if __name__ == "__main__":
    
    updater = OrganizationLocationNodeTypeAndIdUpdater()

    try:
        updater.update()

    finally:
        updater.close()

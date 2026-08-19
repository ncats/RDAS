import os
import sys
import json
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
1. New Clinical Trial rows are already stored in clinical_trial by the discovery task.
2. Insert unique new Clinical Trial records into clinical_trial_unique.

3. clinical_trial_unique table holds UNIQUE Clinical Trials.

CREATE TABLE clinical_trial_unique (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nctid VARCHAR(255) NOT NULL,
    studies MEDIUMTEXT NULL
);
#
"""
# Reference: B_clinical_trial/init_1_clinical_trial_step_2.py

class NewClinicalTrialImportTask(PipelineBase):

    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=False)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("NewClinicalTrialImportTask does not implement find_new_data().")


    def process_new_data(self) -> None:

        self.step_1_add_new_nctid_to_clinical_trial()
        self.step_2_add_new_nctid_to_clinical_trial_unique()
        self.step_3_update_overall_status()
        self.step_4_update_brief_title_and_brief_summary()

        ''' Explicitly close the all the db connections '''
        self.close()



    ''' Clinical Trial discovery now writes directly to clinical_trial. '''
    def step_1_add_new_nctid_to_clinical_trial(self)-> None:

        self.logger.info("New clinical trial rows are inserted directly into clinical_trial by the discovery task.")


    ''' Insert new Clinical Trail into  clinical_trial_unique table '''
    def step_2_add_new_nctid_to_clinical_trial_unique(self)-> None:

        add_new_nctid_sql = '''
            INSERT INTO clinical_trial_unique (nctid, studies, is_new)
            SELECT
                ct.nctid,
                ct.studies,
                1 AS is_new
            FROM clinical_trial AS ct
            INNER JOIN (
                SELECT
                    nctid,
                    MAX(id) AS id
                FROM clinical_trial
                WHERE nctid IS NOT NULL
                AND nctid <> ''
                AND is_new = 1
                GROUP BY nctid
            ) AS latest_ct
                ON latest_ct.id = ct.id
            LEFT JOIN clinical_trial_unique AS ctu
                ON ctu.nctid = ct.nctid
            WHERE ctu.nctid IS NULL
        '''

        cursor = self.mysql.cursor()
        cursor.execute(add_new_nctid_sql)

        self.logger.info(f"\n{cursor.rowcount} rows from clinical_trial have been added into clinical_trial_unique table.\n")

        self.mysql.commit()



    def step_3_update_overall_status(self)-> None:

        '''
        Read protocolSection.statusModule.overallStatus from stored study JSON.

        clinical_trial_unique now has an overall_status column so later tasks can
        filter or reason about completed trials without repeatedly parsing the
        large studies MEDIUMTEXT JSON. This step backfills any missing
        overall_status value and also handles changed NCTIDs whose status was
        reset to NULL by ClinicalTrialStudyChangeHandler.
        '''
        select_missing_status_query = '''
            SELECT id, nctid, studies
            FROM clinical_trial_unique
            WHERE studies IS NOT NULL
            AND studies <> ''
            AND (overall_status IS NULL OR overall_status = '')
            ORDER BY id
            LIMIT %s
        '''

        batch_size = 100
        batch_num = 0
        fetch_cursor = None

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            while True:
                chunks = []

                fetch_cursor.execute(select_missing_status_query, (batch_size,))
                rows = fetch_cursor.fetchall()

                if not rows:
                    self.logger.info("No more clinical_trial_unique rows need overall_status updates.")
                    break

                batch_num += 1
                self.logger.info(f'\n--- overall_status batch# = {batch_num} ---')

                for row in rows:
                    nctid = row['nctid']
                    study_json = row['studies']

                    try:
                        study = json.loads(study_json)
                        protocol_section = study.get('protocolSection', {})
                        status_module = protocol_section.get('statusModule', {})
                        overall_status = status_module.get('overallStatus') or 'N/A'

                    except json.JSONDecodeError as e:
                        self.logger.error(f"Error parsing JSON while reading overall_status for NCTID {nctid}:\n {e}")
                        overall_status = 'N/A'

                    chunks.append((overall_status, nctid))
                    self.logger.info(f'Updated overall_status={overall_status} for NCTID = {nctid}')

                if chunks:
                    self._save_overall_status(chunks)

        except Exception as err:
            self.logger.error(f"Error updating overall_status: {err}")
            self.mysql.rollback()

        finally:
            if fetch_cursor:
                fetch_cursor.close()


    ''' Update the 2 columns: brief_title and brief_summary in the clinical_trial & clinical_trial_unique table '''
    def step_4_update_brief_title_and_brief_summary(self)-> None:

        select_new_query = f'''
            SELECT id, nctid, studies
            FROM clinical_trial_unique
            WHERE brief_title IS NULL
            -- AND id > %s
            AND is_new = 1
            ORDER BY id
            LIMIT %s
        '''

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            last_id = 0
            batch_size = 20
            batch_num = 0

            while True:

                chunks = []

                fetch_cursor.execute(select_new_query, (last_id, batch_size))
                rows = fetch_cursor.fetchall()

                if not rows:
                    self.logger.info(f"No more rows to fetch.")
                    break

                self.logger.info(f'\n--- batch# = {batch_num} ---')
                batch_num += 1

                for row in rows:
                    nctid = row['nctid']
                    study = row['studies']

                    try:
                        study = json.loads(study)

                        protocol_section = study.get('protocolSection', {})
                        brief_title = protocol_section.get('identificationModule', {}).get('briefTitle', 'N/A')
                        brief_summary = protocol_section.get('descriptionModule', {}).get('briefSummary', 'N/A')

                        chunks.append((brief_title, brief_summary, nctid))

                        self.logger.info(f'Updated brief_summary for NCTID = {nctid}')

                    except json.JSONDecodeError as e:
                        self.logger.error(f"Error parsing JSON for ID {nctid}:\n {e}")

                        chunks.append(('N/A', 'N/A', nctid))
                        continue

                ''' save the brief_title and brief_summary into the clinical_trial & clinical_trial_unique table '''
                if len(chunks) > 0:
                    self._save(chunks)

        except Exception as err:
            self.logger.error(f"Error: {err}")
            return None

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            # Close the mysql connection for the whole NewClinicalTrialImportTask.
            if self.mysql.is_connected():
                self.mysql.close()


    ''' save the brief_title and brief_summary into the clinical_trial & clinical_trial_unique table '''
    def _save(self, chunks):

        insert_1_sql = 'UPDATE clinical_trial set brief_title=%s, brief_summary=%s WHERE nctid=%s'
        insert_2_sql = 'UPDATE clinical_trial_unique set brief_title=%s, brief_summary=%s WHERE nctid=%s'

        try:
            cursor = self.mysql.cursor()
            cursor2 = self.mysql.cursor()

            cursor.executemany(insert_1_sql, chunks)
            self.mysql.commit()

            cursor2.executemany(insert_2_sql, chunks)
            self.mysql.commit()

            cursor.close()
            cursor2.close()

        except Exception as e:
            self.logger.error(e)
            self.mysql.rollback()


    def _save_overall_status(self, chunks):

        '''
        Save extracted ClinicalTrials.gov overall_status values.

        The chunks list contains (overall_status, nctid) tuples. Updating by
        NCTID is safe here because clinical_trial_unique has one canonical row
        per NCTID.
        '''
        update_sql = 'UPDATE clinical_trial_unique set overall_status=%s WHERE nctid=%s'
        cursor = None

        try:
            cursor = self.mysql.cursor()
            cursor.executemany(update_sql, chunks)
            self.mysql.commit()

        except Exception as e:
            self.logger.error(e)
            self.mysql.rollback()

        finally:
            if cursor:
                cursor.close()

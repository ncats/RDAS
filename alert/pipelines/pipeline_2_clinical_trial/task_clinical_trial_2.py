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
1. Find new Clinical Trail in table update_clinical_trial
2. Insert new Clinical Trail into  clinical_trial_unique table

3. clinical_trial_unique table holds UNIQUE Clinical Trails

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
        self.step_3_update_brief_title_and_brief_summary()

        ''' Explicitly close the all the db connections '''
        self.close()



    ''' Insert new Clinical Trail into  clinical_trial table '''
    def step_1_add_new_nctid_to_clinical_trial(self)-> None:

        add_new_nctid_sql = '''
            INSERT INTO clinical_trial (
                gardId,
                disease,
                nctid,
                brief_title,
                brief_summary,
                url,
                created,
                studies,
                processed,
                is_new
            )
            SELECT DISTINCT
                uct.gardId,
                uct.disease,
                uct.nctid,
                uct.brief_title,
                uct.brief_summary,
                uct.url,
                uct.created,
                uct.studies,
                uct.processed,
                uct.is_new
            FROM update_clinical_trial AS uct
            LEFT JOIN clinical_trial AS ct
                ON ct.nctid = uct.nctid
            WHERE uct.nctid IS NOT NULL
            AND uct.nctid <> ''
            AND ct.nctid IS NULL
        '''

        cursor = self.mysql.cursor()
        cursor.execute(add_new_nctid_sql)
        self.logger.info(f"\n{cursor.rowcount} rows form update_clinical_trial have been added into clinical_trial table.\n")

        self.mysql.commit()


    ''' Insert new Clinical Trail into  clinical_trial_unique table '''
    def step_2_add_new_nctid_to_clinical_trial_unique(self)-> None:

        add_new_nctid_sql = '''
            INSERT INTO clinical_trial_unique (nctid, studies, is_new)
            SELECT
                uct.nctid,
                uct.studies,
                1 AS is_new
            FROM update_clinical_trial AS uct
            INNER JOIN (
                SELECT
                    nctid,
                    MAX(id) AS id
                FROM update_clinical_trial
                WHERE nctid IS NOT NULL
                AND nctid <> ''
                AND is_new = 1
                GROUP BY nctid
            ) AS latest_uct
                ON latest_uct.id = uct.id
            LEFT JOIN clinical_trial_unique AS ctu
                ON ctu.nctid = uct.nctid
            WHERE ctu.nctid IS NULL
        '''

        cursor = self.mysql.cursor()
        cursor.execute(add_new_nctid_sql)

        self.logger.info(f"\n{cursor.rowcount} rows form update_clinical_trial have been added into clinical_trial_unique table.\n")

        self.mysql.commit()



    ''' Update the 2 columns: brief_title and brief_summary in the clinical_trial & clinical_trial_unique table '''
    def step_3_update_brief_title_and_brief_summary(self)-> None:

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

                        self.logger.info(f'NCTID = {nctid}')

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


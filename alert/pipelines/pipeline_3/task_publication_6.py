import os
import sys
import json

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _hash, _normalize_txt, _to_txt

'''
Retrieve chemical substances from update_publication_article and insert them
into publication_substance and publication_substance_unique tables.
'''
# Reference: C_publication/init_9_publication_substance.py


class PublicationTask_6(PipelineBase):

    '''
    Keep the fetch size in one place so the batch volume can be tuned safely.
    '''
    BATCH_SIZE = 100

    '''
    Step 1: Fetch new publication articles that may contain chemical substances.
    '''
    FETCH_QUERY = '''
        SELECT pubmed_id, source_json
        FROM update_publication_article
        WHERE is_new = 1
    '''

    '''
    Step 2: Insert filtered chemical rows with a plain multi-row insert.
    '''
    INSERT_SQL = '''
        INSERT INTO publication_substance (pubmed_id, substance_name, registry_number, hash_id)
        VALUES (%s, %s, %s, %s)
    '''

    '''
    Step 3: Insert unique substances from new publication articles only when the
    registry_number, substance_name, and hash_id combination does not exist.
    '''
    INSERT_UNIQUE_SQL = '''
        INSERT INTO publication_substance_unique (registry_number, substance_name, hash_id)
        SELECT DISTINCT
            ps.registry_number,
            ps.substance_name,
            ps.hash_id
        FROM publication_substance ps
        INNER JOIN update_publication_article upa
            ON upa.pubmed_id = ps.pubmed_id
        WHERE upa.is_new = 1
        AND NOT EXISTS (
            SELECT 1
            FROM publication_substance_unique psu
            WHERE psu.registry_number <=> ps.registry_number
            AND psu.substance_name <=> ps.substance_name
            AND psu.hash_id <=> ps.hash_id
        )
    '''

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    # Not implemented
    def find_new_data(self) -> None:
        raise NotImplementedError("PublicationTask_6 does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:

        count = 0
        batch_num = 0

        insert_cursor = None
        fetch_cursor = None

        try:
            insert_cursor = self.mysql.cursor()

            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(self.FETCH_QUERY)

            while True:

                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    self.logger.info(f"No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f'--- batch# = {batch_num} ---')

                chemicals = []

                for row in rows:
                    raw_pubmed_id = row.get('pubmed_id')

                    '''
                    Step 4: Validate pubmed_id so one bad row does not stop the batch.
                    '''
                    try:
                        pubmed_id = int(raw_pubmed_id)
                    except (TypeError, ValueError) as e:
                        self.logger.error(f'Invalid pubmed_id found: {raw_pubmed_id}. Error: {e}')
                        continue

                    source_json = row.get('source_json') or None

                    if not source_json:
                        self.logger.info(f'No valid source_json found for pubmed_id: {pubmed_id}')
                        continue

                    '''
                    Step 5: Parse source_json safely and continue on bad JSON.
                    '''
                    try:
                        source_obj = json.loads(source_json)
                    except (json.JSONDecodeError, TypeError) as e:
                        self.logger.error(f'Error parsing source_json for pubmed_id: {pubmed_id}. Error: {e}')
                        continue

                    '''
                    Step 6: Use the helper so chemical extraction is isolated.
                    '''
                    chemicals.extend(self.extract_chemicals(pubmed_id, source_obj))

                try:
                    if chemicals:
                        '''
                        Step 7: Filter existing pubmed_id and hash_id pairs once per batch.
                        '''
                        chemicals = self.filter_new_chemicals(chemicals, insert_cursor)

                    if chemicals:
                        # publication_substance table
                        insert_cursor.executemany(self.INSERT_SQL, chemicals)
                        self.mysql.commit()

                        inserted_count = insert_cursor.rowcount
                        count += inserted_count
                        '''
                        Step 8: Count actual inserted rows after batch duplicate filtering.
                        '''
                        self.logger.info(f'Inserted {inserted_count} of {len(chemicals)} chemicals into publication_substance table. Current total count = {count}')

                except Exception as e:
                    self.logger.error(f'While inserting into publication_substance table:\n{e}')
                    '''
                    Step 9: Roll back failed insert batches before continuing.
                    '''
                    self.mysql.rollback()

            '''
            Step 10: Save unique substances after publication_substance is updated.
            '''
            # publication_substance_unique
            self.insert_unique_substances()

        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            if insert_cursor:
                insert_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()


    def insert_unique_substances(self):
        '''
        Step 11: Insert unique substances for new publication articles.
        '''
        insert_unique_cursor = None

        try:
            insert_unique_cursor = self.mysql.cursor()
            insert_unique_cursor.execute(self.INSERT_UNIQUE_SQL)
            self.mysql.commit()

            self.logger.info(f'Inserted {insert_unique_cursor.rowcount} rows into publication_substance_unique table.')

        except Exception as e:
            self.logger.error(f'While inserting into publication_substance_unique table:\n{e}')
            '''
            Step 12: Roll back failed unique-substance inserts before continuing.
            '''
            self.mysql.rollback()

        finally:
            if insert_unique_cursor:
                insert_unique_cursor.close()


    def filter_new_chemicals(self, chemicals, cursor):
        '''
        Step 13: Remove duplicates already present in publication_substance before
        calling executemany. This avoids running NOT EXISTS once per row.
        '''
        unique_chemicals = []
        seen_pairs = set()

        for chemical in chemicals:
            pubmed_id, substance_name, registry_number, hash_id = chemical
            pair = (pubmed_id, hash_id)

            if pair in seen_pairs:
                continue

            seen_pairs.add(pair)
            unique_chemicals.append((pubmed_id, substance_name, registry_number, hash_id))

        if not unique_chemicals:
            return []

        placeholders = ', '.join(['(%s, %s)'] * len(unique_chemicals))
        lookup_query = f'''
            SELECT pubmed_id, hash_id
            FROM publication_substance
            WHERE (pubmed_id, hash_id) IN ({placeholders})
        '''

        lookup_values = []
        for pubmed_id, substance_name, registry_number, hash_id in unique_chemicals:
            lookup_values.extend([pubmed_id, hash_id])

        cursor.execute(lookup_query, lookup_values)
        existing_pairs = {
            (row[0], row[1])
            for row in cursor.fetchall()
        }

        return [
            chemical
            for chemical in unique_chemicals
            if (chemical[0], chemical[3]) not in existing_pairs
        ]


    def extract_chemicals(self, pubmed_id, source_obj):
        '''
        Step 14: Extract chemical substance records from source_json and build
        insert tuples that match INSERT_SQL.
        '''
        chemicals = []
        chemical_list = source_obj.get('chemicalList', {}).get('chemical', [])

        '''
        Step 15: Handle a single chemical object returned as a dict.
        '''
        if isinstance(chemical_list, dict):
            chemical_list = [chemical_list]

        '''
        Step 16: Skip unexpected chemical list shapes without stopping the task.
        '''
        if not isinstance(chemical_list, list):
            self.logger.info(f'No valid chemical list found for pubmed_id: {pubmed_id}')
            return chemicals

        for chem in chemical_list:
            '''
            Step 17: Skip invalid chemical items before calling dict methods.
            '''
            if not isinstance(chem, dict):
                self.logger.info(f'Invalid chemical item found for pubmed_id: {pubmed_id}. Item: {chem}')
                continue

            substance_name = _normalize_txt(chem.get('name'))
            registry_number = _normalize_txt(chem.get('registryNumber'))

            if registry_number == '0':
                registry_number = None

            if registry_number or substance_name:
                '''
                Step 18: Build a stable hash from registry number and substance name.
                '''
                hash_id = _hash(_to_txt(registry_number) + _to_txt(substance_name))

                '''
                Step 19: Add values for publication_substance bulk insert.
                '''
                chemicals.append((
                    pubmed_id,
                    substance_name,
                    registry_number,
                    hash_id
                ))

        return chemicals

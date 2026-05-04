import os
import sys 
import json
import time
from typing import Any, Dict, List, Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.pubtator_worker import PubtatorWorker 

"""
Retrieve pubtator data from API and insert into table publication_pubtator
"""
# Reference: C_publication/init_7a_publication-pubtator-Retrieve.py
# Reference: C_publication/init_7b_publication-pubtator-Parse.py

class PublicationTask_5(PipelineBase):


    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=False) 

        self.worker = PubtatorWorker()


    # Not implemented
    def find_new_data(self) -> None:
        
        raise NotImplementedError("PublicationTask_5 does not implement find_new_data().")
    

    # implement
    def process_new_data(self) -> None:
        
        ''' step 1 '''
        self.retrieve_pubtator()

        ''' step 2 '''
        self.parse_pubtator()

        ''' step 3  '''

        ''' Explicitly close the all the db connections '''
        self.close()


        
    def parse_pubtator(self):

        '''
        Select new PubMed IDs from update_publication_article that have PubTator
        source JSON in publication_pubtator, but have not yet been parsed into
        publication_pubtator_parsed.
        '''
        fetch_query = '''
            SELECT btp.pubmed_id, btp.source_json
            FROM (
                SELECT pp.pubmed_id, pp.source_json
                FROM update_publication_article upa
                INNER JOIN publication_pubtator pp ON upa.pubmed_id = pp.pubmed_id
                WHERE upa.is_new = 1
                AND pp.source_json IS NOT NULL
            ) btp
            LEFT JOIN publication_pubtator_parsed parsed
                ON btp.pubmed_id = parsed.pubmed_id
            WHERE parsed.pubmed_id IS NULL
        '''

        insert_sql = f'INSERT INTO publication_pubtator_parsed (pubmed_id, infons_identifier, infons_type, infons_text, relation_type) VALUES (%s, %s, %s, %s, %s)'

        count = 0
        batch_num = 0
        batch_size = 100

        insert_cursor = None
        fetch_cursor = None

        try:
            insert_cursor = self.mysql.cursor()
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            fetch_cursor.execute(fetch_query)

            while True:

                rows = fetch_cursor.fetchmany(batch_size)

                if not rows:
                    self.logger.info(f"No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f'--- batch# = {batch_num} ---')

                val_list = []

                for row in rows:
                    pubmed_id = row['pubmed_id']
                    source_json = row.get('source_json') or '{}'

                    try:
                        ''' This will raise a TypeError if row['source_json'] is None, or a JSONDecodeError if it's an empty string or invalid JSON. '''
                        data = json.loads(source_json)

                        if not data:
                            self.logger.info(f'No valid content found for pubmed_id: {pubmed_id}')
                            continue

                        pubTator3_content = data.get('PubTator3', [{}])
                        ''' Safely get the passages list from the first element, defaulting to an empty list '''
                        passages = pubTator3_content[0].get('passages', [])

                        if not passages:
                            self.logger.info(f'No PubTator3 or no passages found for pubmed_id: {pubmed_id}')
                            continue

                        relation_type = None

                        for passage in passages:
                            ''' Use .get() with a default empty dict to prevent errors if 'infons' is missing '''
                            relation_type = passage.get('infons', {}).get('type')

                            for ann in passage.get('annotations', []):
                                ''' Use .get() with default empty dictionary {} for safe nested access '''
                                ann_infons = ann.get('infons', {})

                                ''' Safely extract from nested dicts, defaulting to None if key is absent '''
                                obj = {
                                    'pubmed_id': pubmed_id,
                                    'infons_identifier': ann_infons.get('identifier'),
                                    'infons_type': ann_infons.get('type'),
                                    'infons_text': ann.get('text'),
                                    'relation_type': relation_type
                                }

                                val_list.append(obj)

                    except (json.JSONDecodeError, TypeError) as e:
                        ''' Catch issues with parsing or if 'source_json' is missing/None '''
                        self.logger.error(f'Error processing JSON for pubmed_id: {pubmed_id}. Error: {e}')
                        continue


                merged_val_list = self.merge_json_items(val_list)

                list_of_tuples = self.convert_to_tuples(merged_val_list)

                try:
                    if list_of_tuples:
                        insert_cursor.executemany(insert_sql, list_of_tuples)
                        self.mysql.commit()

                        count += len(list_of_tuples)
                        self.logger.info(f'Inserted {len(list_of_tuples)} rows into publication_pubtator_parsed table. Current total count = {count}')

                except Exception as e:
                    self.logger.error(f'While inserting into publication_pubtator_parsed table:\n{e}')
                    self.mysql.rollback()

        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            if insert_cursor:
                insert_cursor.close()
        
        self.logger.info(f'Total inserted = {count} rows into publication_pubtator_parsed table')
        

    def retrieve_pubtator(self):

        ''' Retrieve the pubmed_id's which are in update_publication_article table but not in publication_pubtator table '''
        fetch_query = '''
            SELECT upa.pubmed_id
            FROM update_publication_article upa
            LEFT JOIN publication_pubtator pp
                ON upa.pubmed_id = pp.pubmed_id
            WHERE upa.is_new = 1
            AND pp.pubmed_id IS NULL
        '''

        insert_sql = 'INSERT INTO publication_pubtator (pubmed_id, source_json) VALUES (%s, %s)'

        count = 0
        batch_num = 0
        batch_size = 20

        insert_cursor = None
        fetch_cursor = None

        try:
            insert_cursor = self.mysql.cursor()
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            fetch_cursor.execute(fetch_query)

            while True:

                rows = fetch_cursor.fetchmany(batch_size)

                if not rows:
                    self.logger.info(f"No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f'--- batch# = {batch_num} ---')

                pubmed_id_list = [row['pubmed_id'] for row in rows]

                val_list = []

                for pubmed_id in pubmed_id_list:
                    
                    pubmed_id, source_json  = self.worker.download_by_pmid(pubmed_id) 
 
                    if source_json:
                        source_json = json.dumps(source_json)
 
                    val_list.append((pubmed_id, source_json)) 
                     
                    count += 1
                     
                    ''' https://www.ncbi.nlm.nih.gov/research/pubtator3/api '''
                    ''' In order not to overload the PubTator3 server, we ask that users post no more than three requests per second. '''
                    time.sleep(0.5)

                try:              
                    insert_cursor.executemany(insert_sql, val_list)
                    self.mysql.commit()  
                    self.logger.info(f'Inserted {len(val_list)} rows into publication_pubtator table. Current total count = {count}')
                    self.logger.info('\n'.join(str(pubmed_id) for pubmed_id in pubmed_id_list))
     
                except Exception as e:
                    self.logger.error(f'{e}') 

            self.logger.info(f'\n*** Inserted total = {count} rows into publication_pubtator table ***')

        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            if insert_cursor:
                insert_cursor.close()

        self.logger.info(f'Total inserted = {count} rows into publication_pubtator table')
 


    def merge_json_items(self, json_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        '''
        Merge duplicate PubTator annotations by PubMed ID, identifier, type, and
        case-insensitive annotation text. Each merged annotation keeps the first
        original text value and collects unique relation types.
        '''
        merged_data = {}

        for item in json_list:
            pubmed_id = item.get('pubmed_id')
            infons_identifier = item.get('infons_identifier')
            infons_type = item.get('infons_type')
            infons_text = item.get('infons_text') or ''
            relation_type = item.get('relation_type')

            text_key = infons_text.casefold()
            key = (pubmed_id, infons_identifier, infons_type, text_key)

            if key not in merged_data:
                merged_data[key] = {
                    'pubmed_id': pubmed_id,
                    'infons_identifier': infons_identifier,
                    'infons_type': infons_type,
                    'infons_text': infons_text,
                    'relation_type': set()
                }

            if relation_type:
                merged_data[key]['relation_type'].add(str(relation_type).casefold())

        return [
            {
                'pubmed_id': value['pubmed_id'],
                'infons_identifier': value['infons_identifier'],
                'infons_type': value['infons_type'],
                'infons_text': value['infons_text'],
                'relation_type': sorted(value['relation_type'])
            }
            for value in merged_data.values()
        ]
    


    def convert_to_tuples(self, merged_val_list: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
        ''' Convert merged PubTator annotation dictionaries into database rows. '''
        return [
            (
                item.get('pubmed_id'),
                item.get('infons_identifier'),
                item.get('infons_type'),
                item.get('infons_text'),
                json.dumps(item.get('relation_type', []))
            )
            for item in merged_val_list
        ]

import os
import sys
import json
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv()

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from baseclass.conn import DBConnection as db
from utils.tools import ask_to_continue, _id_range_generator
from utils.minmaxid import MinMaxIdLoader

# --- Delete duplicate rows ---
'''
use rdas_db;
DELETE t1 FROM publication_pubtator AS t1
WHERE EXISTS (
    SELECT 1
    FROM publication_pubtator AS t2
    WHERE t2.pubmed_id = t1.pubmed_id
      AND t2.id < t1.id
);
'''

def merge_json_items(json_list):
    merged_data = {}

    for item in json_list:
        # Convert infons_text to lowercase for the key
        infons_text_lower = item['infons_text'].lower()
        key = (item['pubmed_id'], item['infons_identifier'], item['infons_type'], infons_text_lower)

        if key not in merged_data:
            # If the key doesn't exist, initialize with current values.
            # infons_text is taken as is from the *first* occurrence for the value,
            # but the grouping is based on the lowercase version.
            merged_data[key] = {
                'pubmed_id': item['pubmed_id'],
                'infons_identifier': item['infons_identifier'],
                'infons_type': item['infons_type'],
                'infons_text': item['infons_text'], # Keep original case for the value
                'relation_type': {item['relation_type'].lower()} # Use set for unique, lowercase relation_type
            }
        else:
            # If the key exists, only append to the 'relation_type' set
            merged_data[key]['relation_type'].add(item['relation_type'].lower())

    # Convert sets back to sorted lists for the final output
    final_output = []
    for key_tuple, value_dict in merged_data.items():
        value_dict['relation_type'] = sorted(list(value_dict['relation_type']))
        final_output.append(value_dict)

    # Sort the final output for consistent results, perhaps by pubmed_id, then identifier, then type, then text
    #final_output.sort(key=lambda x: (x['pubmed_id'], x['infons_identifier'], x['infons_type'], x['infons_text'].lower()))

    return final_output


def convert_to_tuples(merged_val_list):

    list_of_tuples = []

    for item in merged_val_list:

        list_of_tuples.append((
                item['pubmed_id'],
                item['infons_identifier'],
                item['infons_type'],
                item['infons_text'],
                json.dumps(item['relation_type'])
            ))

    return list_of_tuples


if __name__ == "__main__":

    publication_pubtator = 'publication_pubtator'
    publication_pubtator_parsed = 'publication_pubtator_parsed'

    ok = ask_to_continue(f'Parse pubtator data from table {publication_pubtator} and and insert into table {publication_pubtator_parsed}?')
    if not ok:
        sys.exit('------Stopped ------')

    # Deduplicate -- just run once
    deduplicate_sql = f'''
        DELETE t1 FROM rdas_db.{publication_pubtator} AS t1
        WHERE EXISTS (
            SELECT 1
            FROM rdas_db.{publication_pubtator} AS t2
            WHERE t2.pubmed_id = t1.pubmed_id
            AND t2.id < t1.id
        )
    '''

    insert_sql = f'INSERT INTO {publication_pubtator_parsed} (pubmed_id, infons_identifier, infons_type, infons_text, relation_type) VALUES (%s, %s, %s, %s, %s)'

    _count = 0
    batch_num = 0
    # Set a very long timeout (e.g., 8 hours = 28800 seconds)
    timeout_in_seconds = 28800

    min_id, max_id = MinMaxIdLoader().get_min_max_ids(publication_pubtator)
    min_id = 433001

    step = 1
    batch_size = 1000
    id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

    try:
        with db().mysql_conn() as fetch_conn, \
            fetch_conn.cursor(dictionary=True, buffered=True) as fetch_cursor, \
            db().mysql_conn() as insert_conn, \
            insert_conn.cursor(buffered=True) as insert_cursor, \
            db().mysql_conn() as update_conn, \
            update_conn.cursor(buffered=True) as update_cursor:

            # 1.  Deduplicate
            ''' Just run once '''
            '''
            try:
                update_cursor.execute(f"SET SESSION wait_timeout = {timeout_in_seconds}")
                update_cursor.execute(deduplicate_sql)
                update_conn.commit()
            except Exception as e:
                print(e)
            '''


            for start_id, end_id in id_ranges:

                fetch_query = f"""
                    WITH BatchToProcess AS (
                    SELECT pubmed_id, source_json
                    FROM rdas_db.{publication_pubtator}
                    WHERE id BETWEEN {start_id} AND {end_id} AND source_json IS NOT NULL
                )
                SELECT btp.pubmed_id, btp.source_json
                FROM BatchToProcess btp
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM rdas_db.{publication_pubtator_parsed} parsed2
                    WHERE btp.pubmed_id = parsed2.pubmed_id
                )
                """
                # 2.
                batch_num += 1
                print(f'\nBatch#: {batch_num}, fetched Id range [{start_id} - {end_id}]')

                fetch_cursor.execute(fetch_query)
                rows = fetch_cursor.fetchall()

                if not rows:
                    print('rows.length = 0')
                    continue

                val_list = []

                for row in rows:

                    pubmed_id = row['pubmed_id']
                    source_json = row['source_json']

                    try:
                        # This will raise a TypeError if row['source_json'] is None, or a JSONDecodeError if it's an empty string or invalid JSON.
                        data = json.loads(row.get('source_json') or '{}')

                        if not data:
                            print(f'No valid content found for pubmed_id: {pubmed_id}')
                            continue
                    except (json.JSONDecodeError, TypeError) as e:
                        # Catch issues with parsing or if 'source_json' is missing/None
                        print(f'Error processing JSON for pubmed_id: {pubmed_id}. Error: {e}')
                        continue


                    pubTator3_content = data.get('PubTator3', [{}])
                    # Safely get the passages list from the first element, defaulting to an empty list
                    passages = pubTator3_content[0].get('passages', [])

                    if not passages:
                        print(f'No PubTator3 or no passages found for pubmed_id: {pubmed_id}')
                        continue

                    relation_type = None

                    for passage in passages:
                        # Use .get() with a default empty dict to prevent errors if 'infons' is missing
                        relation_type = passage.get('infons', {}).get('type')

                        for ann in passage.get('annotations', []):
                            # Use .get() with default empty dictionary {} for safe nested access
                            ann_infons = ann.get('infons', {})

                            obj = {
                                'pubmed_id': pubmed_id,
                                # Safely extract from nested dicts, defaulting to None if key is absent
                                'infons_identifier': ann_infons.get('identifier'),
                                'infons_type': ann_infons.get('type'),
                                'infons_text': ann.get('text'),
                                'relation_type': relation_type
                            }

                            val_list.append(obj)

                # 3.
                merged_val_list = merge_json_items(val_list)
                list_of_tuples = convert_to_tuples(merged_val_list)

                try:
                    if len(list_of_tuples) > 0:
                        insert_cursor.executemany(insert_sql, list_of_tuples)
                        insert_conn.commit()

                        _count += len(list_of_tuples)
                        print(f'Total count = {_count}, insert size = {len(list_of_tuples)}')

                except Exception as e:
                    print(e)

    except Exception as e:
        print(e)

    print(f'\n\n -------------- Total count = {_count} -------------- \n\n')



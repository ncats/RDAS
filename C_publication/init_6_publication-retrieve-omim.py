import os
import sys
import json
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from utils.conn import DBConnection as db
from utils.https_request import HTTPSUtils as HttpsUtil

'''
    1. Get unique omim_id list from '{publication_gard_omim_mapping}' which are not in '{publication_omim}'
    2. Retrieve OMIM data from API and insert into table {publication_omim}
''' 

from utils.tools import ask_to_continue, _id_range_generator

publication_omim = 'publication_omim_'
publication_gard_omim_mapping = 'publication_gard_omim_mapping'

ok = ask_to_continue(f'Retrieve OMIM data from API and insert into table {publication_omim}?')
if not ok:
    sys.exit('------Stopped ------')


def get_omim(url):

    def parse_api_response(response):
        #empty_result = {'isEpi': False, 'probability': None}
        try: 
            response_json = response.json() 
            return response_json
        except TypeError as e:
            print(f'TypeError: {e}') 
        return None
 
    return HttpsUtil.with_api_retry_GET(url, parse_api_response)



api_key = os.getenv('OMIM_API_KEY')

fetch_conn = db().mysql_conn()
fetch_cursor = fetch_conn.cursor() 

insert_conn = db().mysql_conn()
insert_cursor = insert_conn.cursor() 

 
# 1. Get unique omim_id list from '{publication_gard_omim_mapping}' which are not in '{publication_omim}'
query = f'''
    SELECT DISTINCT q1.omim_id
    FROM rdas_db.{publication_gard_omim_mapping} q1
    LEFT JOIN rdas_db.{publication_omim} q2 ON q1.omim_id = q2.omim_id
    WHERE q2.omim_id IS NULL
    ORDER BY q1.omim_id;
'''
insert_sql = 'INSERT INTO {publication_omim} (omim_id, entry_json) VALUES (%s, %s)'

batch_num = 0
batch_size = 20

try:
    fetch_cursor.execute(query)

    while True:

        batch = fetch_cursor.fetchmany(batch_size)
        
        if not batch:
            print('\n\n---------------- All data fetched, no more data ----------------\n\n')
            break

        batch_num += 1
        omim_id_list = [row[0] for row in batch]

        val_list = []

        for omim_id in omim_id_list:

            url = f'https://api.omim.org/api/entry?mimNumber={omim_id}&include=all&format=json&apiKey={api_key}'
            #https://api.omim.org/api/entry?mimNumber=611126&include=all&format=json&apiKey=TV0j9GgAT3K4T8nyzOCQJw

            entry_json = get_omim(url)

            # Error: Failed executing the operation; Python type dict cannot be converted
            # Solution: json.dumps(entry_json)
            val = (omim_id, json.dumps(entry_json))
            val_list.append(val)

        try:  
            insert_cursor.executemany(insert_sql, val_list)
            insert_conn.commit()

            print(f'Batch#: {batch_num} ---') 
            
        except Exception as e:
            print(f'insert_sql error: \n{e}')
            sys.exit() 

except Exception as e:
    print(e)

 
if fetch_cursor:
    fetch_cursor.close()

if insert_cursor:
    insert_cursor.close()

if fetch_conn:
    fetch_conn.close()

if insert_conn:
    insert_conn.close()


print('\n\n-------------------------------- Done -------------------------------------\n\n')
 
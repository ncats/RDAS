import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from collections import OrderedDict
import requests
import time
import json
from dotenv import load_dotenv
load_dotenv()


from baseclass.conn import DBConnection as db
from utils.applogger import AppLogger
logger = AppLogger().get_logger()
 
"""  
# 1. Generate gard_id -> search item -> pubmed_id mapping table
#  
# (Do this BEFORE 3_publication/v2-init_2_2_publication-unique-pubmed-id-file.py, this py file is deprecated, ignore it)
"""

from utils.tools import ask_to_continue


publication_gard_pubmed = 'publication_gard_pubmed'
publication_gard_searchterm_pubmed_mapping = 'publication_gard_searchterm_pubmed_mapping'


ok = ask_to_continue(f'Generate  Generate gard_id - search_term - pubmd_id mapping and insert into database table {publication_gard_searchterm_pubmed_mapping}?')
if not ok:
    sys.exit('------Stopped ------')


mysql = db().mysql_conn()
select_cursor = mysql.cursor(buffered=True)
insert_cursor = mysql.cursor()

batch_size = 100
query = f'''
    SELECT
        gard_id,
        search_term,
        GROUP_CONCAT(pubmed_ids SEPARATOR ',') AS pubmed_ids 
    FROM rdas_db.{publication_gard_pubmed}
    WHERE year_range != 'ignore'
    GROUP BY gard_id, search_term
    ORDER BY gard_id, search_term
'''

# 2. Inert unique gard_id & pubmed_id mapping into table {publication_gard_searchterm_pubmed_mapping}
insert_sql = f'''
    INSERT INTO {publication_gard_searchterm_pubmed_mapping}
        (gard_id, search_term, pubmed_id)
    VALUES (%s, %s, %s)
'''

select_cursor.execute(query)

total_groups = 0
total_inserted = 0

while True:

    rows = select_cursor.fetchmany(batch_size)
    if not rows:
        break

    for row in rows:

        gard_id = row[0]
        search_term = row[1]
        ids_str = row[2]

        if not ids_str or not ids_str.strip():
            continue

        pubmed_ids = sorted({
            pubmed_id.strip()
            for pubmed_id in ids_str.split(',')
            if pubmed_id.strip()
        })

        val_list = [(gard_id, search_term, pubmed_id) for pubmed_id in pubmed_ids]
        if not val_list:
            continue

        insert_cursor.executemany(insert_sql, val_list)

        total_groups += 1
        total_inserted += len(val_list)
        print(f'{gard_id} - {len(val_list)}')

    mysql.commit()

print(f'\nInserted {total_inserted} rows from {total_groups} grouped gard/search_term rows.\n')

''' 
print(f'\n**********Process done, create indexes **********\n') 


# create indexes
idx_gard_id = f'CREATE INDEX idx_gard_id ON {publication_gard_searchterm_pubmed_mapping} (gard_id)'
insert_cursor.execute(idx_gard_id)
print(idx_gard_id)

idx_pubmed_id = f'CREATE INDEX idx_pubmed_id ON {publication_gard_searchterm_pubmed_mapping} (pubmed_id)'
insert_cursor.execute(idx_pubmed_id)
print(idx_pubmed_id)
  
idx_gard_id_searchterm = f'CREATE INDEX idx_gardid_searchterm ON {publication_gard_searchterm_pubmed_mapping} (gard_id, search_term)'
insert_cursor.execute(idx_gard_id_searchterm)
print(idx_gard_id_searchterm)
 
idx_gard_id_searchterm_pubmed = f'CREATE INDEX idx_gardid_searchterm_pubmed on  {publication_gard_searchterm_pubmed_mapping} (gard_id, search_term, pubmed_id)'
insert_cursor.execute(idx_gard_id_searchterm_pubmed)
print(idx_gard_id_searchterm_pubmed)
'''
# commit create indexes
mysql.commit()

if select_cursor:
    select_cursor.close()

if insert_cursor:
    insert_cursor.close()

if mysql:
    mysql.close()

print(f'\n\n---------- ALL DONE ----------\n\n') 
 
    # select *  from rdas_db.{publication_gard_pubmed} where year_range !='ignore' and year_range != '1970-2025';
    # select *  from rdas_db.{publication_gard_pubmed} where year_range !='ignore' and year_range != '1970-2025' and total > retmax;
    # select *  from rdas_db.{publication_gard_pubmed} where year_range !='ignore' and length(search_term)>4

    # NEXT Step
    #https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=9672646&format=json&resultType=core

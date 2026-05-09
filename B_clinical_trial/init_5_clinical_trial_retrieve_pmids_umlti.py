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

from multiprocessing import Pool, cpu_count
from baseclass.conn import DBConnection as db
from utils.tools import ask_to_continue, _id_range_generator, _normalize_txt, _clean
from utils.publication_worker import PublicationWorker

# Clean
''' update rdas_db.clinical_trial_unique set pmid_processed=null where pmid_processed is not null;'''

# Check duplicates
''' SELECT nctid, pmid, count(*) as cnt FROM rdas_db.clinical_trial_nctid_pmids_mapping group by nctid, pmid order by cnt desc; '''


def get_nctid_pmids(cursor, start_id, end_id):

    query = f'''
        SELECT nctid, studies  FROM clinical_trial_unique
        WHERE 
            nctid IS NOT NULL 
        AND 
            (id BETWEEN {start_id} AND {end_id})
        AND 
            pmid_processed IS NULL
    '''

    cursor.execute(query)
    rows = cursor.fetchall()
            
    chunks = []
    for row in rows: 
        nctid = row['nctid'] 
        study = json.loads(row['studies'])

        ref_module = study.get('protocolSection', dict()).get('referencesModule', {}) 
        references = ref_module.get('references', [])

        if not references:
            continue

        for ref in references: 
            if not ref.get('pmid'):
                continue
 
            chunks.append((nctid, _clean(ref.get('pmid'))))
        
    return chunks

  

if __name__ == "__main__": 

    nctid_pmids_table_name = 'clinical_trial_nctid_pmids_mapping'

    ok = ask_to_continue(f'Find the ClinicalTrail reference PMIDs, and store into the table {nctid_pmids_table_name}?')
    if not ok:
        sys.exit('------Stopped ------')


    mysql = db().mysql_conn()
 
    fetch_cursor = mysql.cursor(buffered=True, dictionary=True)
    update_cursor = mysql.cursor(buffered=True)
    insert_nctid_pmid_cursor = mysql.cursor(buffered=True) 
 
    # SELECT max(id) FROM rdas_db.clinical_trial_unique;
    min_id = 0
    max_id = 378099
    step = 3
    batch_size = 20

    _count = 0
    id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

    #
    insert_nctid_pmid_sql = 'INSERT INTO clinical_trial_nctid_pmids_mapping (nctid, pmid) VALUES(%s,%s)' 
 
    with Pool(processes=20) as pool:

        for start_id, end_id in id_ranges:

            chunks = get_nctid_pmids(fetch_cursor, start_id, end_id) 

            #1. 
            if len(chunks) == 0:
                print(f'\n{Fore.BLUE}[{start_id} - {end_id}] chunks.size = 0{Style.RESET_ALL}\n')
                update_cursor.execute(f'UPDATE clinical_trial_unique SET pmid_processed = 1 WHERE id BETWEEN {start_id} AND {end_id}')
                mysql.commit() 
                continue

            #2. Populate table
            try: 
                insert_nctid_pmid_cursor.executemany(insert_nctid_pmid_sql, chunks)
                mysql.commit() 

                _count += len(chunks)
                
            except Exception as e:
                print(e)
   
            except Exception as e:
                print(e)
            
            print(f'{Fore.BLUE+Style.BRIGHT} --- [{start_id} - {end_id}] - insert size = {len(chunks)}  - Total: {_count} ---{Style.RESET_ALL}\n')
   
    print(f'{Fore.BLUE+Style.BRIGHT}{"="*50} Total = {_count} {"="*50}{Style.RESET_ALL}\n\n') 


    if fetch_cursor:
        fetch_cursor.close()

    if update_cursor:
        update_cursor.close()

    if insert_nctid_pmid_cursor:
        insert_nctid_pmid_cursor.close() 

    if mysql:
        mysql.close()






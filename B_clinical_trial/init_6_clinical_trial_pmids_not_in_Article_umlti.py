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
from utils.conn import DBConnection as db
from utils.tools import ask_to_continue, _id_range_generator, _normalize_txt, _clean
from utils.publication_worker import PublicationWorker

# Clean
'''update rdas_db.clinical_trial_nctid_pmids_mapping set pmid_processed = null where pmid_processed is not null;'''
 
 

def ct_pmids_need_to_download(cursor, start_id, end_id):
     
    query = f'''
        SELECT DISTINCT ctnp.pmid
        FROM  rdas_db.clinical_trial_nctid_pmids_mapping ctnp
       
        LEFT JOIN rdas_db.publication_article pa ON ctnp.pmid = pa.pubmed_id 

        WHERE ctnp.id BETWEEN {start_id} AND {end_id}
        AND pa.pubmed_id IS NULL
        AND ctnp.pmid_processed IS NULL 
    '''

    cursor.execute(query)
    rows = cursor.fetchall()
    pmids = [row['pmid'] for row in rows]

    return pmids
 

def download_by_pmid(pmid): 
    return PublicationWorker().download_by_pmid(pmid) 


def update_pmid_processed(start_id, end_id):
    return f'UPDATE clinical_trial_nctid_pmids_mapping SET pmid_processed = 1 WHERE id BETWEEN {start_id} AND {end_id}'


if __name__ == "__main__": 
 
    publication_article_table = 'publication_article'

    ok = ask_to_continue(f'''
        Find the ClinicalTrail reference PMIDs which are in clinical_trial_nctid_pmids_mapping but not present in the publication_article table, 
        download the publication by pmid and store into the table {publication_article_table}?
    ''')

    if not ok:
        sys.exit('------Stopped ------')


    mysql = db().mysql_conn()
 
    fetch_cursor = mysql.cursor(buffered=True, dictionary=True)
    update_cursor = mysql.cursor(buffered=True)
    insert_publication_cursor = mysql.cursor(buffered=True) 
 
    # SELECT max(id) FROM rdas_db.clinical_trial_nctid_pmids_mapping;
    min_id = 0
    max_id = 227910
    step = 1
    batch_size = 100

    _count = 0
    id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

    insert_publication_sql = PublicationWorker().get_insert_sql(publication_article_table)
 
    with Pool(processes=20) as pool:

        for start_id, end_id in id_ranges:

            pmids = ct_pmids_need_to_download(fetch_cursor, start_id, end_id) 

            #1. 
            if len(pmids) == 0:
                print(f'\n{Fore.CYAN}[{start_id} - {end_id}] pmids.size = 0{Style.RESET_ALL}\n')
                update_cursor.execute(update_pmid_processed(start_id, end_id))
                mysql.commit() 
                continue 

            #2.
            batch_val = pool.map(download_by_pmid, pmids)
            batch_val = [val for val in batch_val if val is not None]
            
            _count += len(pmids)

            try: 
                insert_publication_cursor.executemany(insert_publication_sql, batch_val)
                mysql.commit() 

                update_cursor.execute(update_pmid_processed(start_id, end_id))
                mysql.commit() 
                
                print(f'\n{Fore.GREEN}[{start_id} - {end_id}], total count = {_count}. This batch insert size = {len(batch_val)}{Style.RESET_ALL}\n') 

            except Exception as e:
                print(e)
             
   
    print(f'{Fore.BLUE+Style.BRIGHT}{"="*50} Total = {_count} {"="*50}{Style.RESET_ALL}\n\n') 


    if fetch_cursor:
        fetch_cursor.close()

    if update_cursor:
        update_cursor.close()

    if insert_publication_cursor:
        insert_publication_cursor.close()

    if mysql:
        mysql.close()






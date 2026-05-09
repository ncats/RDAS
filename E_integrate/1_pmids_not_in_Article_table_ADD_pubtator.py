import os
import sys
import time
import random
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
from dotenv import load_dotenv
load_dotenv()

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from multiprocessing import Pool, cpu_count
from baseclass.conn import DBConnection as db
from utils.minmaxid import MinMaxIdLoader
from utils.tools import ask_to_continue, _id_range_generator, _normalize_txt, _clean
from utils.pubtator_worker import PubtatorWorker
#
# Add pubtator into table publication_pubtator
# https://ncats-nih.atlassian.net/browse/RM-9
#

'''
    SELECT min(id), max(id) FROM rdas_db.publication_pubtator; 
'''

def download_by_pmid(pmid):

    #time.sleep(random.random()*5)
    return PubtatorWorker().download_by_pmid(pmid)


if __name__ == "__main__":  

    worker = PubtatorWorker()

    publication_article_table = 'publication_article'

    ok = ask_to_continue(f'''Add {Fore.GREEN}{publication_article_table}{Style.RESET_ALL} table's {Fore.RED}pubtator{Style.RESET_ALL} into table {Fore.RED}publication_pubtator{Style.RESET_ALL} ?''')

    if not ok:
        sys.exit('------Stopped ------')


    min_id, max_id = MinMaxIdLoader().get_min_max_ids(publication_article_table)
    print(f'min_id: {min_id}, max_id: {max_id}')

    step = 1
    batch_size = 100

    _count = 0
    id_ranges = _id_range_generator(min_id, max_id, step, batch_size)


    mysql = db().mysql_conn()
    
    insert_pubtator_cursor = mysql.cursor(buffered=True) 

    fetch_cursor = mysql.cursor(buffered=True, dictionary=True) 

    insert_sql = 'INSERT INTO publication_pubtator (pubmed_id, source_json) VALUES (%s, %s)'
    
    # Multi will generate Errors --- too much requests the Client cannot handle. 
    # See the error examples below:
    '''
    Request error: 429 Client Error: Too Many Requests for url: https://www.ncbi.nlm.nih.gov/research/pubtator-api/publications/export/biocjson?pmids=3525597
    Request error: 429 Client Error: Too Many Requests for url: https://www.ncbi.nlm.nih.gov/research/pubtator-api/publications/export/biocjson?pmids=1963130
    '''
    """
    with Pool(processes=20) as pool:

        for start_id, end_id in id_ranges:

            #1. Select the pmids in {publication_article_table} which has no pubtator in publication_pubtator table
            query = f'''
                SELECT distinct pa.pubmed_id
                FROM {publication_article_table} pa
                LEFT JOIN publication_pubtator pp on pa.pubmed_id = pp.pubmed_id

                WHERE pa.id between {start_id} and {end_id}
                AND pp.pubmed_id is null
            '''
            
            pmids = []
            fetch_cursor.execute(query)
            rows = fetch_cursor.fetchall()
            for row in rows:
                pmids.append(row['pubmed_id'])

            #2. 
            batch_val = pool.map(download_by_pmid, pmids)
            batch_val = [val for val in batch_val if val is not None]
            
            #3.
            try:
                #insert_pubtator_cursor.executemany(insert_sql, batch_val)
                #mysql.commit() 
                _count += len(batch_val)
            except Exception as e:
                print(e)

            print(f'\n{Fore.GREEN}[{start_id} - {end_id}], total count = {_count}. This batch insert size = {len(batch_val)}{Style.RESET_ALL}\n') 
    """
    
    # Single
    for start_id, end_id in id_ranges:

            #1. Select the pmids in {publication_article_table} which has no pubtator in publication_pubtator table
            query = f'''
                SELECT distinct pa.pubmed_id
                FROM {publication_article_table} pa
                LEFT JOIN publication_pubtator pp on pa.pubmed_id = pp.pubmed_id

                WHERE pa.id between {start_id} and {end_id}
                AND pp.pubmed_id is null
            '''
            
            pmids = []
            fetch_cursor.execute(query)
            rows = fetch_cursor.fetchall()
            for row in rows:
                pmids.append(row['pubmed_id'])

            if len(pmids) == 0:
                print(f'\n{Fore.CYAN}[{start_id} - {end_id}], total count = {_count}. This batch insert size = 0{Style.RESET_ALL}\n') 
                continue
            
            #2. 
            batch_val = []
            print(f'\n------ {Fore.BLUE}[{start_id} - {end_id}], pmids.size = {len(pmids)} ------ {Style.RESET_ALL}')

            for pmid in pmids:
                print('.', end=' ', file=sys.stdout, flush=True)

                val = worker.download_by_pmid(pmid)
                batch_val.append(val)

                time.sleep(0.5)

            batch_val = [val for val in batch_val if val is not None]
            
            #3.
            try:
                insert_pubtator_cursor.executemany(insert_sql, batch_val)
                mysql.commit() 
                _count += len(batch_val)

            except Exception as e:
                print(e)

            print(f'\n****** {Fore.GREEN}[{start_id} - {end_id}], total count = {_count}. This batch insert size = {len(batch_val)}{Style.RESET_ALL} ******\n') 

    print(f'{Fore.BLUE+Style.BRIGHT}{"="*50} Done. Total = {_count} {"="*50}{Style.RESET_ALL}\n\n') 
        

        

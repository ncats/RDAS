import os
import sys
import time
import json
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv()

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from baseclass.conn import DBConnection as db
from utils.https_request import HTTPSUtils as HttpsUtil
from utils.tools import ask_to_continue, _elapsed_time
from utils.pubtator_worker import PubtatorWorker 
 
 
if __name__ == "__main__": 

    publication_article = 'publication_article'
    publication_pubtator = 'publication_pubtator' 

    ok = ask_to_continue(f'Retrieve pubtator data from API and insert into table {publication_pubtator}?')
    if not ok:
        sys.exit('------Stopped ------')
  

    pubmed_id_list = []
    batch_size = 100
    batch_num = 0
    count = 0

    worker = PubtatorWorker()

    start_time_0 = time.time()
   
    try: 
        with db().mysql_conn() as fetch_conn, \
             fetch_conn.cursor(dictionary=True, buffered=True) as fetch_cursor, \
             db().mysql_conn() as insert_conn, \
             insert_conn.cursor(dictionary=True, buffered=True) as insert_cursor:

            fetch_query = f'''
                SELECT pa.pubmed_id
                FROM {publication_article} pa
                LEFT JOIN {publication_pubtator} pp
                ON pa.pubmed_id = pp.pubmed_id
                WHERE pp.pubmed_id IS NULL
            '''

            #insert_sql = f'INSERT INTO {publication_pubtator} (pubmed_id, source_json, source_json_len) VALUES (%s, %s, %s)'
            insert_sql = f'INSERT INTO {publication_pubtator} (pubmed_id, source_json) VALUES (%s, %s)'
    
            fetch_cursor.execute(fetch_query)

            while True:
                batch = fetch_cursor.fetchmany(batch_size)

                if not batch:
                    print('\n\n---------------- All data fetched, no more data ----------------\n\n')
                    break
                
                batch_num += 1
                pubmed_id_list = [row['pubmed_id'] for row in batch]
            
                val_list = []
                for pubmed_id in pubmed_id_list:
                    
                    val = worker.download_by_pmid(pubmed_id)
                    print(f'pubmed_id = {pubmed_id}')

                    pubmed_id, source_json = val
                    #source_json_len = len(source_json) if source_json else 0

                    #val = (pubmed_id, source_json, source_json_len) 
                    if source_json:
                        source_json = json.dumps(source_json)

                    val = (pubmed_id, source_json)
                    val_list.append(val) 
                    
                    count += 1
                     
                    #https://www.ncbi.nlm.nih.gov/research/pubtator3/api
                    ''' In order not to overload the PubTator3 server, we ask that users post no more than three requests per second. '''
                    time.sleep(0.5)

                try:              
                    insert_cursor.executemany(insert_sql, val_list)
                    insert_conn.commit()  
    
                    print(f'{Fore.BLUE+Style.BRIGHT}Batch #: {batch_num}, count = {count}{Style.RESET_ALL}')
                    
                except Exception as e:
                    print(f'{e}')
                    sys.exit() 
        
        # The connections and cursors are automatically closed here
        # No need for manual close() calls

    except Exception as e:
        print(e)

      
    print(f'{Fore.BLUE+Style.BRIGHT}{"="*50} Done. Total = {count} {"="*50}{Style.RESET_ALL}\n') 
    
    end_time_0 = time.time()  
    hours, minutes, seconds = _elapsed_time(start_time_0, end_time_0)

    print(f'{Fore.BLUE+Style.BRIGHT}{"="*40} Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds {"="*40}{Style.RESET_ALL}\n\n') 
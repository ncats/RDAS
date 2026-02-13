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
from multiprocessing import Pool, cpu_count
from utils.https_request import HTTPSUtils as HttpsUtil
from utils.tools import ask_to_continue, elapsed_time, _to_txt

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

'''
Update is_EPI, is_NHS (default is NULL) in table {publication_article}
''' 

### ### ### Manually set is_abbreviation column ### ### ###
run_sql = '''
    UPDATE rdas_db.publication_gard_searchterm_pubmed_mapping pgs
    JOIN rdas_db.gard g 
        ON pgs.search_term = g.Label
    SET pgs.is_abbreviation = 1
    WHERE g.Label_Predicate_Mapping LIKE 'ABBRE%'
'''

def get_nhsExtract(texts):
   
    def parse_api_response(response):
        empty_result = {'isEpi': False, 'probability': None}
        try: 
            nhs_info = response.json() 
            if nhs_info:
                return nhs_info['predictions'][0]==1
            else:
                return False
        except KeyError as e:
            print(f'KeyError: {e}')
            print(f'\n{response}')
        except TypeError as e:
            print(f'TypeError: {e}')
        except AttributeError as e:
            print(f'AttributeError: {e}')
        return False
    
    payload = {'texts': texts}
    return HttpsUtil.with_api_retry(os.getenv('NHS_PREDICT_API'), payload, parse_api_response)


def get_isEpi(text):
    """
    Check if the given text corresponds to an epidemiology article using an external API.
    """ 
    def parse_api_response(response):
        #empty_result = {'isEpi': False, 'probability': None}
        try: 
            response = response.json() 
            if 'IsEpi' in response:
                return {'isEpi': response['IsEpi'], 'probability': response['EPI_PROB']}
                #return response['IsEpi']
            else:
                return False 
        except TypeError as e:
            print(f'TypeError: {e}') 
        return False


    payload = {'text': text}
    return HttpsUtil.with_api_retry(os.getenv('EPI_CLASSIFY_API'), payload, parse_api_response)
  


def get_epiExtract(text):
    """
        Extract epidemiological information from the given text using an external API.
        Returns:
        dict: A dictionary containing epidemiological information extracted from the text.
        {'DATE': ['1989'], 'LOC': ['Uruguay', 'Brazil'], 'STAT': ['1 in 10000', 1/83423], ...}
        {
            'EPI': ['crude incidence rate', 'crude incidence rates'], 
            'STAT': ['per 100,000 adults per year', '0.25 per 100,000 adults per year', 'to 1.11'], 
            'LOC': ['Zambia', 'Lusaka , Zambia', 'Europe', 'USA'], 
            'DATE': ['1980-1989', '1980-1983', '1984-1989'], 'SEX': None, 'ETHN': None
        }
    """
    def parse_api_response(response):
        try:
            return response.json()
        except Exception as e:
            print(f'Exception during get_epiExtract 1. text: {text}, error: {e}')
            return None
  
    payload = {'text': text,'extract_diseases':False}

    return HttpsUtil.with_api_retry(os.getenv('EPI_EXTRACT_API'), payload, parse_api_response)


 
def do_work(obj):
    
    id = obj['id']
    pubmed_id = obj['pubmed_id']
    title = _to_txt(obj['title'])
    abstract_text = _to_txt(obj['abstract_text'])

    text_to_predict =  (title+ ' ' + abstract_text).strip()  
    
    epiPredicted = get_isEpi(text_to_predict)
    
    is_EPI = epiPredicted['isEpi']
    epiProbability = epiPredicted['probability']
    
    is_NHS = get_nhsExtract([text_to_predict])
    
    print(f'OS.process_id:{os.getpid()}\tId:{id} - pubmed_id:{pubmed_id}\tis_EPI={is_EPI}\tepiProbability={epiProbability}\tis_NHS={is_NHS}')
            
    epiExtract = None
    if is_EPI:
        epiExtractJson = get_epiExtract(text_to_predict)
        if epiExtractJson:
            epiExtract = json.dumps(epiExtractJson)
            print(f'\t\t{epiExtract}') 

    return (is_EPI, is_NHS, epiProbability, epiExtract, pubmed_id) 

#
#
#
#
#
# For later UPDATE, use  5_integrate/update-EPI-NHS-of-publications-multi.py instead
#
#
#
#

if __name__ == "__main__": 
    
    print(f'\n\n{Fore.BLUE+Style.BRIGHT}Manually set is_abbreviation column:{Style.RESET_ALL}')
    print(run_sql)
    print(f'{Fore.BLUE+Style.BRIGHT}{"-"*100}{Style.RESET_ALL}\n\n')

    publication_article = 'publication_article' 

    #ok = ask_to_continue(f'Update is_EPI, is_NHS (default is NULL) in table {publication_article}?')
    #if not ok:
    #    sys.exit('------Stopped ------')
    

    batch_size = 10
    batch_num = 0
    start_time = time.time()

    try:
        with db().mysql_conn() as select_conn, \
             select_conn.cursor(dictionary=True, buffered=True) as select_cursor, \
             db().mysql_conn() as query_conn, \
             query_conn.cursor(dictionary=True, buffered=True) as query_cursor, \
             db().mysql_conn() as update_conn, \
             update_conn.cursor(buffered=True) as update_cursor:
            
            query = f'SELECT pubmed_id FROM {publication_article} WHERE is_EPI is null'
            update_sql = f" UPDATE {publication_article} SET is_EPI = %s, is_NHS = %s, epi_probability =%s, epi_extract = %s WHERE pubmed_id = %s "
             
            select_cursor.execute(query)
             
            with Pool(processes=batch_size) as active_pool:

                while True: 

                    batch = select_cursor.fetchmany(batch_size)

                    if not batch:
                        print('\n\n---------------- All data fetched, no more data ----------------\n\n')
                        break
                    
                    batch_num += 1
                    pubmed_id_list = [row['pubmed_id'] for row in batch]
                    
                    query = f"""
                        SELECT pubmed_id, title, abstract_text, id
                        FROM {publication_article}
                        WHERE pubmed_id IN {tuple(pubmed_id_list)} AND is_EPI is null
                    """
                    
                    query_cursor.execute(query)
                    results = query_cursor.fetchall()
                    
                    print(f'\n{Fore.BLUE+Style.BRIGHT}Batch# {batch_num}{Style.RESET_ALL}')
                    
                    obj_list = [{
                        'id': row['id'],
                        'title': row['title'],
                        'abstract_text': row['abstract_text'],
                        'pubmed_id': row['pubmed_id']
                    } for row in results]
                    
                    val_list = active_pool.map(do_work, obj_list)
                    
                    try:
                        update_cursor.executemany(update_sql, val_list)
                        update_conn.commit() 
                    
                    except Exception as e:
                        print(f"Error during update: {e}")
                        update_conn.rollback() # It's good practice to rollback on error

        # The connections and cursors are automatically closed here
        # No need for manual close() calls

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit('------Stopped ------')


    end_time = time.time()
    hours, minutes, seconds = elapsed_time(start_time, end_time)
    print(f'\n\n{Fore.BLUE+Style.BRIGHT}{"="*50} Completed. Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds {"="*50}{Style.RESET_ALL}\n\n') 

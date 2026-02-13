import os
import sys
import json
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import requests
import urllib3
import warnings
# Suppress only the InsecureRequestWarning
warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

import time
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from multiprocessing import Pool, cpu_count
from utils.conn import DBConnection as db
from utils.tools import ask_to_continue, _id_range_generator, _normalize_txt
from utils.publication_worker import PublicationWorker

# Create table grant_publication_not_in_article the SAME as publication_article
'''
The table grant_publication_not_in_article contains the PMIDs which are in Grant but not in publication_article.
'''

# Clean the grant_gard_project_relation data by
'''
UPDATE rdas_db.grant_gard_project_relation SET core_project_num = NULL WHERE TRIM(core_project_num) = '';
'''

# Criteria
'''
    grant_publication links to grant_linktable via pmid.
    grant_linktable links to grant_gard_project_relation via project_number and core_project_num.
    grant_gard_project_relation links to grant_gard_project_relation_unique_application_id via application_id.
''' 

# To retrieve gp.pmid from the rdas_db.grant_publication table that are not present in the rdas_db.publication_article table and grant_publication_not_in_article
'''
SELECT gp.pmid
FROM rdas_db.grant_publication gp
JOIN rdas_db.grant_linktable gl ON gp.pmid = gl.pmid
JOIN rdas_db.grant_gard_project_relation gpr ON gl.project_number = gpr.core_project_num
JOIN rdas_db.grant_gard_project_relation_unique_application_id gpru ON gpr.application_id = gpru.application_id
LEFT JOIN rdas_db.publication_article pa ON gp.pmid = pa.pubmed_id
LEFT JOIN rdas_db.grant_publication_not_in_article gpn ON gp.pmid = gpn.pubmed_id
WHERE gpru.pmid_processed is NULL)
AND gpr.core_project_num IS NOT NULL 
AND pa.pubmed_id IS NULL
AND gpn.pubmed_id IS NULL
'''

# Check duplicates
''' SELECT pubmed_id, count(*) as cnt FROM rdas_db.grant_publication_not_in_article group by pubmed_id order by cnt desc; '''

base_url = os.getenv('EURO_PEPMC_SERVICE_URL')
chars_to_remove = "!@#$%^&*()_+-={}[]|\\:;\"'<>,.?/`~"

def check_key_value(obj, key):
    result = key in obj and obj[key] == 'Y'
    return result
 

def get_pmids_need_to_download(start_id, end_id):

    query = f'''
        SELECT DISTINCT gp.pmid
        FROM rdas_db.grant_publication gp
        JOIN rdas_db.grant_linktable gl ON gp.pmid = gl.pmid
        JOIN rdas_db.grant_gard_project_relation gpr ON gl.project_number = gpr.core_project_num
        JOIN rdas_db.grant_gard_project_relation_unique_application_id gpru ON gpr.application_id = gpru.application_id
        LEFT JOIN rdas_db.publication_article pa ON gp.pmid = pa.pubmed_id 
        WHERE gpru.id BETWEEN {start_id} AND {end_id}
        AND gpru.pmid_processed IS NULL
        AND gpr.core_project_num IS NOT NULL
        AND pa.pubmed_id IS NULL 
    '''

    dict_cursor = mysql.cursor(dictionary=True, buffered=True)
    dict_cursor.execute(query)
    rows = dict_cursor.fetchall()

    pmids = [row['pmid'] for row in rows]
    dict_cursor.close()
    
    return pmids
    
''' 
def download_by_pmid(pmid):
    #https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:5770408&resultType=core&format=json&pageSize=1000    
    url = f"{base_url}?query=EXT_ID:{pmid}&resultType=core&format=json"
    
    retries = 0
    max_retries=10
    while retries < max_retries:
        try:                
            response = requests.get(url, verify=False)
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx and 5xx)                                
            try:
                bigObj = response.json()
            
                resultList = bigObj['resultList']['result']
                if len(resultList) <= 0:
                    break

                for result in resultList:
                    if 'pmid' not in result:
                        continue
                        
                    pubmed_id  = result['pmid']

                    # The query returned more other pubmed_id(s)
                    if str(pubmed_id) != str(pmid):
                        continue

                    source = _normalize_txt(result['source']) if 'source' in result else None
                    doi = _normalize_txt(result['doi']) if 'doi' in result else None
                    title = _normalize_txt(result['title']) if 'title' in result else None
                    abstract_text = _normalize_txt(result['abstractText']) if 'abstractText' in result else None
                    affiliation = _normalize_txt(result['affiliation']) if 'affiliation' in result else None
                    first_publication_date = result['firstPublicationDate'] if 'firstPublicationDate' in result else None
                    publication_year = int(datetime.strptime(result['firstPublicationDate'], '%Y-%m-%d').year) if 'firstPublicationDate' in result else None
                    is_open_access = check_key_value(result, 'isOpenAccess')
                    in_EPMC = check_key_value(result, 'inEPMC')
                    in_PMC = check_key_value(result, 'inPMC')
                    has_PDF = check_key_value(result, 'hasPDF')
                    
                    pub_type = json.dumps(result['pubTypeList']['pubType']) if 'pubTypeList' in result else None
                    cited_by_count = int(result['citedByCount']) if 'citedByCount' in result else 0
                    
                    if title:
                        title = title.strip(chars_to_remove)

                    if abstract_text:
                        abstract_text = abstract_text.strip(chars_to_remove)

                    print(f'pubmed_id = {pubmed_id}\t{publication_year}\t{doi}')

                     
                    #pubmed_id, doi, title, abstract_text, affiliation,
                    #first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC, 
                    #in_PMC, has_PDF,  pub_type, source_json
                     
                    val = (pubmed_id, doi, title, abstract_text, affiliation, 
                        first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC,
                        in_PMC, has_PDF, pub_type, json.dumps(result)
                        ) 

                    return val 

            except KeyError as e:
                print(f'KeyError: {e}\n{url}')
            except TypeError as e:
                print(f'TypeError: {e}\n{url}')
            except AttributeError as e:
                print(f'AttributeError: {e}\n{url}')
            
            break  # Exit the loop if successful
        except requests.exceptions.Timeout:
            retries += 1
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            break  # Exit the loop for non-retryable errors

    return None
'''

def download_by_pmid(pmid):
    return PublicationWorker().download_by_pmid(pmid)


def main_multiprocessing(num_processes, pmids):

    # Create a Pool of worker processes 'with' statement ensures the pool is properly closed
    with Pool(processes=num_processes) as pool:
        # map() applies the download_url function to each item in urls. 
        # For very long tasks, pool.apply_async() or pool.imap() might be better.

        # pool.map() is a blocking call. 
        # This means the main program's execution will pause at this line until all the tasks have been completed 
        # by the worker processes and all their results have been returned.
        # The 'results' is a list
        results = pool.map(download_by_pmid, pmids)

    return results


if __name__ == "__main__": 

    ok = ask_to_continue('Find the Grant publication PMIDs which are not present in the publication_article table , and store into the table grant_publication_not_in_article?')
    if not ok:
        sys.exit('------Stopped ------')


    mysql = db().mysql_conn()

    update_cursor = mysql.cursor(buffered=True)
    insert_cursor = mysql.cursor(buffered=True) 
 
    # select min(id), max(id) from rdas_db.grant_gard_project_relation_unique_application_id;
    min_id = 1 #200236 #163696 #82476 #1
    max_id = 388186
    step = 1
    batch_size = 5

    pmid_count = 0
    id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

    insert_sql = PublicationWorker().get_insert_sql()

    with Pool(processes=22) as pool:

        for start_id, end_id in id_ranges:
        
            pmids = get_pmids_need_to_download(start_id, end_id)
            if len(pmids) == 0:
                print(f'\n{Fore.BLUE}[{start_id} - {end_id}] pmids.size = 0{Style.RESET_ALL}\n')
                update_cursor.execute(f'UPDATE grant_gard_project_relation_unique_application_id SET pmid_processed = 1 WHERE id BETWEEN {start_id} AND {end_id}')
                mysql.commit() 
                continue

            #num_processes = min(len(pmids), cpu_count()-1) # Don't create more processes than tasks
            #batch_val = main_multiprocessing(num_processes=num_processes, pmids=pmids)

            ''' No overhead of Pool creation '''
            batch_val = pool.map(download_by_pmid, pmids)

            batch_val = [val for val in batch_val if val is not None]
            pmid_count += len(batch_val)

            try: 
                insert_cursor.executemany(insert_sql, batch_val)
                mysql.commit() 
                
                print(f'\n{Fore.BLUE}[{start_id} - {end_id}], total PMIDs = {pmid_count}. This batch insert size = {len(batch_val)}{Style.RESET_ALL}\n') 

                update_cursor.execute(f'UPDATE grant_gard_project_relation_unique_application_id SET pmid_processed = 1 WHERE id BETWEEN {start_id} AND {end_id}')
                mysql.commit() 

            except Exception as e:
                print(e)

    
    if insert_cursor:
        insert_cursor.close() 
    if update_cursor:
        update_cursor.close()
    if mysql:
        mysql.close()   


    print( f'{Fore.BLUE+Style.BRIGHT}{"="*50} Total = {pmid_count} {"="*50}{Style.RESET_ALL}\n\n')
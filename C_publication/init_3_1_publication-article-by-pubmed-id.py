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

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from utils.conn import DBConnection as db

#MySQL
"""
1. The pubmed_id in {publication_gard_searchterm_pubmed_mapping}
2. The pubmed_id is NOT in {publication_article}
3. Retrieve Articles information by the pubmed_id by API endpoint, and store into the table {publication_article}
"""

'''
SELECT CONCAT( GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ',')) 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'rdas_db' 
AND TABLE_NAME = '{publication_article}'
'''

from utils.tools import ask_to_continue, elapsed_time


publication_article = 'publication_article'
publication_gard_searchterm_pubmed_mapping = 'publication_gard_searchterm_pubmed_mapping'


ok = ask_to_continue(f'Retrieve Articles information by the pubmed_id by API endpoint, and store into the table {publication_article}?')
if not ok:
    sys.exit('------Stopped ------')


def check_key_value(obj, key):
    result = key in obj and obj[key] == 'Y'
    return result
   

query = f'''
    SELECT DISTINCT T1.pubmed_id
    FROM rdas_db.{publication_gard_searchterm_pubmed_mapping} AS T1
    LEFT JOIN rdas_db.{publication_article} AS T2
        ON T1.pubmed_id = T2.pubmed_id
    WHERE T2.pubmed_id IS NULL
'''

insert_sql = f'''
    INSERT INTO {publication_article} (
        pubmed_id, doi, title, abstract_text, affiliation,
        first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC, 
        in_PMC, has_PDF, pub_type, source_json)
    VALUES(%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s)
'''

base_url = os.getenv('EURO_PEPMC_SERVICE_URL')
chars_to_remove = "!@#$%^&*()_+-={}[]|\\:;\"'<>,.?/`~"

# 2. Get the Article and store into database
mysql = db().mysql_conn()
fetch_cursor = mysql.cursor()

insert_cnn = db().mysql_conn()
insert_cursor = insert_cnn.cursor()

fetch_cursor.execute(query)

batch_size = 5#30
batch_num = 0
start_time = time.time()

while True:

    batch = fetch_cursor.fetchmany(batch_size)
    
    if not batch:
        print('\n\n---------------- All copied, no more data ----------------\n\n')
        break

    batch_num += 1
    pubmed_id_list = [row[0] for row in batch]

    query = " OR ".join(f"EXT_ID:{pmid}" for pmid in pubmed_id_list)
    url = f"{base_url}?query={query}&resultType=core&format=json&pageSize=1000" 
    print(url)
    #https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:5770408&resultType=core&format=json&pageSize=1000
    
    retries = 0
    max_retries=10

    while retries < max_retries:
        try:                
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx and 5xx)                                
            try:
                bigObj = response.json()
                resultList = bigObj['resultList']['result']
                if len(resultList) <= 0:
                    break

                for result in resultList:
                    
                    if 'pmid' not in result:
                        continue
                        
                    pubmed_id  = int(result['pmid'])

                    # The query returned more other pubmed_id(s)
                    if pubmed_id not in pubmed_id_list:
                        continue

                    source = result['source'] if 'source' in result else None
                    doi = result['doi'] if 'doi' in result else None
                    title = result['title'] if 'title' in result else None
                    abstract_text = result['abstractText'] if 'abstractText' in result else None
                    affiliation = result['affiliation'] if 'affiliation' in result else None
                    first_publication_date = result['firstPublicationDate'] if 'firstPublicationDate' in result else None
                    publication_year = int(datetime.strptime(result['firstPublicationDate'], '%Y-%m-%d').year) if 'firstPublicationDate' in result else None
                    is_open_access = check_key_value(result, 'isOpenAccess')
                    in_EPMC = check_key_value(result, 'inEPMC')
                    in_PMC = check_key_value(result, 'inPMC')
                    has_PDF = check_key_value(result, 'hasPDF')
                    
                    pub_type = json.dumps(result['pubTypeList']['pubType']) if 'pubTypeList' in result else None
                    cited_by_count = int(result['citedByCount']) if 'citedByCount' in result else 0

                    #texts_to_predict = [ ((title if title is not None else '') + ' ' + (abstract_text if abstract_text is not None else '')).strip() ]
                    #is_NHS = (get_nhsExtract(texts_to_predict)) == 1
                    
                    if title:
                        title = title.strip(chars_to_remove)

                    if abstract_text:
                        abstract_text = abstract_text.strip(chars_to_remove)

                    print(f'pubmed_id = {pubmed_id}\t{publication_year}\t{doi}')

                    '''
                    pubmed_id, doi, title, abstract_text, affiliation,
                    first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC, 
                    in_PMC, has_PDF,  pub_type, source_json
                    '''
                    val = (pubmed_id, doi, title, abstract_text, affiliation, 
                        first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC,
                        in_PMC, has_PDF, pub_type, json.dumps(result)
                    ) 

                    ''' Use executemany to insert the batch data efficiently, if there are no endcoding issues need to be handled '''
                    try: 
                        insert_cursor.execute(insert_sql, val) 
                        insert_cnn.commit()
                    except Exception as e:
                        print(e)
                    
            except KeyError as e:
                print(f'KeyError: {e}\n{url}')
                print(f'\n{resultList}\n')  
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
    
    end_time = time.time()
    hours, minutes, seconds = elapsed_time(start_time, end_time)
    print(f'{Fore.BLUE+Style.BRIGHT}Batch#: {batch_num} - {hours} hours, {minutes} minutes, {seconds} seconds {Style.RESET_ALL}\n')
 
insert_cnn.commit()


# 3. Close MySQL connection
if fetch_cursor:
    fetch_cursor.close()

if insert_cursor:
    insert_cursor.close()

if mysql:
    mysql.close()

if insert_cnn:
    insert_cnn.close()


end_time = time.time()
hours, minutes, seconds = elapsed_time(start_time, end_time)
print(f'\n\n{Fore.BLUE+Style.BRIGHT}{"="*50} Completed. Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds {"="*50}{Style.RESET_ALL}\n\n') 







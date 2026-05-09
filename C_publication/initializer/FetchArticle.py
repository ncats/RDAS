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

from baseclass.conn import DBConnection as db
from utils.https_request import HTTPSUtils

#MySQL
# Retrieve Articles information by the pubmed_id by API endpoint, and store into the table publication_article
'''
SELECT CONCAT( GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ',')) 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'rdas' 
AND TABLE_NAME = 'publication_article'
'''

class ArticleFetcher:

    def __init__(self):

        self.mysql = db().mysql_conn() 
        self.mycursor = self.mysql.cursor()
        self.base_url = os.getenv('EURO_PEPMC_SERVICE_URL')
        self.chars_to_remove = "!@#$%^&*()_+-={}[]|\\:;\"'<>,.?/`~"


    def check_key_value(self, obj, key):
        result = key in obj and obj[key] == 'Y'
        return result
      

    def fetch_and_save(self, pubmed_id, source):

        val = self.fetch(pubmed_id, source)

        self.save(val)



    def save(self, val):

        insert_sql = '''
            INSERT INTO publication_article (
                pubmed_id, doi, title, abstract_text, affiliation,
                first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC, 
                in_PMC, has_PDF, pub_type, source, source_json)
            VALUES(%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s)
        '''
        #print(val)
        
        try: 
            self.mycursor.execute(insert_sql, val) 
            self.mysql.commit()
        except Exception as e:
            print(e) 
         

    
    def fetch(self, pubmed_id, source):
 
        def parse_api_response(response, source):
            try:
                bigObj = response.json()
                resultList = bigObj['resultList']['result']
                if len(resultList) <= 0:
                    return None

                for result in resultList:
                    
                    if 'pmid' not in result:
                        continue
                        
                    pmid  = result['pmid'] 
                    if str(pmid) != str(pubmed_id):
                        continue

                    result_source = result['source'] if 'source' in result else None
                    doi = result['doi'] if 'doi' in result else None
                    title = result['title'] if 'title' in result else None
                    abstract_text = result['abstractText'] if 'abstractText' in result else None
                    affiliation = result['affiliation'] if 'affiliation' in result else None
                    #print(f'\naffiliation.length = {len(affiliation)}')
                    first_publication_date = result['firstPublicationDate'] if 'firstPublicationDate' in result else None
                    publication_year = int(datetime.strptime(result['firstPublicationDate'], '%Y-%m-%d').year) if 'firstPublicationDate' in result else None
                    is_open_access = self.check_key_value(result, 'isOpenAccess')
                    in_EPMC = self.check_key_value(result, 'inEPMC')
                    in_PMC = self.check_key_value(result, 'inPMC')
                    has_PDF = self.check_key_value(result, 'hasPDF')
                    
                    pub_type = json.dumps(result['pubTypeList']['pubType']) if 'pubTypeList' in result else None
                    cited_by_count = int(result['citedByCount']) if 'citedByCount' in result else 0

                    #texts_to_predict = [ ((title if title is not None else '') + ' ' + (abstract_text if abstract_text is not None else '')).strip() ]
                    #is_NHS = (get_nhsExtract(texts_to_predict)) == 1
                
                    if title:
                        title = title.strip(self.chars_to_remove)

                    if abstract_text:
                        abstract_text = abstract_text.strip(self.chars_to_remove)

                    print(f'pubmed_id = {pubmed_id}\t{publication_year}\t{doi}')

                    '''
                    pubmed_id, doi, title, abstract_text, affiliation,
                    first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC, 
                    in_PMC, has_PDF,  pub_type, source, source_json
                    '''
                    if result_source and (source.lower() != result_source.lower()):
                        source += result_source+ ','+source

                    val = (pubmed_id, doi, title, abstract_text, affiliation, 
                        first_publication_date, publication_year, cited_by_count, is_open_access, in_EPMC,
                        in_PMC, has_PDF, pub_type, source, json.dumps(result)
                    ) 

                    return val                    
            except KeyError as e:
                print(f'KeyError: {e}\n{url}')
                print(f'\n{resultList}\n')  
            except TypeError as e:
                print(f'TypeError: {e}\n{url}')
            except AttributeError as e:
                print(f'AttributeError: {e}\n{url}')



        #url = f"{self.base_url}?query=EXT_ID:{pubmed_id}&resultType=core&format=json&pageSize=1000"      
        url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:{pubmed_id}&resultType=core&format=json&pageSize=1000"        
        #https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:5770408&resultType=core&format=json&pageSize=1000
        print(url)

        return HTTPSUtils.with_api_retry_GET(url, lambda response: parse_api_response(response, source))
    


    
 


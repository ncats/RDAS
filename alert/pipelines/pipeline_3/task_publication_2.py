import os
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterator, List, Sequence
from collections import OrderedDict
import requests
import time
import json
import re
from multiprocessing import Pool, cpu_count
from utils.https_request import HTTPSUtils as HttpsUtil
from utils.tools import ask_to_continue, elapsed_time, _to_txt

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from dotenv import load_dotenv
load_dotenv()

from pipelines.pipeline_base import PipelineBase
from utils.publication_worker import PublicationWorker

"""
Update the is_EPI, is_NHS (default is NULL) in table UPDATE_publication_article
"""
# Reference: C_publication/init_4_publication-update-EPI-NHS-of-Article-multi.py

class PublicationTask_2(PipelineBase):


    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=False)



    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("PublicationTask_2 does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        
        fetch_epi_is_null_query = f'SELECT id, pubmed_id, title, abstract_text FROM update_publication_article WHERE is_EPI is null'
        update_sql = " UPDATE update_publication_article SET is_EPI = %s, is_NHS = %s, epi_probability =%s, epi_extract = %s WHERE pubmed_id = %s "

        
        update_cursor = self.mysql.cursor()    

        fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
        fetch_cursor.execute(fetch_epi_is_null_query)

        batch_num = 0
        batch_size = 15

        try: 
            with Pool(processes=batch_size) as active_pool:
                while True:

                    rows = fetch_cursor.fetchmany(batch_size)

                    batch_num += 1
                    self.appender.log_stdout(f'\n--- batch# = {batch_num} ---')

                    if not rows:
                        self.appender.log_stdout(f"No more rows to fetch.")
                        break
 
                    obj_list = [{
                        'id': row['id'],
                        'title': row['title'],
                        'abstract_text': row['abstract_text'],
                        'pubmed_id': row['pubmed_id']
                    } for row in rows]

                    val_list = active_pool.map(self.do_work, obj_list)
                    print(val_list)
                    
                    '''
                    try:
                        update_cursor.executemany(update_sql, val_list)
                        self.mysql.commit() 
                    
                    except Exception as e:
                        self.appender.log_stdout(f"Error during update: {e}")
                        self.mysql.rollback() 
                    '''
        except Exception as e:
            self.appender.log_stdout(f"An unexpected error occurred: {e}")
            raise
        
        finally:
            if fetch_cursor:
                fetch_cursor.close() 

            # Explicitly close the all the db connections
            self.close()



    def do_work(self, obj):
    
        id = obj['id']
        pubmed_id = obj['pubmed_id']
        title = _to_txt(obj['title'])
        abstract_text = _to_txt(obj['abstract_text'])

        text_to_predict =  (title+ ' ' + abstract_text).strip()  
        
        epiPredicted = self.get_isEpi(text_to_predict)
        
        is_EPI = epiPredicted['isEpi']
        epiProbability = epiPredicted['probability']
        
        is_NHS = self.get_nhsExtract([text_to_predict])
        
        self.appender.log_stdout(f'OS.process_id:{os.getpid()}\tId:{id} - pubmed_id:{pubmed_id}\tis_EPI={is_EPI}\tepiProbability={epiProbability}\tis_NHS={is_NHS}')
                
        epiExtract = None

        if is_EPI:
            epiExtractJson = self.get_epiExtract(text_to_predict)
            if epiExtractJson:
                epiExtract = json.dumps(epiExtractJson)
                print(f'\t\t{epiExtract}') 

        return (is_EPI, is_NHS, epiProbability, epiExtract, pubmed_id) 



    def get_nhsExtract(self, texts):
   
        def parse_api_response(response):
            try: 
                nhs_info = response.json() 
                if nhs_info:
                    return nhs_info['predictions'][0]==1
                else:
                    return False
            except KeyError as e:
                self.appender.log_stdout(f'KeyError: {e}')
                self.appender.log_stdout(f'\n{response}')
            except TypeError as e:
                self.appender.log_stdout(f'TypeError: {e}')
            except AttributeError as e:
                self.appender.log_stdout(f'AttributeError: {e}')
            return False
        
        payload = {'texts': texts}

        return HttpsUtil.with_api_retry(os.getenv('NHS_PREDICT_API'), payload, parse_api_response)


    def get_isEpi(self, text):
        """
        Check if the given text corresponds to an epidemiology article using an external API.
        """ 
        def parse_api_response(response):
            try: 
                response = response.json() 
                if 'IsEpi' in response:
                    return {'isEpi': response['IsEpi'], 'probability': response['EPI_PROB']}
                    #return response['IsEpi']
                else:
                    return False 
            except TypeError as e:
                self.appender.log_stdout(f'TypeError: {e}') 
            return False


        payload = {'text': text}
        
        return HttpsUtil.with_api_retry(os.getenv('EPI_CLASSIFY_API'), payload, parse_api_response)
    


    def get_epiExtract(self, text):
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
                self.appender.log_stdout(f'Exception during get_epiExtract 1. text: {text}, error: {e}')
                return None
    
        payload = {'text': text,'extract_diseases':False}

        return HttpsUtil.with_api_retry(os.getenv('EPI_EXTRACT_API'), payload, parse_api_response)


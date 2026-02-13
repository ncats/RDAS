import os
import sys
# Add the project root to the Python path
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
    os.path.abspath(os.path.join(_dir, '../..'))
]) 

import json
import time
import hashlib
from dotenv import load_dotenv
load_dotenv() 
from utils.https_request import HTTPSUtils as HttpsUtil
from concurrent.futures import ThreadPoolExecutor
from utils.tools import ask_to_continue, elapsed_time, _to_txt, _date_string

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from baseclass.init_base import InitBase
from utils.file_appender import FileAppender 
 
'''
1. Update is_EPI, is_NHS (default is NULL) in table publication_article
2. Update EpidemiologyAnnotation in Memgraph db
''' 
class EPIAndNHSUpdater(InitBase):


    def __init__(self):

        super().__init__('publication_article', 'PublicationEPI_NHS_Updater')
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/G-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    # Override the abstract method
    def init_nodes(self):
        self.update()


    def update(self):

        batch_size = 10
        batch_num = 0
        total_article = 0
        total_epidemiology = 0

        start_time = time.time()

        select_isEpi_nll_query = 'SELECT pubmed_id FROM publication_article WHERE is_EPI is null'

        update_sql = " UPDATE publication_article SET is_EPI = %s, is_NHS = %s, epi_probability = %s, epi_extract = %s WHERE pubmed_id = %s "
            
        try:
            self.dict_cursor.execute(select_isEpi_nll_query)

            query_cursor = self.mysql.cursor(buffered=True, dictionary=True)

            '''
                Since you're doing I/O-bound work (API calls), threads work well.
                1. Your workload is I/O-bound (API calls), not CPU-bound
                2. Threads handle I/O concurrency efficiently
                3. No pickling issues
            '''
            with ThreadPoolExecutor(max_workers=batch_size) as executor:

                while True:

                    # The is_EPI = null
                    batch = self.dict_cursor.fetchmany(batch_size)

                    if not batch:
                        self.appender.log_stdout(f'{Fore.RED}\n--- All is_EPI and is_NHS are updated. ---\n{Style.RESET_ALL}')
                        break

                    batch_num += 1
                    
                    # The pubmed ID list which is_EPI = null
                    pubmed_id_list = [row['pubmed_id'] for row in batch] 
    
                    query_by_pubmed_id = f"""
                        SELECT pubmed_id, title, abstract_text, id
                        FROM publication_article
                        WHERE pubmed_id IN ({','.join(['%s']*len(pubmed_id_list))})
                        AND is_EPI is null
                    """
                    
                    query_cursor.execute(query_by_pubmed_id, pubmed_id_list)
                    results = query_cursor.fetchall()

                    # The publication info list which is_EPI = null
                    publication_list = [{
                        'id': row['id'],
                        'title': row['title'],
                        'abstract_text': row['abstract_text'],
                        'pubmed_id': row['pubmed_id']
                    } for row in results]
                    
                    api_query_results = list(executor.map(self.thread_work, publication_list))
                     
                    # update database                     
                    try:
                        # MySQL
                        self.update_cursor.executemany(update_sql, api_query_results)
                        self.mysql.commit() 

                        # Memgraph
                        num_article, num_epidemiology = self.update_memgraph_article_node(api_query_results)

                        total_article += num_article
                        total_epidemiology += num_epidemiology                        
                        self.appender.log_stdout(f'{Fore.BLUE}Batch# = {batch_num}, total_article = {total_article}, total_epidemiology = {total_epidemiology}{Style.RESET_ALL} \n')

                    except Exception as e:
                        self.appender.log_stdout(f"Error during update: {e}")
                        self.mysql.rollback()
                                 
        except Exception as e:
            self.appender.log_stdout(f"An unexpected error occurred: {e}")
            sys.exit('------Stopped ------')

        end_time = time.time()
        hours, minutes, seconds = elapsed_time(start_time, end_time)
        self.appender.log_stdout(f'\n\n{Fore.BLUE+Style.BRIGHT}{"="*50} Completed. Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds {"="*50}{Style.RESET_ALL}\n\n')


    def thread_work(self, obj):
    
        id = obj['id']
        pubmed_id = obj['pubmed_id']
        title = _to_txt(obj['title'])
        abstract_text = _to_txt(obj['abstract_text'])

        text_to_predict =  (title+ ' ' + abstract_text).strip()  
        
        # https://rdas.ncats.nih.gov/api/epi/postEpiClassifyText/
        epiPredicted = self.get_EPI_info(text_to_predict)
        
        is_EPI = epiPredicted['isEpi']
        epiProbability = epiPredicted['probability']
        
        # https://rdas.ncats.nih.gov/api/article_prediction/v1/predict
        is_NHS = self.get_NHS_extract_info([text_to_predict])
        
        self.appender.log_stdout(f'OS.process_id:{os.getpid()}\tId:{id} - pubmed_id:{pubmed_id}\tis_EPI={is_EPI}\tepiProbability={epiProbability}\tis_NHS={is_NHS}')
                
        epiExtract = None

        if is_EPI:
            # https://rdas.ncats.nih.gov/api/epi/postEpiExtractText/
            epiExtractJson = self.get_epiExtract(text_to_predict)

            if epiExtractJson:
                epiExtract = json.dumps(epiExtractJson)
                print(f'\t\t{epiExtract}') 

        return (is_EPI, is_NHS, epiProbability, epiExtract, pubmed_id) 
    


    def _1_true(self, value):
        return True if value =='1' else False

    # list of (is_EPI, is_NHS, epiProbability, epiExtract, pubmed_id) 
    def update_memgraph_article_node(self, epi_info_list):
        
        cypher_update_query = '''
            UNWIND $chunks AS chunk
            MATCH (a:Article {pubmedId: chunk.pubmed_id})
            SET 
                a.is_EPI = chunk.is_EPI, 
                a.is_NHS = chunk.is_NHS

            WITH chunk, a 
            WHERE chunk.epiObj IS NOT NULL
            
            MERGE (epi:EpidemiologyAnnotation {_composite_key: chunk.epiObj._composite_key})
            ON CREATE SET 
                epi.epidemiologyType = chunk.epiObj.epidemiologyType,
                epi.epidemiologyRate = chunk.epiObj.epidemiologyRate,
                epi.date = chunk.epiObj.date,
                epi.location = chunk.epiObj.location,
                epi.ethnicity = chunk.epiObj.ethnicity,
                epi.sex = chunk.epiObj.sex,
                epi.dateCreatedByRDAS = chunk.epiObj.dateCreatedByRDAS,
                epi.lastUpdatedByRDAS = chunk.epiObj.lastUpdatedByRDAS

            ON MATCH SET
                epi.lastUpdatedByRDAS = chunk.epiObj.lastUpdatedByRDAS
             
            MERGE (a) -[r:has_epidemiological_annotation {epidemiology_probability: chunk.epiObj.epiProbability}]-> (epi)
        '''

        chunks = [ ]

        for is_EPI, is_NHS, epiProbability, epiExtract, pubmed_id in epi_info_list:

            isEPI = self._1_true(is_EPI) if is_EPI is not None else False
            isNHS = self._1_true(is_NHS) if is_NHS is not None else False

            big_obj = {
                'is_EPI': isEPI,
                'is_NHS': isNHS         
            }

            if epiExtract:

                epiObj = json.loads(epiExtract)

                # Get all the fields
                epidemiology_type = epiObj['EPI'] or []
                epidemiology_rate = epiObj['STAT'] or []
                study_date = epiObj['DATE'] or []
                study_location = epiObj['LOC'] or []
                ethnicity = epiObj['ETHN'] or []
                sex = epiObj['SEX'] or []

                # Create composite key string from all fields
                composite_key_str = f"{'_'.join(sorted(epidemiology_type))}_{'_'.join(sorted(epidemiology_rate))}_{'_'.join(sorted(study_date))}_{'_'.join(sorted(study_location))}_{'_'.join(sorted(ethnicity))}_{'_'.join(sorted(sex))}"
                composite_key_str = "_".join(composite_key_str.split())  # Replace whitespaces

                # Hash the composite key
                composite_key_hash = hashlib.sha256(composite_key_str.encode()).hexdigest()
        
                big_obj['epiObj'] = {
                    'pubmedId': pubmed_id,

                    # realtionship property: epidemiology_probability 
                    # gqlalchemy.exceptions.GQLAlchemyDatabaseError: value of type 'decimal.Decimal' can't be used as query parameter
                    'epiProbability': str(epiProbability),

                    "epidemiologyType": epidemiology_type,
                    "epidemiologyRate": epidemiology_rate,
                    "date": study_date,
                    "location": study_location,
                    "ethnicity": ethnicity,
                    "sex": sex,
                    "_composite_key": composite_key_hash,
                    "dateCreatedByRDAS": self.formatted_today,
                    "lastUpdatedByRDAS": self.formatted_today
                }

            chunks.append(big_obj)
         
        if chunks:
            try:
                self.memgraph.execute(cypher_update_query, {"chunks": chunks})  

                how_many_epidemiology = len([ck for ck in chunks if 'epiObj' in ck])
                self.appender.log_stdout(f'Updated {len(chunks)} Articles, which {how_many_epidemiology} Articles has epidemiology annotations.')  

                return len(chunks), how_many_epidemiology
            
            except Exception as e:
                self.appender.append_and_print(f'Exception while insert: {e}')
                raise 

 

    # https://rdas.ncats.nih.gov/api/article_prediction/v1/predict
    def get_NHS_extract_info(self, texts):
 
        def parse_api_response(response):
            try:
                nhs_info = response.json()
                return nhs_info.get('predictions', [None])[0] == 1
            except (KeyError, TypeError, AttributeError, IndexError) as e:
                self.appender.log_stdout(f'{type(e).__name__}: {e}')
                return False
            
        payload = {'texts': texts}
        return HttpsUtil.with_api_retry(os.getenv('NHS_PREDICT_API'), payload, parse_api_response)
    

    # https://rdas.ncats.nih.gov/api/epi/postEpiClassifyText/
    def get_EPI_info(self, text):

        """
        Check if the given text corresponds to an epidemiology article by using an external API.
        """ 
        def parse_api_response(response):
            try: 
                data = response.json()
                if 'IsEpi' in data:
                    return {'isEpi': data['IsEpi'], 'probability': data['EPI_PROB']}
                return False
            except (TypeError, KeyError, AttributeError) as e:
                self.appender.log_stdout(f'{type(e).__name__}: {e}')
                return False

        payload = {'text': text}
        return HttpsUtil.with_api_retry(os.getenv('EPI_CLASSIFY_API'), payload, parse_api_response)
    

    # https://rdas.ncats.nih.gov/api/epi/postEpiExtractText/
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
    



if __name__ == "__main__":

    # 0.
    prompts = [
        'Did you update the .env and clean up the indexes on the memgraph database?',
        'Did you change the stage value in .env? [ DEV/TEST/PROD ]',
        'Did you commented the initializers that do not need to be processed again?'
    ]
    
    # 1.
    for prompt in prompts:
        if not ask_to_continue(f'*** {prompt} ***'):
            sys.exit('------Stopped------')

    
    # 2.
    updater = EPIAndNHSUpdater()

    updater.update()
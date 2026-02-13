import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from collections import OrderedDict
import requests
import time
import json
import re
from dotenv import load_dotenv
load_dotenv()
 
from utils.conn import DBConnection as db
from utils.tools import _is_english, _len_greater_than_threshold, ask_to_continue
from utils.applogger import AppLogger
logger = AppLogger().get_logger()

from utils.quality import exclude_words

import xml.etree.ElementTree as ET


"""
# Get pubmed ids by GARD name & synonyms, store the result into {publication_gard_pubmed} table

# Query API https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi
# 
# 
""" 

'''
    SELECT CONCAT('\'',  GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR '\',\''), '\'') 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'rdas' 
    AND TABLE_NAME = '{publication_gard_pubmed}';
'''

publication_gard_pubmed = 'publication_gard_pubmed'


class PublicationInitializer:

    def __init__(self):

        self.memgraph = db().memgraph_conn()
        self.mysql = db().mysql_conn()
        self.mycursor = self.mysql.cursor()
        self.api_key = os.getenv("NCBI_KEY")
        self.mindate = '1970'
        self.maxdate = '2025'
        self.insert_sql = f'''
            INSERT INTO {publication_gard_pubmed}
            (gard_id,year_range,search_term,total,retmax,retstart,pubmed_ids,query_translation,phrases_ignored,quoted_phrases_not_found, ref_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''


    def retrieve_pubmed_ids(self, idx, gard_id, search_term,  mindate=None, maxdate=None):
                 
        count = None 
        if not mindate:
            mindate = self.mindate

        if not maxdate:
            maxdate = self.maxdate

        search_term = search_term.lower() # lower case in database
 
        #x# Exact match: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ESearch_
        search_term_normalized = re.sub(r'\s+', '+', search_term) # replace whitespace with + sign
        term_search_query = f'"{search_term_normalized}"[Title/Abstract:~0]'
        #x#

        retries = 0
        max_retries=10
    
        url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term={term_search_query}&mindate={mindate}&maxdate={maxdate}&retmode=json&retmax=10000&api_key={self.api_key}" 
        #print(url)
        #x#https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term="momo+syndrome"[Title/Abstract:~0]&mindate=1970&maxdate=2025&retmode=json&retmax=10000&api_key=83921ee6740b5b55962599605076c1427807
        
        while retries < max_retries:
            try:                
                response = requests.get(url)
                response.raise_for_status()  # Raise an exception for HTTP errors (4xx and 5xx)                                
                try:
                    obj = response.json()
                    result = obj['esearchresult']
                    
                    count = result['count']
                    retmax = result['retmax']
                    retstart = result['retstart']
                    pubmed_ids = result['idlist']
                    querytranslation = result['querytranslation']
                    phrasesignored = result['warninglist']['phrasesignored']
                    quotedphrasesnotfound = result['warninglist']['quotedphrasesnotfound'] 

                    print(f'{idx} - {gard_id}\tCount: {count}\t{search_term}')

                except KeyError as e:
                    print(f"KeyError: {e} - The required key does not exist in the JSON structure.")
                    print(f'{url}')
                    print(f'\n{result}\n')  
                except (TypeError, AttributeError):
                    print("The JSON structure is not as expected or 'response' might not be JSON.")
                    print(f'{url}')
                
                break  # Exit the loop if successful
            except requests.exceptions.Timeout:
                retries += 1
                time.sleep(1)
            except requests.exceptions.RequestException as e:
                break  # Exit the loop for non-retryable errors
         
        """
        {
            "header": { "type": "esearch",  "version": "0.3" },
            "esearchresult": {
                "count": "10",
                "retmax": "10",
                "retstart": "0",
                "idlist": [
                    "39634243",
                    "31075093",
                    "23034868",
                    "22821547",
                    "23074680",
                    "19996736",
                    "19048502",
                    "18230893",
                    "11045586",
                    "8322820"
                ],
                "translationset": [],
                "querytranslation": "\"momo syndrome\"[Title/Abstract:~0] AND 1970/01/01:2025/12/31[Date - Entry]",
                "warninglist": {
                "phrasesignored": [],
                "quotedphrasesnotfound": [],
                "outputmessages": [ "Restrictions achieved. start and count adjusted to 0, 9999" ]
                }
            }
        }
        """
        if count is not None:
            try:
                #(gard_id,search_term,total,retmax,retstart,pubmed_ids,query_translation,phrases_ignored,quoted_phrases_not_found)
                val = (gard_id, f'{mindate}-{maxdate}', search_term, count, retmax, retstart, ",".join(pubmed_ids), querytranslation, ",".join(phrasesignored), ",".join(quotedphrasesnotfound), json.dumps(obj)) 
                print(f'{gard_id} {mindate}-{maxdate} {count} {search_term}')

                self.mycursor.execute(self.insert_sql, val)

            except Exception as e:
                print(f'{val}\n{e}')
        

    def filter_synonyms(self, syns): 
        
        syn_list =[syn for syn in syns if syn not in exclude_words]
        syns_eng = [syn for syn in syn_list if _is_english(syn)]
        filtered_syns = [syn for syn in syns_eng if _len_greater_than_threshold(syn, 4)]  

        return filtered_syns
    

    def get_GARD_disease(self):

        query = '''
            MATCH (n: GARD) 
            return 
            n.GardId as GardId, 
            n.Name as GardName, 
            COALESCE(n.Synonyms, '') as Synonyms, 
            COALESCE(n.Orpha_DisorderType, '') as DisorderType,
            COALESCE(n.Orpha_ClassificationLevel, '') as ClassificationLevel
        '''
        # Note: to expect the raw_data to be json, you need to specify the property names and with 'as'. 
        # (instead of just return 'n' only)
        results = self.memgraph.execute_and_fetch(query)

        the_data = {}

        # Process each result and populate the dictionary
        for disease in results:
            gard_id = disease["GardId"]
            the_data[gard_id] = disease

        # Return the ordered dictionary sorted based on GARD IDs
        return OrderedDict(sorted(the_data.items()))
 

    def do_init(self):

        #1.
        disease_dict = self.get_GARD_disease()

        #2. Iterate over GARD diseases
        for idx, gard_id in enumerate(disease_dict):

            disease = disease_dict[gard_id]

            searchterms = self.filter_synonyms(disease['Synonyms'])
            searchterms.extend([disease['GardName']])

            for searchterm in searchterms:     
                self.retrieve_pubmed_ids(idx, gard_id, searchterm) 
 
            self.mysql.commit()

        #3. 
        if self.mycursor:
            self.mycursor.close()
        if self.mysql:
            self.mysql.close()



    def find_articles_pubmed_ids_more_than_9999_with_splitted_years(self):

        query = f"SELECT gard_id, search_term, total, retmax, retstart FROM rdas_db.{publication_gard_pubmed} where year_range = 'ignore' "

        years = list(range(1970, 2026))  # 2026 because range() is exclusive at the end
  
        self.mycursor.execute(query)
        rows = self.mycursor.fetchall()

        idx =0
        for row in rows:
            idx += 1
            gard_id = row[0]
            searchterm = row[1]
            total = row[2]
            retmax = row[3]
            retstart = row[4]
 
            for i in range(len(years) - 1):
                #print(f'{years[i]}, {years[i+1]}')
                mindate = years[i]
                maxdate = years[i+1]

                self.retrieve_pubmed_ids(idx, gard_id, searchterm, mindate, maxdate)

            self.mysql.commit()


if __name__ == '__main__':

    ok = ask_to_continue(f'Query the API, insert pubmed_id list into {publication_gard_pubmed}?')
    if not ok:
        sys.exit('------Stopped ------')

    initlzr = PublicationInitializer()

    # Step 1.  Query the api, insert pubmed_id list into {publication_gard_pubmed}
    # Default year_range = '1970-2025'
    # initlzr.do_init()
    #

    print(f'\n\n--------------------- Manaully create indexes for next steps ------------------------------------------\n')
    print(f'CREATE INDEX idx_year_range ON {publication_gard_pubmed} (year_range)')
    print(f'CREATE INDEX idx_total ON {publication_gard_pubmed} (total)')
    print(f'CREATE INDEX idx_retmax ON {publication_gard_pubmed} (retmax)')
    print(f'\n----------------------------------------------------------------------------------------------\n\n')
    # Step 2. After step 1
    # 2.1 SELECT * FROM rdas_db.{publication_gard_pubmed} where total>(retmax+retstart);

    # In the year_range = '1970-2025', if the total > retmax
    # 2.2 update rdas_db.{publication_gard_pubmed} set year_range = 'ignore' where total>retmax; 

    # Step 3. 
    # In the year_range = '1970-2025',if the total > retmax, set the years list from 1970 t0 2025, increase by 1, re-run
    print(f'\n\n**********************************************************************************************\n')

    print('Continue run manaully:')
    print(f'SELECT * FROM rdas_db.{publication_gard_pubmed} where total>(retmax+retstart);')
    print(f"update rdas_db.{publication_gard_pubmed} set year_range = 'ignore' where total>retmax;")
    print('Uncomment and run the below method: initlzr.find_articles_pubmed_ids_more_than_9999_with_splitted_years()')

    #initlzr.find_articles_pubmed_ids_more_than_9999_with_splitted_years()

    print(f'\n\n**********************************************************************************************\n')
    
    # check the running results afterward
    # select *  from rdas_db.{publication_gard_pubmed} where year_range !='ignore' and year_range != '1970-2025';
    # select *  from rdas_db.{publication_gard_pubmed} where year_range !='ignore' and year_range != '1970-2025' and total > retmax;
 
    print('Manually find and remove low-quality search_terms/Synonyms:')
    print(f'select distinct search_term from  rdas_db.{publication_gard_pubmed} order by length(search_term) ASC, search_term limit 500;')
    print(f'\n\n**********************************************************************************************\n')
    
    # NEXT Step: ***
    # Generate unique pubmed_id list as file, and gard_id - pubmd_id mapping
    # select *  from rdas_db.{publication_gard_pubmed} where year_range !='ignore' and length(search_term)>4 and pubmed_ids is not null

    # NEXT Step
    #https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=9672646&format=json&resultType=core
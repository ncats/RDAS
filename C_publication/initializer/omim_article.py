import os
import sys
import json
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.base import BaseClass
from utils.minmaxid import MinMaxIdLoader
from .FetchArticle import ArticleFetcher
from utils.tools import ask_to_continue, _id_range_generator


#
#    1. Run OMIMArticleInitializer before insert Article nodes into Memgraph
#

'''
    Fetch and add the article information by pubmed_id which contains in publication_omim but not in publication_article, into table publication_article
'''
publication_article = 'publication_article'
publication_omim = 'publication_omim'


class OMIMArticleInitializer(BaseClass):

    def __init__(self):
        pass
 
    
    def get_unique_pubmed_id_from_publication_article(self):

        query = f'SELECT DISTINCT pubmed_id FROM {publication_article} ORDER BY pubmed_id'
        try:
            with self.dict_cursor as cursor:
                cursor.execute(query)
                return [row['pubmed_id'] for row in cursor.fetchall()]
        except Exception as e:
            print(e)
            return []


    def get_omim_to_pubmed_id_mapping(self):
 
        min_id, max_id = MinMaxIdLoader().get_min_max_ids(publication_omim)
        id_ranges = _id_range_generator(min_id, max_id, step=3, batch_size=200)

        #pubmed_id_set = set()
        omim_pubmedids_dict = {}

        for start_id, end_id in id_ranges:
            query = f'''
                SELECT  id, omim_id, entry_json FROM  {publication_omim}
                WHERE id BETWEEN {start_id} AND {end_id} ORDER BY id
            '''
            try:
                self.dict_cursor.execute(query)
                rows = self.dict_cursor.fetchall()
                print(f'Id range [{start_id} - {end_id}]') 

                for row in rows: 
                    #id = row['id']
                    omim_id = row['omim_id']
                    entry_json = row['entry_json']
                    #print(f'id = {id}, omim_id = {omim_id}')
                    
                    omimObj = json.loads(entry_json)
                    if omimObj and omimObj['omim'].get('entryList'):
                        entry_list = omimObj['omim']['entryList']

                        if len(entry_list) > 0:
                            entry = entry_list[0]['entry']
                            
                            if entry.get('referenceList'):

                                reference_list = entry['referenceList']

                                pubmed_id_list = []
                                for item in reference_list:
                                    if item['reference'].get('pubmedID'):
                                        #pubmed_id_set.add(item['reference']['pubmedID'])
                                        pubmed_id_list.append(item['reference']['pubmedID'])

                                omim_pubmedids_dict[omim_id] = pubmed_id_list

            except Exception as e:
                print(e)

        if self.dict_cursor:
            self.dict_cursor.close()
         
        return omim_pubmedids_dict
 


    # 1. Add omim_id -> pubmed_id to table publication_omim_pubmed_mapping
    # Later usage: join the tables: publication_gard_omim and publication_omim_pubmed_mapping
    def add_omim_pubmed_mappings_to_db(self):

        insert_sql = 'INSERT INTO publication_omim_pubmed_mapping (omim_id, pubmed_id) VALUES(%s, %s)'
        omim_pubmedids_dict = self.get_omim_to_pubmed_id_mapping()

        count = 0
        val_list = []       

        for omim_id, pubmed_id_list in omim_pubmedids_dict.items():
            for pmid in pubmed_id_list:
                count += 1
                val_list.append((omim_id, pmid))

            if count%500 == 0:
                self.update_cursor.executemany(insert_sql, val_list)
                self.mysql.commit()
                val_list = []   

        self.update_cursor.executemany(insert_sql, val_list)
        self.mysql.commit()

        if self.update_cursor:
            self.update_cursor.close()

        print('\n--------------------- add_omim_pubmed_mappings_to_db done ---------------------------------------')



    # 2. Fetch and add the article information by pubmed_id which contains in publication_omim but not in publication_article, into table publication_article
    def add_omim_articles(self):

        # 1. The pubmed_id in publication_omim
        omim_pubmed_id_set = set()

        omim_pubmedids_dict = self.get_omim_to_pubmed_id_mapping()

        for omim_id, pubmed_id_list in omim_pubmedids_dict.items():
            omim_pubmed_id_set.update(pubmed_id_list)

        print(f'omim_pubmed_id_set.size = {len(omim_pubmed_id_set)}')

        # 2. The pubmed_id already in publication_article
        article_pubmed_id_list = self.get_unique_pubmed_id_from_publication_article()
        print(f'Database: pubmed_id_list.size = {len(article_pubmed_id_list)}')

        # 3.
        print('Find pubmed_id which are in omim_pubmed_id_set but NOT in article_pubmed_id_list')
        # Convert the large list to a set once
        article_pubmed_id_set = set(article_pubmed_id_list)

        # Use set difference to find items not in article_pubmed_id_set
        not_in_article_list = list(omim_pubmed_id_set - article_pubmed_id_set)

        print(f'OMIM pubmed_id not_in_article_list.size = {len(not_in_article_list)}')

        # 4. Fetch the article information by pubmed_id which contains in publication_omim but not in publication_article
        fetcher = ArticleFetcher()
        
        for pmid in not_in_article_list: 
            fetcher.fetch_and_save(pmid, 'OMIM')
            print(f'added pubmed_id = {pmid}')
        
        print('\n--------------------- add_omim_articles done ---------------------------------------')



if __name__ == '__main__':

    ok = ask_to_continue('Find pubmed_id which are in OMIM but NOT in Article, fetch by pubmed_id and save to publication_article table?')
    if not ok:
        sys.exit('------Stopped ------')

    initlzr = OMIMArticleInitializer()

    # 1. Add omim_id -> pubmed_id to table publication_omim_pubmed_mapping
    # Later usage: join the tables: publication_gard_omim and publication_omim_pubmed_mapping
    #initlzr.add_omim_pubmed_mappings_to_db()

    # 2. Fetch and add the article information by pubmed_id which contains in publication_omim but not in publication_article, into table publication_article
    initlzr.add_omim_articles() 
    

    print('\n--------------------- All Done ---------------------------------------')


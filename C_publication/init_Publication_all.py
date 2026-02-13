import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from dotenv import load_dotenv
load_dotenv()

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init() 
from utils.conn import DBConnection as db 
from utils.tools import _id_range_generator, ask_to_continue

from initializer.omim_article import OMIMArticleInitializer
from initializer.article import ArticleInitializer
from initializer.omimref import OMIMRefInitializer
from initializer.pubtator import PubtatorInitializer
from initializer.epidemiology import EpidemiologyAnnotationInitializer
from initializer.relationship_GARD import GARDToArticleRelationshipInitializer
from initializer.mesh_term import MeshTermInitializer
from initializer.journal import JournalInitializer
from initializer.keyword import KeywordInitializer
from initializer.article_attrs import ArticleExtraAttributesInitializer 
from initializer.substance import SubstanceInitializer

# NOT used, see x-do-not-delete-substance_and_merge.py
#from initializer.substance_and_merge import SubstanceInitializer2

from initializer.relationship_clinical_trial import ClinicalTrialToPublicationRelationshipInitializer 

"""
    SELECT CONCAT('\'',  GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR '\',\''), '\'') 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'rdas_db' 
    AND TABLE_NAME = 'publication_article';
"""
"""
    SELECT CONCAT( GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ',')) 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'rdas_db' 
    AND TABLE_NAME = 'publication_article';
"""

class PublicationDatabaseInitializer:

    def __init__(self, stage = 'dev'):
        self.stage = stage
        pass


    def first_step_run_omim_article_initlzr(self):

        print(f'\n### This process already done, do not need to run it again ###\n\n{Fore.RED}Input \'n\' for the following prompt{Style.RESET_ALL}\n')
        ok = ask_to_continue('Find pubmed_id which are in OMIM but NOT in Article, fetch by pubmed_id and save to publication_article table?')

        if ok: 
            initlzr = OMIMArticleInitializer()

            initlzr.add_omim_pubmed_mappings_to_db()
            initlzr.add_omim_articles() 
        else:
            print('------Skip the OMIMArticleInitializer ------')


  
    def init_all_nodes(self):

        # List of initializer classes in execution order         
        initializers = [ 
            ArticleInitializer,
            ArticleExtraAttributesInitializer,
            EpidemiologyAnnotationInitializer,
            KeywordInitializer,
            JournalInitializer,
            MeshTermInitializer,
            GARDToArticleRelationshipInitializer,
            PubtatorInitializer,     
            SubstanceInitializer,

            # Do this first: 3_publication/initializer/omim_article.py before doing the OMIMRefInitializer
            OMIMRefInitializer
        ]
 
        # Execute all initializers
        for index, InitializerClass in enumerate(initializers):
            
            initializer = InitializerClass()

            # check whether the initializer was executed or not
            processed_flag = initializer.get_current_processed_flag()

            print(f'\n------ {Fore.GREEN}Current processed_flag: {processed_flag}{Style.RESET_ALL} ------\n')
            if processed_flag:
                idx = int(processed_flag[0:1])

                if idx > index and self.stage in processed_flag:
                    # Already processed
                    continue
                
            initializer.processed_flag = str(index)+ f"_{self.stage}_"+ "".join(initializer.label_name.split()) 

            initializer.init_nodes ()
 
  

if __name__ == '__main__': 
    
    print('\nPublicationDatabaseInitializer\n')

    ok = ask_to_continue(f'*** Did you update the .env and clean up the indexes on the memgrap database? *** ')

    if not ok:
        sys.exit(Fore.RED + '\n------------------------ Stopped ------------------------\n'+ Style.RESET_ALL)


    initlzer = PublicationDatabaseInitializer()

    #min_id, max_id = initlzer.min_max_id() 
    #initlzer.first_step_run_omim_article_initlzr()

    initlzer.init_all_nodes()
 
    print(f'\n{Fore.BLUE+Style.BRIGHT}{"="*50}  All Done {"="*50}{Style.RESET_ALL}\n\n')  
    
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

from initializer.project import ProjectInitializer 
from initializer.gard_project_relation import GardProjectReleationInitializer
from initializer.core_project import CoreProjectInitializer
from initializer.patent import PatentInitializer
from initializer.agent  import AgentInitializer
from initializer.annotation import AnnotationInitializer
from initializer.core_project_article_relation import CoreProjectToArticleRelationInitializer
from initializer.core_project_clinical_trail_relation import CoreProjectClinicalTrialRelationInitializer
from initializer.investigator import PrincipalInvestigatorInitializer


class GrantDatabaseInitializer:

    def __init__(self, stage = 'dev'):
        self.stage = stage
        pass

  
    def init_all_nodes(self):
 
        ''' No need anymore '''
        #PrincipalInvestigatorInitializer

         # List of initializer classes in execution order
        initializers = [
            ProjectInitializer,
            GardProjectReleationInitializer,
            CoreProjectInitializer, 
            PatentInitializer,
            AgentInitializer,
            AnnotationInitializer,
            CoreProjectToArticleRelationInitializer,
            CoreProjectClinicalTrialRelationInitializer
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

            #initializer.populate_all_nodes()
            initializer.init_nodes()
            
            ''' for testing only '''
            # for testing only
            #min_id = 1
            #max_id = 1000
            #initializer.populate_nodes(min_id, max_id)
  
 
if __name__ == '__main__': 
    
    print('\nGrant Memgraph Database Initializers\n')
     
    ok = ask_to_continue(f'*** Did you update the .env and clean up the indexes on the memgrap database? *** ')

    if not ok:
        sys.exit(Fore.RED + '\n------------------------ Stopped ------------------------\n'+ Style.RESET_ALL)


    initlzer = GrantDatabaseInitializer() 
    initlzer.init_all_nodes()

    decoration = "".join("=**" for i in range(15)) + '='
    print(Fore.BLUE + f'{decoration} All Done {decoration}'+ Style.RESET_ALL)
    
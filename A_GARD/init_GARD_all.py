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

from utils.tools import ask_to_continue

from initializer.GARD import GARDInitializer
from initializer.xref import XrefInitializer
from initializer.gene import GeneInitializer
from initializer.phenotype import PhenotypeInitializer
from initializer.gard_relation import GARDRelationInitializer


class GARDDatabaseInitializer:
 

    def __init__(self, stage = 'dev'):
        self.stage = stage
        pass


    def init_all_nodes(self):
        '''
        ContactInitializer must be before ContactInitializer
        '''
  
        initializers = [
            GARDInitializer,
            XrefInitializer,
            GeneInitializer,
            PhenotypeInitializer,
            GARDRelationInitializer
        ]

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

            initializer.init_nodes()



if __name__ == '__main__': 
    
    print('\nRun GARD Memgraph Database Initializers\n')
     
    ok = ask_to_continue(f'*** Did you update the .env and clean up the indexes on the memgrap database? *** ')

    if not ok:
        sys.exit(Fore.RED + '\n------------------------ Stopped ------------------------\n'+ Style.RESET_ALL)


    initlzer = GARDDatabaseInitializer() 
    initlzer.init_all_nodes()

    decoration = "".join("=**" for i in range(15)) + '='
    print(Fore.BLUE + f'\n{decoration} All Done {decoration}'+ Style.RESET_ALL+'\n')
    
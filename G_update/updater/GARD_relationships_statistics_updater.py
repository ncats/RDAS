import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
    os.path.abspath(os.path.join(_dir, '../..'))
])
 
from colorama import init, Fore, Style
init()

import time
from io import StringIO
from utils.file_appender import FileAppender
from baseclass.init_base import InitBase
from utils.tools import _date_string, ask_to_continue, elapsed_time

'''
Compute the relationship counts for GARD -> article/project/trials/gene/phenotype
'''
class GARDRelationshipsStatisticsUpdater(InitBase):


    def __init__(self):
        super().__init__('', 'GARD')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/G-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        #Set GARD node to have Disease label
        ''' MATCH (n:GARD)   SET n:Disease '''
        # Add properties to Disease nodes
        """
        MATCH (n:Disease) 
        SET n += {countArticles: 0, countProjects: 0, countTrials: 0, countGenes: 0, countPhenotypes: 0}
        """
        
        self.counting_commands = [         
            # Set count of articles
            """
            MATCH (n:Disease)-[r]-(a:Article)
            WITH count(r) as counts, n
            SET n.countArticles = counts
            """,
            
            # Set count of trials
            """
            MATCH (n:Disease)-[r]-(a:ClinicalTrial)
            WITH count(r) as counts, n
            SET n.countTrials = counts
            """,
            
            # Set count of genes
            """
            MATCH (n:Disease)-[r]-(a:Gene)
            WITH count(r) as counts, n
            SET n.countGenes = counts
            """,
            
            # Set count of phenotypes
            """
            MATCH (n:Disease)-[r]-(a:Phenotype)
            WITH count(r) as counts, n
            SET n.countPhenotypes = counts
            """,
            
            # Set disease count for Genes
            """
            MATCH (n:Gene)-[r]-(d:Disease) 
            WITH count(r) as diseaseCount, n
            SET n.countDiseases = diseaseCount
            """,
            
            # Set disease count for Phenotypes
            """
            MATCH (n:Phenotype)-[r]-(d:Disease) 
            WITH count(r) as diseaseCount, n
            SET n.countDiseases = diseaseCount
            """
        ]

    
    # Override the abstract method
    def init_nodes(self):
        self.update()


    # Override
    def update(self):

        start_time = time.time()

        # Execute each command
        for i, command in enumerate(self.counting_commands, 1):

            _start = time.time()

            try:
                self.appender.log_stdout(f"Executing command {i}/{len(self.counting_commands)}...")
                self.appender.log_stdout(f'{Fore.BLUE}{command}{Style.RESET_ALL}')
                
                self.memgraph.execute(command)                
                self.appender.log_stdout(f"Command {i} completed successfully")

            except Exception as e:
                self.appender.log_stdout(f"Error in command {i}: {e}")
                raise

            _end = time.time()

            hours, minutes, seconds = elapsed_time(_start, _end)
            self.appender.log_stdout(f'Time elapsed: {hours}:{minutes}:{seconds}\n')

        end_time = time.time()
        hours, minutes, seconds = elapsed_time(start_time, end_time)
        self.appender.log_stdout(f'Total time elapsed: {hours}:{minutes}:{seconds}\n')

        self.appender.close()



if __name__ == '__main__':

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
    updater = GARDRelationshipsStatisticsUpdater() 
    updater.update()
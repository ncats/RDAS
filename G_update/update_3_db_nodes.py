import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv()

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from utils.tools import ask_to_continue

# updaters

# - Article
from G_update.updater.article_is_gene_review_updater import ArticleIsGeneReviewUpdater
from G_update.updater.article_EPI_NHS_updater_multithread import EPIAndNHSUpdater

# - Organization & Location
from G_update.updater.organization_location_step_1_finder_multiple import OrganizationLocationFinder
from G_update.updater.organization_location_step_2_updater import OrganizationLocationUpdater


def run_updaters(stage):

    updaters = [

        # GARDRelationshipsStatisticsUpdater

        ArticleIsGeneReviewUpdater,
        # EPIAndNHSUpdater

        #OrganizationLocationFinder,
        #OrganizationLocationUpdater,
       
    ]

    for updater in updaters:

        print(f'\n\n{Fore.BLUE}{"*"*50} Stage: {stage}, Updater: {updater.__name__} {"*"*50}{Style.RESET_ALL}\n\n')

        updater().update()


 
if __name__ == '__main__':

    stage = os.getenv('STAGE')

    print(f'\n\n{Fore.RED}{"*"*50} Stage: {stage} {"*"*50}{Style.RESET_ALL}\n\n')
    
    prompts = [
        'Did you update the .env and clean up the indexes on the memgraph database?',
        'Did you change the stage value in .env? [ DEV/TEST/PROD ]',
        'Did you commented the initializers that do not need to be processed again?'
    ]
    
    for prompt in prompts:
        if not ask_to_continue(f'*** {prompt} ***'):
            sys.exit('------Stopped------')
    
    # 1. Updaters
    run_updaters(stage)


      

   

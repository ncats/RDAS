import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from dotenv import load_dotenv
load_dotenv()

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from utils.tools import ask_to_continue

from A_GARD.initializer.GARD import GARDInitializer
from A_GARD.initializer.gene import GeneInitializer
from A_GARD.initializer.phenotype import PhenotypeInitializer
from A_GARD.initializer.xref import XrefInitializer
from A_GARD.initializer.gard_relation import GARDRelationInitializer

from B_clinical_trial.initializer.clinicaltrial import ClinicalTrialInitializer
from B_clinical_trial.initializer.clinicaltrial_gard_mapping import ClinicalTrialToGARDMappingInitializer
from B_clinical_trial.initializer.study_design import StudyDesignInitializer
from B_clinical_trial.initializer.patient_data import IndividualPatientDataInitializer
from B_clinical_trial.initializer.outcome import PrimaryOutcomeInitializer
from B_clinical_trial.initializer.participant import ParticipantInitializer
from B_clinical_trial.initializer.intervention import InterventionInitializer
from B_clinical_trial.initializer.condition import ConditionInitializer
from B_clinical_trial.initializer.reference import ReferenceInitializer
from B_clinical_trial.initializer.drug import DrugInitializer
from B_clinical_trial.initializer.annotation import ClinicalTrialAnnotationInitializer
from B_clinical_trial.initializer.organization_location import OrganizationLocationInitializer

from C_publication.initializer.article import ArticleInitializer
from C_publication.initializer.omim_article import OMIMArticleInitializer
from C_publication.initializer.article import ArticleInitializer
from C_publication.initializer.omimref import OMIMRefInitializer
from C_publication.initializer.pubtator import PubtatorInitializer
from C_publication.initializer.epidemiology import EpidemiologyAnnotationInitializer
from C_publication.initializer.relationship_GARD import GARDToArticleRelationshipInitializer
from C_publication.initializer.mesh_term import MeshTermInitializer
from C_publication.initializer.journal import JournalInitializer
from C_publication.initializer.keyword import KeywordInitializer
from C_publication.initializer.article_attrs import ArticleExtraAttributesInitializer 
from C_publication.initializer.substance import SubstanceInitializer
from C_publication.initializer.x_epi_nhs_count import EpiAndNhsCountsInitializer

from D_grant.initializer.project import ProjectInitializer 
from D_grant.initializer.gard_project_relation import GardProjectReleationInitializer
from D_grant.initializer.core_project import CoreProjectInitializer
from D_grant.initializer.patent import PatentInitializer
#from D_grant.initializer.agent  import AgentInitializer
from D_grant.initializer.annotation import GrantAnnotationInitializer
from D_grant.initializer.core_project_GARD_relation import CoreProjectToGARDRelationInitializer
from D_grant.initializer.core_project_article_relation import CoreProjectToArticleRelationInitializer
from D_grant.initializer.core_project_clinical_trail_relation import CoreProjectClinicalTrialRelationInitializer
from D_grant.initializer.funding_IC import FundingIcInitializer


if __name__ == '__main__':

    stage = os.getenv('STAGE')
    print(f'\n\n{Fore.RED}{"*"*50} Stage: {stage} {"*"*50}{Style.RESET_ALL}\n\n')

    ok = ask_to_continue(f'*** Did you update the .env and clean up the indexes on the memgrap database? *** ')

    if not ok:
        sys.exit('------Stopped ------')

    ok = ask_to_continue(f'*** Did you change the stage? dev/test/prod *** ')

    if not ok:
        sys.exit('------Stopped ------? *** ') 


    base_initializers = [

        # GARD
        GARDInitializer,
        XrefInitializer,
        GeneInitializer,
        PhenotypeInitializer,
        GARDRelationInitializer,

        # Clinical Trial
        ClinicalTrialInitializer,
        ClinicalTrialToGARDMappingInitializer,
        ConditionInitializer,

        # InterventionInitializer must before the DrugInitializer
        InterventionInitializer,
        DrugInitializer,

        ParticipantInitializer,
        PrimaryOutcomeInitializer,
        StudyDesignInitializer,
        IndividualPatientDataInitializer,
        ClinicalTrialAnnotationInitializer,

        OrganizationLocationInitializer,

        # Publication
        ArticleInitializer,
        ArticleExtraAttributesInitializer,
        EpidemiologyAnnotationInitializer,
        KeywordInitializer,
        JournalInitializer,
        MeshTermInitializer,
        GARDToArticleRelationshipInitializer,
        PubtatorInitializer,     
        SubstanceInitializer,
        EpiAndNhsCountsInitializer,
        
        # Do this first: 3_publication/initializer/omim_article.py before doing the OMIMRefInitializer
        OMIMRefInitializer,

        # Grant
        ProjectInitializer,
        GardProjectReleationInitializer,
        CoreProjectInitializer, 
        PatentInitializer,
        #AgentInitializer,
        GrantAnnotationInitializer,
        CoreProjectToGARDRelationInitializer,
        CoreProjectToArticleRelationInitializer,
        CoreProjectClinicalTrialRelationInitializer,
        FundingIcInitializer
    ]
 

    # testing
    base_initializers = [  
        #EpiAndNhsCountsInitializer
        CoreProjectToGARDRelationInitializer
    ]

    for index, InitializerClass in enumerate(base_initializers):
         
         initializer = InitializerClass()

         initializer.init_nodes()



    # production
    ''' 
    # Execute all initializers
    for index, InitializerClass in enumerate(base_initializers):
        
        initializer = InitializerClass()
 
        processed_flag = initializer.get_current_processed_flag()

        print(f'\n------ {Fore.GREEN}Current processed_flag: {processed_flag}{Style.RESET_ALL} ------\n')
        
        if processed_flag:
            idx = int(processed_flag[0:1])

            if idx > index and stage in processed_flag:
                print('Already processed. Skipping...') 
                continue
            
        initializer.processed_flag = str(index)+ f"_{stage}_"+ "".join(initializer.label_name.split())  
        
        initializer.init_nodes()

    '''
 
    decoration = "".join("=**" for i in range(15)) + '='
    print(Fore.BLUE + f'\n{decoration} All Done {decoration}'+ Style.RESET_ALL+'\n')
        




      

   


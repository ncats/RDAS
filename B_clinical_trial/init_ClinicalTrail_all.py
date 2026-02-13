import os
import sys
''' Add the project root to the Python path '''
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from utils.tools import ask_to_continue

from initializer.clinicaltrial import ClinicalTrialInitializer
from initializer.clinicaltrial_gard_mapping import ClinicalTrialToGARDMappingInitializer
from initializer.study_design import StudyDesignInitializer
from initializer.patient_data import IndividualPatientDataInitializer
from initializer.outcome import PrimaryOutcomeInitializer
from initializer.participant import ParticipantInitializer
from initializer.intervention import InterventionInitializer
from initializer.condition import ConditionInitializer
from initializer.reference import ReferenceInitializer
from initializer.organization import OrganizationInitializer 
from initializer.location import LocationInitializer
from initializer.drug import DrugInitializer
from initializer.annotation import AnnotationInitializer


class ClinicalTrialDatabaseInitializer:

    def __init__(self, stage = 'dev'):
        self.stage = stage
        pass

    
    def init_all_nodes(self):
        '''
        ContactInitializer must be before ContactInitializer
        '''
        
        ''' No need anymore '''
        #InvestigatorInitializer,
        #ContactInitializer,    

        # List of initializer classes in execution order
        
        initializers = [
            ClinicalTrialInitializer,
            ClinicalTrialToGARDMappingInitializer,
            ConditionInitializer,
            InterventionInitializer,
            DrugInitializer,
            ParticipantInitializer,
            PrimaryOutcomeInitializer,
            StudyDesignInitializer,
            IndividualPatientDataInitializer,
            AnnotationInitializer,

            # OrganizationInitializer must before the LocationInitializer
            OrganizationInitializer,
            LocationInitializer
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
            
            initializer.init_nodes()


if __name__ == '__main__':

    ok = ask_to_continue(f'*** Did you update the .env and clean up the indexes on the memgrap database? *** ')

    if not ok:
        sys.exit(Fore.RED + '\n------------------------ Stopped ------------------------\n'+ Style.RESET_ALL)

    db_initlizer = ClinicalTrialDatabaseInitializer()
    
    db_initlizer.init_all_nodes()

    print(Fore.BLUE + f'\n=**=**=**=**=**=**=**=**=**=**=**=**=**=**=**= All Done  =**=**=**=**=**=**=**=**=**=**=**=**=**=**=**=\n'+ Style.RESET_ALL)
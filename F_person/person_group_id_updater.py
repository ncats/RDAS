import os
import sys
import re
import copy
import string
import pandas as pd
_dir = os.path.dirname(__file__)
sys.path.extend([
    _dir,
    os.path.abspath(os.path.join(_dir, '..'))
])

from colorama import init, Fore, Style
init()
 
from person_worker import PersonWorker
from person_disambiguation import PersonDisambiguator
from utils.tools import ask_to_continue, _curr_timestamp

'''
Process the last names whose rows still need person group-id assignment.

PersonWorker.get_last_names_for_group_id_update() reads distinct last_name values
from person_of_all_sources where group_id_processed IS NULL.
Those last names are the work queue for this updater.

After this updater assigns rdas_group_id to the new/unprocessed person rows,
PersonWorker.update_rdas_group_id_with_tuples() also writes group_id_processed =
self.processed_flag. That processed flag prevents the same rows from being picked
up again the next time this updater runs.
'''
class PersonGroupIdUpdater:


    def __init__(self):

        self.EMPTY_FIRST_NAME = 'NONE'
        self.processed_flag = '20260105'         

    
    def update(self):

        worker = PersonWorker()

        '''
        Step 1:
        Get the distinct last names that still have at least one row with
        group_id_processed IS NULL. The grouping process runs by last name so
        PersonDisambiguator can compare people with the same family name.
        '''
        last_names_list = worker.get_last_names_for_group_id_update()
        #last_names_list = ['Zai']

        if not last_names_list:  
            print(f'{Fore.RED}No last names to update.{Style.RESET_ALL}')
            return True
        

        for last_name in last_names_list:

            print(f"\n\n{'-'*100}")
            print(f'Last name: {last_name}')

            person_list = worker.fetch_person_by_last_name_for_group_id_update(last_name)

            if not person_list:
                continue

            grouped_by_letter = {}

            # If >= 5000, apply batch processes by first letter of the first_name
            if len(person_list) >= 5000:                    
                
                lowerchars = list(string.ascii_lowercase)
                # 1. Append 'NONE' to lowerchars list
                lowerchars.append(self.EMPTY_FIRST_NAME)

                for char in lowerchars:
                    if char == self.EMPTY_FIRST_NAME:
                        # If first_name is None or empty, add to grouped_by_letter["NONE"]
                        grouped_by_letter[char] = [
                            person for person in person_list 
                            if not person.get('first_name')
                        ]
                    else:
                        # Create a sub-list for each letter
                        grouped_by_letter[char] = [
                            person for person in person_list 
                            if person.get('first_name') and person['first_name'][0].lower() == char
                        ]
            else:
                grouped_by_letter ={'A-Z': person_list} 


            for char, sublist in grouped_by_letter.items():
                    print(f'\n- The first letter of first_name = {Fore.BLUE}{char}{Style.RESET_ALL}')

                    if not sublist or len(sublist) <= 0:
                        continue

                    #1. only need two columns id & final
                    tuples = self.disambiguate(last_name, sublist)

                    worker.update_rdas_group_id_with_tuples(tuples) 

        

            print(f'{Fore.GREEN}# Processed and updated the processed flag:: last_name: {last_name}, processed_flag: {self.processed_flag} #{Style.RESET_ALL}')

        worker.close_conn()


    def disambiguate(self, last_name, person_list):

        disambiguator = PersonDisambiguator(last_name, person_list)

        # 1.
        df = disambiguator.process() 

        #1. only need 3 columns: id, rdas_group_id, final
        df_subset = df[['id', 'rdas_group_id', 'final']]

        
        # 2. Convert DataFrame to list of dictionaries
        list_of_dicts = df_subset.to_dict('records')
         
        ''' Don't use shallow copy !!! '''
        #temp_list = list_of_dicts.copy()

        # 3. Deep copy
        temp_list = copy.deepcopy(list_of_dicts)

        # 4.
        ''' 
            1. Keep the existing rdas_group_id of person, for consistancy in MySQL & Memgraph
            2. Replace the 'final' values with the existing rdas_group_id
        '''
        for dict in list_of_dicts:

            new_rdas_group_id = dict['final'] # new rdas_group_id
            existing_rdas_group_id = dict['rdas_group_id'] # existing rdas_group_id            

            if existing_rdas_group_id and new_rdas_group_id:

                for item in temp_list:
                    # change ALL the 'final' values in the list to the existing 'rdas_group_id'
                    if item['final'] and item['final'] == new_rdas_group_id:
                        item['final'] = existing_rdas_group_id

            elif existing_rdas_group_id and not new_rdas_group_id:

                for item in temp_list:
                    # Assign the existing 'rdas_group_id' to the empty 'final'(new rdas_group_id)
                    if item['rdas_group_id'] == existing_rdas_group_id and not item['final']:
                        item['final'] = existing_rdas_group_id
      
        # 5. Convert list of dictionaries back to DataFrame
        df_result = pd.DataFrame(temp_list)

        # 6. Add a extra column 'processed_flag' 
        df_result['processed_flag'] = self.processed_flag

        # 7. Set missing final values to last_name+ _curr_timestamp + _<index>
        last_name = re.sub(r'\W+', '', last_name)

        # Add _curr_timestamp to avoid generating duplicate rdas_group_id values, which could otherwise conflict with existing rdas_group_ids
        df_result.loc[df_result['final'].isna(), 'final'] = (last_name+ '_'+ _curr_timestamp()+'_' + df_result.index[df['final'].isna()].astype(str))

        # 8 Filter out the rows which have existing rdas_group_id (the rows which alread processed and stored in database)
        # Only new person kept
        df_result = df_result[df_result['rdas_group_id'].isna()]

        # 9. Reorder columns to desired tuple order for SQL script parameters
        df_result = df_result[['final', 'processed_flag', 'id']]
 
        # 11. convert to tuples for MySQL update
        tuples = list(df_result.itertuples(index=False, name=None))

        return tuples
    


if __name__ == '__main__':

    ok = ask_to_continue(f'\n{Fore.BLUE}Update the person groups by last name?{Style.RESET_ALL} ')

    if not ok:
        sys.exit(f'{Fore.RED}------Stopped ------{Style.RESET_ALL}') 

    updater = PersonGroupIdUpdater()
    updater.update()

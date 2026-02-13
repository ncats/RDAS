import os
import sys
import string
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
from colorama import init, Fore, Style
init()

from baseclass.conn import DBConnection as db
from person_worker import PersonWorker
from person_disambiguation import PersonDisambiguator
from utils.tools import ask_to_continue


def process_person_into_groups():

    """ Set up the processed_flag  """
    processed_flag = '20260107'
    
   # Lowercase a-z
    lowercase = list(string.ascii_lowercase)
    # ['a', 'b', 'c', ..., 'x', 'y', 'z']

    # Uppercase A-Z
    UPPERCASE = list(string.ascii_uppercase)
    # ['A', 'B', 'C', ..., 'X', 'Y', 'Z']

    UPPERCASE.append("-")# 1. hyphen
    #LIKE '_b%' has a leading wildcard (_), which prevents MySQL from using the index efficiently
    #MySQL has to scan every row to check the 2nd character
    #UPPERCASE.append("_")# 2. underscore
    UPPERCASE.append("'")# 3. single quote
    UPPERCASE.append("/")# 4. forward slash 
    
    # some last name may also starts with lower case characters
    UPPERCASE.extend(lowercase)

    # append extra characters to lowercase
    lowercase.append(' ')# 1. empty space
    lowercase.append("'")# 2. single quote
    lowercase.append(".")# 3. dot
    lowercase.append("-")# 4. hyphen
    lowercase.append("_")# 5. underscore   
 
    EMPTY_FIRST_NAME = 'NONE'

    # init
    worker = PersonWorker()

    for upp in UPPERCASE:

        for low in lowercase:

            prefix = upp + low 

            print(f'\n\n{Fore.BLUE}{"="*57} Last name starts with: {prefix} {"="*57}{Style.RESET_ALL}')

            last_names_list = worker.get_last_names_by_prefix_for_group_id_update(prefix, processed_flag)

            if not last_names_list:               
                continue

            for last_name in last_names_list:

                print(f"\n\n{'-'*100}")
                print(f'Last name: {last_name}')

                person_list = worker.fetch_person_by_last_name_for_group_id_update(last_name, processed_flag)

                if not person_list:
                    continue
                '''
                for person in person_list:
                    print(person)
                '''
                grouped_by_letter = {}

                # If >= 5000, apply batch process
                if len(person_list) >= 5000:                    
                   
                    lowerchars = list(string.ascii_lowercase)
                    # 1. Append 'NONE' to lowerchars list
                    lowerchars.append(EMPTY_FIRST_NAME)

                    for char in lowerchars:
                        if char == EMPTY_FIRST_NAME:
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
                        df_subset = df_subset = disambiguate(last_name, sublist)

                        worker.update_rdas_group_id(df_subset, last_name, processed_flag) 

                print(f'{Fore.GREEN}# Processed and updated the processed flag:: last_name: {last_name}, processed_flag: {processed_flag} #{Style.RESET_ALL}')
 
    worker.close_conn()


def disambiguate(last_name, person_list):

    disambiguator = PersonDisambiguator(last_name, person_list)

    df = disambiguator.process() 

    #1. only need two columns id & final
    df_subset = df[['id', 'final']]

    return df_subset




if __name__ == '__main__':

    ok = ask_to_continue(f'\n{Fore.BLUE}Generate person groups by last name?{Style.RESET_ALL} ')

    if not ok:
        sys.exit(f'{Fore.RED}------Stopped ------{Style.RESET_ALL}') 

    process_person_into_groups()
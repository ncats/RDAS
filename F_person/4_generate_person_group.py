import os
import sys
import re
import copy
import string
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
from colorama import init, Fore, Style
init()

from person_worker import PersonWorker
from person_disambiguation import PersonDisambiguator
from utils.tools import ask_to_continue, _curr_timestamp


'''
Generate rdas_group_id values for people in person_of_all_sources.

This script is the bulk/person-group generation workflow. It scans last names by
prefix, fetches all people for each selected last name, runs PersonDisambiguator,
and writes rdas_group_id back to MySQL.

Important behavior:
    1. Existing rdas_group_id values are preserved.
    2. New rows that match an existing person group reuse the existing rdas_group_id.
    3. Only rows with missing rdas_group_id are updated.
    4. group_id_processed is set to processed_flag so future runs know these rows
       have already been handled by this grouping pass.
'''
def process_person_into_groups():

    '''
    Set up the processed flag for this grouping run.

    This value is written to person_of_all_sources.group_id_processed for rows
    updated by this script. PersonWorker uses this value when selecting rows, so
    the script can skip rows that were already processed by the same run/version.
    '''
    processed_flag = '20260107'
    
    '''
    Build the two-character prefix search list.

    The script does not fetch every last name in one query. Instead, it searches
    by prefixes such as "Za", "Zh", "O'", etc. This keeps each query smaller and
    lets MySQL use last_name LIKE 'prefix%' patterns instead of broad scans.
    '''

    # Lowercase a-z
    lowercase = list(string.ascii_lowercase)
    # ['a', 'b', 'c', ..., 'x', 'y', 'z']

    # Uppercase A-Z
    UPPERCASE = list(string.ascii_uppercase)
    # ['A', 'B', 'C', ..., 'X', 'Y', 'Z']

    UPPERCASE.append("-")# 1. hyphen
    '''
    Do not include "_" as the first prefix character.

    In SQL LIKE, "_" is a wildcard for one character. A pattern such as "_b%"
    would force MySQL to inspect many more rows because it cannot use a normal
    prefix lookup the same way it can for "Ab%" or "Zh%".
    '''
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

    '''
    EMPTY_FIRST_NAME is used later for very large last-name groups.
    People with no first_name are put into this separate bucket so they are not
    lost when the group is split by first-name initial.
    '''
    worker = PersonWorker()

    for upp in UPPERCASE:

        for low in lowercase:

            '''
            Step 1:
            Build one two-character last-name prefix and find last names under
            that prefix that still need group-id processing.

            Example:
                prefix = "Zh"
                SQL condition becomes last_name LIKE 'Zh%'
            '''
            prefix = upp + low 

            print(f'\n\n{Fore.BLUE}{"="*57} Last name starts with: {prefix} {"="*57}{Style.RESET_ALL}')

            '''
            Step 2:
            Fetch distinct last names for this prefix where group_id_processed
            is NULL or does not match processed_flag.

            This gives the script a smaller last-name work queue for the current
            prefix instead of trying to group the whole person table at once.
            '''
            last_names_list = worker.get_last_names_by_prefix_for_group_id_update(prefix, processed_flag)

            if not last_names_list:               
                continue

            for last_name in last_names_list:

                '''
                Step 3:
                For each selected last name, fetch person rows from all supported
                sources. The query includes publication, clinical trial, and grant
                project context so PersonDisambiguator has enough information to
                decide which rows are likely the same person.
                '''
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

                '''
                Step 4:
                If a last name has a very large number of rows, split the people
                by the first letter of first_name before disambiguation.

                This keeps PersonDisambiguator from processing one huge group for
                common last names. Rows with missing first_name go into the NONE
                bucket. Smaller last-name groups are processed together as A-Z.
                '''
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

                        '''
                        Step 5:
                        Run disambiguation for this last-name/first-initial group.

                        disambiguate() returns tuples in this order:
                            (rdas_group_id, group_id_processed, person_row_id)

                        The returned tuples only include rows whose rdas_group_id
                        is currently missing. Existing rdas_group_id values are
                        used as anchors but are not overwritten.
                        '''
                        tuples = disambiguate(last_name, sublist, processed_flag)

                        '''
                        Step 6:
                        Write the new rdas_group_id values and group_id_processed
                        flag back to person_of_all_sources.
                        '''
                        worker.update_rdas_group_id_with_tuples(tuples) 

                print(f'{Fore.GREEN}# Processed and updated the processed flag:: last_name: {last_name}, processed_flag: {processed_flag} #{Style.RESET_ALL}')
 
    '''
    Step 7:
    Close the MySQL connection after all prefix and last-name groups finish.
    '''
    worker.close_conn()


def disambiguate(last_name, person_list, processed_flag):

    '''
    Run PersonDisambiguator for one person group and convert the result into
    database update tuples.

    PersonDisambiguator writes a temporary grouping result into the final column.
    This function converts that temporary result into the rdas_group_id that
    should be saved in MySQL.
    '''
    disambiguator = PersonDisambiguator(last_name, person_list)

    df = disambiguator.process() 

    '''
    Step 1:
    Keep only the columns needed to decide and write group IDs.

    id:
        MySQL row id in person_of_all_sources.
    rdas_group_id:
        Existing saved group id. If present, it should be preserved.
    final:
        Temporary group id generated by PersonDisambiguator for this run.
    '''
    df_subset = df[['id', 'rdas_group_id', 'final']]

    '''
    Step 2:
    Convert the DataFrame to dictionaries so the existing rdas_group_id values
    can be propagated across matching temporary final groups.
    '''
    list_of_dicts = df_subset.to_dict('records')

    '''
    Step 3:
    Use a deep copy because resolved records are modified below.
    A shallow copy would still point to the same dictionaries and make it harder
    to compare original PersonDisambiguator output with the resolved output.
    '''
    #temp_list = list_of_dicts.copy()
    temp_list = copy.deepcopy(list_of_dicts)

    '''
    Step 4:
    Preserve existing rdas_group_id values.

    If an older row already has rdas_group_id and PersonDisambiguator puts a new
    row into the same temporary final group, the new row should reuse that
    existing rdas_group_id. This keeps MySQL and Memgraph consistent and avoids
    creating a second group id for the same person.
    '''
    for dict in list_of_dicts:

        new_rdas_group_id = dict['final'] # new rdas_group_id
        existing_rdas_group_id = dict['rdas_group_id'] # existing rdas_group_id

        if existing_rdas_group_id and new_rdas_group_id:

            for item in temp_list:
                '''
                Case 1:
                The current row already has rdas_group_id and also belongs to a
                temporary final group. Every row with that same temporary final
                group should now point to the existing rdas_group_id.
                '''
                if item['final'] and item['final'] == new_rdas_group_id:
                    item['final'] = existing_rdas_group_id

        elif existing_rdas_group_id and not new_rdas_group_id:

            for item in temp_list:
                '''
                Case 2:
                The current row already has rdas_group_id but PersonDisambiguator
                did not assign it a temporary final group. Copy the existing
                rdas_group_id into final so the resolved state is explicit.
                '''
                if item['rdas_group_id'] == existing_rdas_group_id and not item['final']:
                    item['final'] = existing_rdas_group_id

    '''
    Step 5:
    Convert resolved dictionaries back to a DataFrame for filtering and tuple
    generation.
    '''
    df_result = pd.DataFrame(temp_list)

    '''
    Step 6:
    Add processed_flag. This value becomes group_id_processed in MySQL when the
    update tuples are written.
    '''
    df_result['processed_flag'] = processed_flag

    '''
    Step 7:
    If PersonDisambiguator did not assign final and there is no existing
    rdas_group_id to reuse, create a fallback id using:

        normalized_last_name + current timestamp + row index

    The timestamp makes the fallback less likely to collide with an existing
    group id from an earlier run.
    '''
    last_name = re.sub(r'\W+', '', last_name)

    df_result.loc[df_result['final'].isna(), 'final'] = (last_name+ '_'+ _curr_timestamp()+'_' + df_result.index[df['final'].isna()].astype(str))

    '''
    Step 8:
    Do not update rows that already have rdas_group_id. Those rows are used only
    as anchors for assigning group ids to new/unprocessed rows.
    '''
    df_result = df_result[df_result['rdas_group_id'].isna()]

    '''
    Step 9:
    Reorder columns to match PersonWorker.update_rdas_group_id_with_tuples():

        UPDATE person_of_all_sources
        SET rdas_group_id = %s,
            group_id_processed = %s
        WHERE id = %s
    '''
    df_result = df_result[['final', 'processed_flag', 'id']]

    '''
    Step 10:
    Convert the DataFrame to tuples so MySQL executemany can update the rows.
    '''
    tuples = list(df_result.itertuples(index=False, name=None))

    return tuples




if __name__ == '__main__':

    ok = ask_to_continue(f'\n{Fore.BLUE}Generate person groups by last name?{Style.RESET_ALL} ')

    if not ok:
        sys.exit(f'{Fore.RED}------Stopped ------{Style.RESET_ALL}') 

    process_person_into_groups()

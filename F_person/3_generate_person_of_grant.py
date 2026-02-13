import os
import sys
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
from colorama import init, Fore, Style
init()

from baseclass.conn import DBConnection as db
from utils.minmaxid import MinMaxIdLoader
from utils.tools import ask_to_continue, _id_range_generator, _arr, _normalize_tuple, _curr_timestamp
 
#3. From Grant
''' 
    associated_id
    associated_type: PI, contact, author
    source -- ClinicalTrial, Publication, Grant
''' 
'''
    SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ', ')
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'rdas_db' AND TABLE_NAME = 'person_of_ct_and_grant';
'''
 
'''
INSERT INTO rdas_db.person_of_all_sources (associate_id, associate_type, source, title, first_name, last_name, role, affiliation, email, phone, orc_id, PI_id)
SELECT associate_id, associate_type, source, title, first_name, last_name, role, affiliation, email, phone, orc_id, PI_id
FROM rdas_db.person_of_ct_and_grant;
'''

def extract_name(s):
    if not s:
        return None, None   
    # Strip whitespace and split by commas
    parts = [part.strip() for part in s.strip().split(',')]
    
    # Handle different cases based on the number of parts
    if len(parts) == 1:
        # Only one part, treat as last name
        last_name = parts[0].lower().capitalize() if parts[0] else None
        return None, last_name
    elif len(parts) == 2:
        # Two parts, assume format: last_name, first_name
        last_name = parts[0].lower().capitalize()
        first_name = parts[1].lower().capitalize()
        return first_name, last_name
    elif len(parts) >= 3:
        # Three or more parts, assume format: last_name, suffix, first_name [middle_name]
        # Example: ROBERTS, II, L JACKSON
        last_name = parts[0].lower().capitalize()
        suffix = parts[1] if parts[1].upper() in ['JR', 'SR', 'II', 'III', 'IV', 'V'] else None
        first_name_parts = parts[2:] if suffix else parts[1:]
        # Capitalize each word in the first name
        first_name = ' '.join(part.lower().capitalize() for part in ' '.join(first_name_parts).split())
        return first_name, last_name



if __name__ == "__main__":  
 
    processed_flag = '20251220'
    grant_project_table = 'grant_project'
    person_table = 'person_of_all_sources'   
    grant_gard_project_relation_table = 'grant_gard_project_relation'
     
    ok = ask_to_continue(f'''Add Person from {Fore.GREEN}{grant_project_table}{Style.RESET_ALL} table into table {Fore.RED}{person_table}{Style.RESET_ALL} ?''')

    if not ok:
        sys.exit(f'{Fore.RED}------Stopped ------{Style.RESET_ALL}') 

    min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(grant_gard_project_relation_table, processed_flag) 
    print(f'min_id: {min_id}, max_id: {max_id}')

    step = 1
    batch_size = 100

    id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

    mysql = db().mysql_conn()
    
    insert_person_cursor = mysql.cursor(buffered=True) 
    update_flag_cursor = mysql.cursor(buffered=True) 
    fetch_cursor = mysql.cursor(buffered=True, dictionary=True) 

    insert_sql = f'''
        INSERT INTO {person_table} (associate_id, associate_type, source, first_name, last_name, role, affiliation, PI_id, location, person_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'investigator')
    '''

    #
    # The column 'associate_id' is application_id
    associate_type = 'Grant_PI'
    source = 'GrantProject' 

    _count = 0
    for start_id, end_id in id_ranges:
        #1.    
        query = f'''
                SELECT  DISTINCT  gp.application_id,  gp.ORG_NAME, gp.PI_IDS, gp.PI_NAMES, gp.ORG_CITY, gp.ORG_STATE, gp.ORG_COUNTRY, gp.ORG_ZIPCODE

                FROM  {grant_gard_project_relation_table} ggpr

                LEFT JOIN {grant_project_table} gp
                ON ggpr.application_id=gp.application_id
 
                WHERE  (ggpr.id BETWEEN {start_id} AND {end_id})               
                AND (gp.processed is null OR gp.processed != '{processed_flag}') 
                AND (ggpr.processed is null OR ggpr.processed != '{processed_flag}')            
            ''' 
        
        #print(query)

        fetch_cursor.execute(query)
        rows = fetch_cursor.fetchall()
        # '8712351',  '8000340 (contact)', 'TROBRIDGE, GRANT D (contact)'    
        # '8715876',  '11354899;10408274;11279673;1870975 (contact);9615631', 'BEGON, MICHAEL ;CHILDS, JAMES EMORY;DIGGLE, PETER ;KO, ALBERT ICKSANG (contact);REIS, MITERMAYER GALVAO DOS'

        #2.
        investigators = []
        #(associate_id, associate_type, source, first_name, last_name, role, affiliation, PI_id, location)

        for row in rows:
            
            application_id = row['application_id']
            org_name = row['ORG_NAME']

            pi_ids = _arr(row['PI_IDS'])  # Ensure empty list if None
            pi_names = _arr(row['PI_NAMES']) # Ensure empty list if None

            if not pi_ids and not pi_names:
                    continue  # Skip rows with no investigators
            
            city = row.get('ORG_CITY') if row.get('ORG_CITY') is not None else ''
            state = row.get('ORG_STATE') if row.get('ORG_STATE') is not None else ''
            country = row.get('ORG_COUNTRY') if row.get('ORG_COUNTRY') is not None else ''
            zipcode = row.get('ORG_ZIPCODE') if row.get('ORG_ZIPCODE') is not None else ''

            location = city + ', ' + state + ' ' + country + ' ' + zipcode

            if location.strip() == ',':
                location = None

            if not pi_ids:
                # Only names provided
                if len(pi_names) == 1:
                    role = 'contact'

                    #'KAUFF, JACKIE (contact)'
                    first_name, last_name = extract_name(pi_names[0].replace('(contact)', ''))

                    #(associate_id, associate_type, source, first_name, last_name, role, affiliation, PI_id, location)
                    investigators.append((application_id, associate_type, source, first_name, last_name, role, org_name, None, location))
                else:
                    # 'FARRELL, MARY (contact);KORALEK, ROBIN'
                    # 'GALLUN, FREDERICK J.;KONRAD-MARTIN, DAWN L (contact)'
                    for name in pi_names:
                        role = None
                        if '(contact)' in (name or ''): 
                            role = 'contact'
                            name = name.replace('(contact)', '') if name else name
                            
                        first_name, last_name = extract_name(name)       

                        #(associate_id, associate_type, source, first_name, last_name, role, affiliation, PI_id, location)                     
                        investigators.append((application_id, associate_type, source, first_name, last_name, role, org_name, None, location))                 
            else:
                if len(pi_ids) == 1:
                    
                    role = 'contact'
                    first_name, last_name = extract_name(pi_names[0].replace('(contact)', ''))
                    pi_id = pi_ids[0].replace('(contact)', '')

                    #(associate_id, associate_type, source, title, first_name, last_name, role, affiliation, PI_id, location)
                    investigators.append((application_id, associate_type, source, first_name, last_name, role, org_name, pi_id, location))  

                else:
                    # '1858757;6970036 (contact)', 'NORTH, CAROL S;SURIS, ALINA  (contact)'
                    for pid, name in zip(pi_ids, pi_names + [None] * (len(pi_ids) - len(pi_names))):
                        role = None

                        if '(contact)' in (pid or '') or '(contact)' in (name or ''):
                            role = 'contact'
                            pid = pid.replace('(contact)', '') if pid else pid
                            name = name.replace('(contact)', '') if name else name
                            
                        first_name, last_name = extract_name(name)
                         
                        #(associate_id, associate_type, source, first_name, last_name, role, affiliation, PI_id, location)
                        investigators.append((application_id, associate_type, source, first_name, last_name, role, org_name, pid, location))   
             
        #3. insert into database
        if investigators:

            normalized_contacts = [ _normalize_tuple(contact) for contact in investigators]
            
            insert_person_cursor.executemany(insert_sql, normalized_contacts)
            mysql.commit() 

            _count += len(investigators)
            print(f'{_curr_timestamp()} flag = {processed_flag}, Id [{start_id} - {end_id}], total = {_count}. #Investigators = {len(investigators)}') 
        

        #4. update flag
        update_flag_cursor.execute(f"UPDATE {grant_gard_project_relation_table} SET processed = '{processed_flag}' WHERE id BETWEEN {start_id} AND {end_id}")   
        mysql.commit() 


    fetch_cursor.close()
    update_flag_cursor.close()
    insert_person_cursor.close() 

    mysql.close()

    print(f'{Fore.BLUE+Style.BRIGHT}{"="*50} Done. Total = {_count} {"="*50}{Style.RESET_ALL}\n\n') 
        

    '''
    rdas-memgraph/tools/execute-the-long-running-SQL.py

    commands = [ 
        f'CREATE INDEX idx_poas_source ON rdas_db.{person_of_all_sources} (source)',
        f'CREATE INDEX idx_poas_first_name ON rdas_db.{person_of_all_sources} (first_name)',
        f'CREATE INDEX idx_poas_last_name ON rdas_db.{person_of_all_sources} (last_name)',  
        f'CREATE INDEX idx_poas_associate_id ON rdas_db.{person_of_all_sources} (associate_id)',
        f'CREATE INDEX idx_poas_affiliation ON rdas_db.{person_of_all_sources} (affiliation)',
        f'CREATE INDEX idx_poas_orcid ON rdas_db.{person_of_all_sources} (orcid)',
        f'CREATE INDEX idx_poas_processed ON rdas_db.{person_of_all_sources} (processed)'
        
        # For finding duplicates, see SQL script below
        f'CREATE INDEX idx_poas_4find_duplicate ON rdas_db.{person_of_all_sources} (associate_id, first_name, last_name)'
    ]    
    '''


    # Any duplicates? 
    # Run the SQL script below to find duplicates by 'source'
    '''
    SELECT  associate_id, first_name, last_name, COUNT(*) as duplicate_count
    FROM 
        rdas_db.person_of_all_sources
    WHERE 
        source = 'Publication'
    GROUP BY associate_id, first_name, last_name
    HAVING COUNT(*) > 1
    LIMIT 100;
    '''
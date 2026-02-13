import os
import sys
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
from colorama import init, Fore, Style
init()

from baseclass.conn import DBConnection as db
from utils.minmaxid import MinMaxIdLoader
from utils.tools import ask_to_continue, _id_range_generator, _normalize_txt, _normalize_tuple, _curr_timestamp
 
#1. From ClinicalTrial 
''' 
    associated_id: nctId
    associated_type: PI, contact, author
    source -- ClinicalTrial, Publication, Grant
'''

def extract_name_title(s):
    '''
        Sonia Caprio, M.D.
        Susan M. O'Brien, MD
        Padmasree Veeraraghavan, N.P.
    '''
   
    # Split by comma, if present
    parts = s.split(',')
    if len(parts) > 1:
        title = parts[1].strip()  # Title exists
        name_part = parts[0].strip()
    else:
        title = None  # No title
        name_part = parts[0].strip()
    
    # Split the name part by space to get first and last names
    name_parts = name_part.split()
    first_name = name_parts[0]  # First name
    last_name = name_parts[-1]  # Last name (handles middle names)
    
    return first_name, last_name, title

    
if __name__ == "__main__":  
 
    processed_flag = '20251219-person'
    person_table = 'person_of_all_sources'      
    clinical_trial_table = 'clinical_trial_unique' 
 
    ok = ask_to_continue(f'''Add Person from {Fore.GREEN}{clinical_trial_table}{Style.RESET_ALL} table into table {Fore.RED}{person_table}{Style.RESET_ALL} ?''')

    if not ok:
        sys.exit(f'{Fore.RED}------Stopped ------{Style.RESET_ALL}') 


    processed_flag = '20251222'
    min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(clinical_trial_table, processed_flag) 
    print(f'min_id: {min_id}, max_id: {max_id}')

    step = 1
    batch_size = 200
    id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

    mysql = db().mysql_conn()
    
    insert_person_cursor = mysql.cursor(buffered=True) 
    update_flag_cursor = mysql.cursor(buffered=True) 
    fetch_cursor = mysql.cursor(buffered=True, dictionary=True) 

    insert_sql = f'''
        INSERT INTO {person_table} (associate_id, associate_type, source, title, first_name, last_name, role, affiliation, email, phone, person_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    #
    # The column 'associate_id' is nctId
    source = 'ClinicalTrial' 
    associate_type_PI = 'PI'
    associate_type_contact = 'contact'
   
    _count = 0
    for start_id, end_id in id_ranges: 
        #1.    
        query = f''' SELECT nctid,studies from  {clinical_trial_table} ctu
            LEFT JOIN {person_table} p 
            ON ctu.nctid=p.associate_id AND p.source = '{source}'
            WHERE
                p.associate_id is null 
            AND ctu.id between {start_id} and {end_id}
        '''  

        query = f''' SELECT nctid,studies 
            FROM  {clinical_trial_table}
            WHERE 
            id between {start_id} and {end_id}
            AND (processed is null OR processed != '{processed_flag}')
        '''  

        # SELECT * FROM rdas_db.clinical_trial where nctid='NCT00000168';  

        fetch_cursor.execute(query)
        rows = fetch_cursor.fetchall()

        '''
        SELECT * FROM rdas_db.clinical_trial_unique 
        where 
            studies like '%sponsorCollaboratorsModule%' 
            and studies like '%investigatorAffiliation%' 
            and studies like '%contactsLocationsModule%' 
            and  studies like '%centralContacts%' 
        limit 0, 5;
        '''
        # SELECT * FROM rdas_db.clinical_trial_unique where nctid='NCT00005095'
            
        #2.
        person = []
        #(associate_id, associate_type, source, title, first_name, last_name, role, affiliation, email, phone, person_type)

        for row in rows:

            nctid = row['nctid']
            study = json.loads(row['studies']) 

            #2.1 sponsor/PI
            sponsorCollaboratorsModule = study.get('protocolSection', {}).get('sponsorCollaboratorsModule', {})
            responsibleParty = sponsorCollaboratorsModule.get('responsibleParty', None)
            '''
            {
                "sponsorCollaboratorsModule": {
                    "responsibleParty": {
                        "type": "PRINCIPAL_INVESTIGATOR",
                        "investigatorFullName": "Lee Shulman",
                        "investigatorTitle": "Professor",
                        "investigatorAffiliation": "Northwestern University"
                    },
                    "leadSponsor": {
                        "name": "Northwestern University",
                        "class": "OTHER"
                    },
                    "collaborators": [
                        {
                        "name": "National Cancer Institute (NCI)",
                        "class": "NIH"
                        }
                    ]
                    },
            }
            '''
            if responsibleParty:
                pi_name =  responsibleParty.get('investigatorFullName', None)  

                if pi_name:
                    first_name, last_name, title = extract_name_title(pi_name) 
                    role =  responsibleParty.get('type', None)
                    affiliation =  responsibleParty.get('investigatorAffiliation', None)     

                    title =  responsibleParty.get('investigatorTitle', None)
                     # SELECT  length(title) len, title FROM rdas_db.person order by length(title) desc;
                    title =  responsibleParty.get('investigatorTitle', None)
                    if title:
                        title = title.strip()
 

                    #(associate_id, associate_type, source, title, first_name, last_name, role, affiliation, email, phone, person_type)
                    person.append((nctid, associate_type_PI, source, title, first_name, last_name, role, affiliation, None, None, 'sponsor'))    
                    _count += 1        

            # contacts
            contactsLocationsModule = study.get('protocolSection', {}).get('contactsLocationsModule', {})            
            centralContacts = contactsLocationsModule.get('centralContacts',None)
            overallOfficials = contactsLocationsModule.get('overallOfficials',None)      

            #2.2
            if centralContacts:               
                for contact in centralContacts:  
                    name =  contact.get('name','') 
                    first_name, last_name, title = extract_name_title(name) 

                    role =  contact.get('role', None)         
                    email =  contact.get('email', None)
                    phone =  contact.get('phone', None) 

                    #(associate_id, associate_type, source, title, first_name, last_name, role, affiliation, email, phone, person_type)
                    person.append((nctid, associate_type_contact, source, title, first_name, last_name, role, None, email, phone, 'contact')) 
                    _count += 1

            #2.1.2
            if overallOfficials: 
                for contact in overallOfficials: 

                    name =  contact.get('name','')  
                    first_name, last_name, title = extract_name_title(name) 

                    role =  contact.get('role', None)
                    affiliation =  contact.get('affiliation', None)

                    #(associate_id, associate_type, source, title, first_name, last_name, role, affiliation, email, phone, person_type)
                    person.append((nctid, associate_type_contact, source, title, first_name, last_name, role, affiliation, None, None, 'study_chair')) 
                    _count += 1
  
        #3. insert into database
        if len(person) > 0:

            normalized_contacts = [ _normalize_tuple(contact) for contact in person]
            
            insert_person_cursor.executemany(insert_sql, normalized_contacts)
            mysql.commit() 

            print(f'{_curr_timestamp()} Id range[{start_id} - {end_id}], total count = {_count}. #Person = {len(person)}') 

        #4. update flag
        update_flag_cursor.execute(f"UPDATE {clinical_trial_table} SET processed = '{processed_flag}' WHERE id BETWEEN {start_id} AND {end_id}")   
        mysql.commit() 

    fetch_cursor.close()
    update_flag_cursor.close()
    insert_person_cursor.close() 

    mysql.close()
    print(f'{Fore.BLUE+Style.BRIGHT}{"="*50} Done! Total = {_count} {"="*50}{Style.RESET_ALL}\n\n') 
        

         
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
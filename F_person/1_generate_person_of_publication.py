import os
import sys
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
from colorama import init, Fore, Style
init()

from baseclass.conn import DBConnection as db
from utils.minmaxid import MinMaxIdLoader
from utils.tools import ask_to_continue, _id_range_generator, _normalize_txt, _normalize_tuple, _curr_timestamp
 
#3. From Publication
''' 
    associated_id: pubmed_id(for publication)
    associated_type: PI, contact, author
    source -- ClinicalTrial, Publication, GrantProject
''' 
#Generate column names separated by coma
'''
    SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ', ')
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'rdas_db' AND TABLE_NAME = 'person_of_all_sources';
'''
  
if __name__ == "__main__":  
 
    processed_flag = '20251220'
    person_table = 'person_of_all_sources'   
    publication_article_table = 'publication_article'  
 
    ok = ask_to_continue(f'''Add Person from {Fore.GREEN}{publication_article_table}{Style.RESET_ALL} table into table {Fore.RED}{person_table}{Style.RESET_ALL} ?''')

    if not ok:
        sys.exit(f'{Fore.RED}------Stopped ------{Style.RESET_ALL}') 


    min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(publication_article_table, processed_flag)  
    print(f'min_id: {min_id}, max_id: {max_id}')

    step = 1
    batch_size = 50
    id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

    
    mysql = db().mysql_conn()
    
    insert_person_cursor = mysql.cursor(buffered=True) 
    update_flag_cursor = mysql.cursor(buffered=True) 
    fetch_cursor = mysql.cursor(buffered=True, dictionary=True) 

    insert_sql = f'''
        INSERT INTO {person_table} (associate_id, associate_type, source, title, first_name, last_name, collective_name, role, affiliation, orcid)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    #
    # The column 'associate_id' is pubmed_id
    associate_type = 'author'
    source = 'Publication' 
    role = 'author'
    
    _count = 0
    for start_id, end_id in id_ranges:
        
        #1.    
        query = f'''
                SELECT  pa.pubmed_id, pa.source_json
                FROM  {publication_article_table} pa
 
                LEFT JOIN {person_table} p
                ON pa.pubmed_id = p.associate_id AND p.source = '{source}'
 
                WHERE p.associate_id is null 
                AND(pa.id BETWEEN {start_id} AND {end_id}) 
                AND (pa.processed is null OR pa.processed != '{processed_flag}')
            '''  
        
        # For quick insertion:
        # No index for 'associate_id' and 'source' in table person_from_publication
        # Create indexes later 
        query = f'''
                SELECT  pa.pubmed_id, pa.source_json
                FROM  {publication_article_table} pa
                WHERE  pa.id BETWEEN {start_id} AND {end_id}
                AND (pa.processed is null OR pa.processed != '{processed_flag}')
            ''' 
        #print(query)

        fetch_cursor.execute(query)
        rows = fetch_cursor.fetchall()
        # Example pubmed_id = 24831517

        #2.
        authors = []

        for row in rows:            
            pubmed_id = row['pubmed_id']
            source_json = row['source_json']

            bigObj = json.loads(source_json)

            authorsList = bigObj.get('authorList', {}).get('author', [])
            if not authorsList:
                continue
            
            for author in authorsList:
                '''
                [
                    {
                        "fullName": "Aloj SM",
                        "firstName": "S M",
                        "lastName": "Aloj",
                        "initials": "SM",
                        "authorId": {
                            "type": "ORCID",
                            "value": "0000-0003-1703-7523"
                        },
                        "authorAffiliationDetailsList": {
                            "authorAffiliation": [
                                {
                                    "affiliation": "Section on Biochemistry of Cell Regulation, Laboratory of Biochemical Pharmacology, National Institute of Arthritis, Metabolism, Bethesda, Maryland 20014"
                                },
                                {
                                    "affiliation": "Centro di Endocrinologia ed Oncologia Sperimentale C.N.R., Naples, Italy"
                                }
                            ]
                        }
                    }
                ]
                '''
                first_name = author.get('firstName')
                last_name = author.get('lastName')

                # If the authorId type is ORCID
                orcid = author.get('authorId', {}).get('value') if author.get('authorId', {}).get('type') == 'ORCID' else None

                authorAffiliation = author.get('authorAffiliationDetailsList', {}).get('authorAffiliation', [])

                affiliation = None
                if authorAffiliation and len(authorAffiliation) > 0:
                    affiliation = authorAffiliation[0].get('affiliation', None)  

                if first_name is not None or last_name is not None:
                     #(associate_id, associate_type, source, title, first_name, last_name, collective_name, role, affiliation, orcid)
                    authors.append((pubmed_id, associate_type, source, None, first_name, last_name, None, role, affiliation, orcid))

                else: 
                    collectiveName = _normalize_txt(author.get('collectiveName', None))

                    if collectiveName:
                        collectiveName = collectiveName[0: 3500] # max legth is 3500
                        #(associate_id, associate_type, source, title, first_name, last_name,collective_name, role, affiliation, orcid)
                        authors.append((pubmed_id, associate_type, source, None, None, None, collectiveName, role, affiliation, orcid))
  
   
        #3. insert into database
        if len(authors) > 0:

            normalized_authors = [ _normalize_tuple(author) for author in authors]
            
            insert_person_cursor.executemany(insert_sql, normalized_authors)

            update_flag_cursor.execute(f"UPDATE {publication_article_table} SET processed = '{processed_flag}' WHERE id BETWEEN {start_id} AND {end_id}")   
            mysql.commit() 

            _count += len(normalized_authors)
            print(f'{_curr_timestamp()} flag={processed_flag}, Id range: [{start_id} - {end_id}], total = {_count}. #Authors = {len(normalized_authors)}')

    
    fetch_cursor.close()
    insert_person_cursor.close() 

    # Create index on 'associate_id' and 'source'
    ''' CREATE INDEX idx_pfp_associate_id ON person_from_publication (associate_id);'''

    create_index_sqls = [
        f'CREATE INDEX idx_personall_associate_id ON {person_table} (associate_id);',
        f'CREATE INDEX idx_personall_source ON {person_table} (source);'
    ]

    print(f'{Fore.BLUE+Style.BRIGHT}\n\n{_curr_timestamp()}Create index on \'associate_id\' and \'source\'{Style.RESET_ALL}\n\n') 

    create_index_cursor = mysql.cursor(buffered=True) 

    # Execute each CREATE INDEX statement separately
    for sql in create_index_sqls:
        print(sql)
        try:
            create_index_cursor.execute(sql)
            mysql.commit()  # Commit after each statement
        except Exception as e:
            print(f"Error creating index: {e}")
            # Continue with next index even if one fails (e.g., index already exists)

    create_index_cursor.close()
    mysql.close()
    
    print(f'{_curr_timestamp()} Indexes have been created.')

    print(f'{Fore.BLUE+Style.BRIGHT}{"="*50} Done. Total = {_count} {"="*50}{Style.RESET_ALL}\n\n') 

    # https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:10758950&resultType=core&format=json
    # https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:11716886&resultType=core&format=json
   

    '''
    rdas-memgraph/tools/execute-the-long-running-SQL.py

    commands = [ 
        f'CREATE INDEX idx_poas_source ON rdas_db.{person_of_all_sources} (source)',
        f'CREATE INDEX idx_poas_first_name ON rdas_db.{person_of_all_sources} (first_name)',
        f'CREATE INDEX idx_poas_last_name ON rdas_db.{person_of_all_sources} (last_name)',  
        f'CREATE INDEX idx_poas_associate_id ON rdas_db.{person_of_all_sources} (associate_id)',
        f'CREATE INDEX idx_poas_affiliation ON rdas_db.{person_of_all_sources} (affiliation)',
        f'CREATE INDEX idx_poas_orcid ON rdas_db.{person_of_all_sources} (orcid)',
        f'CREATE INDEX idx_poas_processed ON rdas_db.{person_of_all_sources} (processed)',

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
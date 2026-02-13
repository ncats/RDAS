import os
import sys
import json
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv()

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from utils.conn import DBConnection as db 
from utils.tools import ask_to_continue, _id_range_generator, _hash, _normalize_txt, _to_txt
from utils.minmaxid import MinMaxIdLoader 


_FLAG = 'ASD-0' #ASD means after shutdown
publication_article = 'publication_article'
publication_substance = 'publication_substance'
publication_substance_unique = 'publication_substance_unique'
 
if __name__ == "__main__": 

    ok = ask_to_continue(f'Retrieve chemical substances from {publication_article} and insert into table {publication_substance}?')
    if not ok:
        sys.exit('------Stopped ------')
 
    mysql = db().mysql_conn()
    insert_cursor = mysql.cursor(buffered=True) 
    update_cursor = mysql.cursor(buffered=True) 
    fetch_cursor = mysql.cursor(dictionary=True, buffered=True) 

  
    min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(publication_article, _FLAG)    
    print(f'min_id: {min_id}, max_id: {max_id}')
 
    #2
    step = 3
    batch_size = 200
    id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

    _count = 0
    for start_id, end_id in id_ranges:

        query = f"""
            SELECT pubmed_id, source_json
            FROM {publication_article}
            WHERE (id BETWEEN {start_id} AND {end_id}) AND (processed is null OR processed != \'{_FLAG}\')
        """
        fetch_cursor.execute(query)
        rows = fetch_cursor.fetchall()

        chemicals = []
        for row in rows:

            pubmed_id = int(row['pubmed_id'])
            source_json = row['source_json']

            sourceObj = json.loads(source_json)

            if 'chemicalList' in sourceObj and  'chemical' in sourceObj['chemicalList']:
                chemicalList = sourceObj['chemicalList']['chemical']

                for chem in chemicalList:
                    hash_id = None
                    substance_name = _normalize_txt(chem.get('name'))
                    registryNumber = _normalize_txt(chem.get('registryNumber'))

                    if registryNumber == '0':
                        registryNumber = None

                    if registryNumber or substance_name:
                        hash_id = _hash(_to_txt(registryNumber) + _to_txt(substance_name)) 

                        chemicals.append((pubmed_id, substance_name, registryNumber,hash_id)) 


        if len(chemicals) > 0:

            _count += len(chemicals)

            insert_cursor.executemany(f"INSERT INTO {publication_substance} (pubmed_id, substance_name, registry_number, hash_id) VALUES (%s, %s, %s, %s)", chemicals)
            mysql.commit()

            update_cursor.execute(f"UPDATE {publication_article} SET processed = \'{_FLAG}\' WHERE (id BETWEEN {start_id} AND {end_id})")
            mysql.commit()

        print(f'Total chemical substance: {_count}, Id range: {start_id} - {end_id}, #chemicals = {len(chemicals)}')


    print(f'\n\n{Fore.BLUE}========= Upload data to {publication_substance} done ========={Fore.RESET}\n\n') 
     

    # Create indexes
    print('------ Created indexes ------')
    # column_name: index_name
    indexes = {
        "hash_id": "idx_pub_substance_hash_id",
        "registry_number": "idx_pub_subs_registry_num",
        "substance_name": "idx_pub_subs_name"
    }

    for i, (column, index_name) in enumerate(indexes.items(), start=1):
        print(f"{i}. Create index on {publication_substance}.{column}\n")
        query = f"CREATE INDEX {index_name} ON {publication_substance} ({column});"

        update_cursor.execute(query)
        mysql.commit()

    
    # Close the cursors and the connection
    for resource in (fetch_cursor, insert_cursor, update_cursor, mysql):
        if resource:
            try:
                resource.close()
            except Exception as e:
                print(f"Error closing resource: {e}")


    print(f'{Fore.BLUE}\n\n************ Total chemical substance: {_count}, DONE ************{Fore.RESET}\n\n')



    # =================================================================================================================================================
    # Create publication_substance_unique table -------------------------------------------------------------------------------------------------------

    ''' f you want to avoid inserting duplicates that may already exist in the destination table, you can use INSERT IGNORE '''

    insert_new_sbustance = f'''
        INSERT IGNORE INTO rdas_db.{publication_substance_unique} (registry_number, substance_name, hash_id)
        SELECT 
            ps.registry_number,
            ps.substance_name,
            ps.hash_id
        FROM rdas_db.{publication_substance} ps
        GROUP BY ps.registry_number, ps.substance_name, ps.hash_id
    
    '''
    print(insert_new_sbustance)
            

    
                            
    

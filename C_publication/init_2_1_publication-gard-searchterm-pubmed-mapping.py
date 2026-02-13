import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from collections import OrderedDict
import requests
import time
import json
from dotenv import load_dotenv
load_dotenv()


from utils.conn import DBConnection as db
from utils.applogger import AppLogger
logger = AppLogger().get_logger()
 
"""  
# 1. Generate gard_id -> search item -> pubmed_id mapping table
#  
# (Do this BEFORE 3_publication/v2-init_2_2_publication-unique-pubmed-id-file.py, this py file is deprecated, ignore it)
"""

from utils.tools import ask_to_continue


publication_gard_pubmed = 'publication_gard_pubmed'
publication_gard_searchterm_pubmed_mapping = 'publication_gard_searchterm_pubmed_mapping'


ok = ask_to_continue(f'Generate  Generate gard_id - search_term - pubmd_id mapping and insert into database table {publication_gard_searchterm_pubmed_mapping}?')
if not ok:
    sys.exit('------Stopped ------')


mysql = db().mysql_conn()
mycursor = mysql.cursor()

SEPARATOR = '$$$'
query = f"select gard_id, search_term, pubmed_ids, id from rdas_db.{publication_gard_pubmed} where year_range !='ignore'"

mycursor.execute(query)
rows = mycursor.fetchall()

# Initialize the collections before the loop 
bigGardPubmedIdDict = {}    # To store GARD ID to PubMed ID sets mapping

for row in rows:

    gard_id = row[0]
    search_term = row[1]
    ids_str = row[2]

    if ids_str and len(ids_str.strip())>0:
  
        key = gard_id+ SEPARATOR +search_term
        pubmed_ids = ids_str.split(',')  # Split the comma-separated string
        
        # Initialize dictionary entry if it doesn't exist
        if key not in bigGardPubmedIdDict:
            bigGardPubmedIdDict[key] = set()
        
        bigGardPubmedIdDict[key].update(pubmed_ids)

 
 
# 2. Inert unique gard_id & pubmed_id mapping into table {publication_gard_searchterm_pubmed_mapping}
sorted_items = sorted(bigGardPubmedIdDict.items())
 
for key, pubmed_id_set in sorted_items:

    val_sorted = sorted(pubmed_id_set)

    temp = key.split(SEPARATOR)
    gard_id = temp[0]
    search_term = temp[1]

    print(f'{gard_id} - {len(val_sorted)}')

    val_list = [(gard_id, search_term, pubmedid) for pubmedid in val_sorted]  # List comprehension for pairs
    
    # SQL query with placeholders
    insert_sql = f"INSERT INTO {publication_gard_searchterm_pubmed_mapping} (gard_id, search_term, pubmed_id) VALUES (%s, %s, %s)"
    
    # Use executemany for batch insert
    mycursor.executemany(insert_sql, val_list)

    # Commit the transaction
    mysql.commit()


print(f'\n**********Process done, create indexes **********\n') 


# create indexes
idx_gard_id = f'CREATE INDEX idx_gard_id ON {publication_gard_searchterm_pubmed_mapping} (gard_id)'
mycursor.execute(idx_gard_id)
print(idx_gard_id)

idx_pubmed_id = f'CREATE INDEX idx_pubmed_id ON {publication_gard_searchterm_pubmed_mapping} (pubmed_id)'
mycursor.execute(idx_pubmed_id)
print(idx_pubmed_id)
  
idx_gard_id_searchterm = f'CREATE INDEX idx_gardid_searchterm ON {publication_gard_searchterm_pubmed_mapping} (gard_id, search_term)'
mycursor.execute(idx_gard_id_searchterm)
print(idx_gard_id_searchterm)
 
idx_gard_id_searchterm_pubmed = f'CREATE INDEX idx_gardid_searchterm_pubmed on  {publication_gard_searchterm_pubmed_mapping} (gard_id, search_term, pubmed_id)'
mycursor.execute(idx_gard_id_searchterm_pubmed)
print(idx_gard_id_searchterm_pubmed)

# commit create indexes
mysql.commit()

if mycursor:
    mycursor.close()

if mysql:
    mysql.close()

print(f'\n\n---------- ALL DONE ----------\n\n') 
 
    # select *  from rdas_db.{publication_gard_pubmed} where year_range !='ignore' and year_range != '1970-2025';
    # select *  from rdas_db.{publication_gard_pubmed} where year_range !='ignore' and year_range != '1970-2025' and total > retmax;
    # select *  from rdas_db.{publication_gard_pubmed} where year_range !='ignore' and length(search_term)>4

    # NEXT Step
    #https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=9672646&format=json&resultType=core
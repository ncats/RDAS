import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import re
import nltk
print(f'nltk version: {nltk.__version__}')
print(f'nltk.data.path: {nltk.data.path}')
# Manually install punkt and stopwords in /Users/zhaot3/nltk_data
# cd /Users/zhaot3/nltk_data
# wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/tokenizers/punkt.zip
# wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/stopwords.zip
# wget --no-check-certificate https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/english_wordnet.zip
'''
nltk.download('wordnet')
nltk.download('punkt')
nltk.download('stopwords')
'''
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize

from dotenv import load_dotenv
load_dotenv()
 
from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from itertools import permutations
from utils.conn import DBConnection as db
from utils.tools import ask_to_continue, _normalize_txt, _normalize_tuple, _stem_text, _remove_stop_words


# 1. Manually create table grant_gard_processed_names
'''
    CREATE table rdas_db.grant_gard_processed_names as (
    SELECT 
            GardID,
            MAX(CASE WHEN Label_Predicate_Type = 'Name' THEN Label END) AS `name`,
            GROUP_CONCAT(CASE WHEN Label_Predicate_Type = 'Synonym' THEN Label END SEPARATOR '$$$') AS `synonyms`,
            Label_Source as data_source
        FROM  rdas_db.gard
        WHERE 
            Label_Predicate_Mapping != 'DEPRECATED' 
            AND LENGTH(Label) > 3
        GROUP BY 
            GardID, MONDO_ID, Label_Source
    )
'''
# 2. Add columns: Synonyms_sw, Synonyms_sw_bow, Synonyms_sw_stem, Synonyms_sw_stem_bow
'''
    ALTER TABLE `rdas_db`.`grant_gard_processed_names` 
    ADD COLUMN `Synonyms_sw` TEXT NULL AFTER `data_source`,
    ADD COLUMN `Synonyms_sw_bow` TEXT NULL AFTER `Synonyms_sw`,
    ADD COLUMN `Synonyms_sw_stem` TEXT NULL AFTER `Synonyms_sw_bow`,
    ADD COLUMN `Synonyms_sw_stem_bow` TEXT NULL AFTER `Synonyms_sw_stem`,
    ADD COLUMN `created` DATETIME NULL DEFAULT Current_timestamp() AFTER `Synonyms_sw_stem_bow`;
'''

def _generate_term_orders(terms):
    words = terms.split()
    if len(words) ==2:
      all_permutations = list(permutations(words))
      orders = [' '.join(permutation) for permutation in all_permutations]
      return orders
    else: 
       return [terms]

def generate_term_orders_list_of_words(words):
    X = []
    for wd in words:
      X += _generate_term_orders(wd)
    return X

def len_chcek(row):
      return [w for w in row if (len(w) >4) or (w == "sars") ]


def stem_text_list(row):
      return [_stem_text(w) for w in row if len(_stem_text(w)) >2 ]

def to_string(the_list):
    return '$$$'.join(the_list)


ok = ask_to_continue(f'*** Process and Upload the GARD names into MySQL database? *** ')

if not ok:
    sys.exit(Fore.RED + '\n------------------------ Stopped ------------------------\n'+ Style.RESET_ALL)


mysql = db().mysql_conn()
update_cursor = mysql.cursor(buffered=True)
update_sql = ''' update grant_gard_processed_names set Synonyms_sw = %s, Synonyms_sw_bow = %s, Synonyms_sw_stem = %s, Synonyms_sw_stem_bow = %s  where gardid =%s'''

dict_cursor = mysql.cursor(dictionary=True, buffered=True)

query = ''' select gardid, name, synonyms, data_source from grant_gard_processed_names'''
dict_cursor.execute(query)
rows = dict_cursor.fetchall()

if len(rows) == 0:
    print(Fore.RED + '\n------------------------ The  grant_gard_processed_names is empty ------------------------\n'+ Style.RESET_ALL)

count = 0
list_of_tuples = []

for row in rows:
    count += 1
    gard_id = row['gardid']

    name = row['name']
    synonyms_str = row['synonyms']
    data_source = row['data_source']  

    name = name.replace('"', '').lower()

    synonyms_list = []
    if synonyms_str:
        synonyms_list = [ _normalize_txt(syn).replace('"', '').lower() for syn in synonyms_str.split('$$$') ]
     
    synonyms_list.append(name)
    synonyms_list = list(set(synonyms_list))

    ''' Ask Qian about the order of the len_chcek'''
    Synonyms_sw = synonyms_list  
    
    Synonyms_sw_stem = stem_text_list(Synonyms_sw) 
    Synonyms_sw_bow = generate_term_orders_list_of_words(Synonyms_sw)  
    Synonyms_sw_stem_bow = generate_term_orders_list_of_words(Synonyms_sw_stem)     
    
    Synonyms_sw = list(set(len_chcek(Synonyms_sw)))
    Synonyms_sw_bow = list(set(len_chcek(Synonyms_sw_bow)))
    Synonyms_sw_stem = list(set(len_chcek(Synonyms_sw_stem))) 
    Synonyms_sw_stem_bow = list(set(len_chcek(Synonyms_sw_stem_bow))) 

    list_of_tuples.append(_normalize_tuple((to_string(Synonyms_sw), to_string(Synonyms_sw_bow), to_string(Synonyms_sw_stem), to_string(Synonyms_sw_stem_bow), gard_id)))

    if count % 20 == 0:
        update_cursor.executemany(update_sql, list_of_tuples)
        mysql.commit() 

        print(f'count = {count}')
        list_of_tuples = []


update_cursor.executemany(update_sql, list_of_tuples)
mysql.commit() 

dict_cursor.close()
mysql.close()

print(Fore.BLUE + f'\n=**=**=**=**=**=**=**=**=**=**=**=**=**=**=**= All Done  =**=**=**=**=**=**=**=**=**=**=**=**=**=**=**=\n'+ Style.RESET_ALL)
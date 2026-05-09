import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from baseclass.conn import DBConnection as db


GARD = 'gard'
publication_gard_omim_mapping = 'publication_gard_omim_mapping'

# Create GARD id --- OMIM id mapping table
message = f''' Retrieve GARD id & OMIM id from gard table, and insert into table {publication_gard_omim_mapping}'''

from utils.tools import ask_to_continue, _id_range_generator

ok = ask_to_continue(f'{message}?')
if not ok:
    sys.exit('------Stopped ------')


mysql = db().mysql_conn()
mycursor = mysql.cursor()

# 1. Get GardID and label_xref information
query = f'SELECT GardID, group_concat(Label_Xref) FROM {GARD} GROUP BY gardid ORDER BY GardID'
insert_sql = f'INSERT INTO {publication_gard_omim_mapping} (gard_id, omim_id) VALUES (%s, %s)'

count = 0

try:
    mycursor.execute(query)

    for row  in mycursor.fetchall():
        gard_id = row[0]
        label_xref = row[1]

        val_list = []

        if 'OMIM' in label_xref:

            omims = [item.split(':')[1] for item in label_xref.split(',') if item.strip().startswith('OMIM')]
            unique_omims = list(set(omims))

            for omim in unique_omims:

                if not omim.isdigit():
                    continue

                count += 1
                val_list.append((gard_id, omim))

            try:
                mycursor.executemany(insert_sql, val_list)
                mysql.commit()
                print(f'{count}\t{gard_id}')

            except Exception as e:
                print(f'insert_sql error: \n{e}')
                sys.exit()

except Exception as e:
    print(e)


if mycursor:
    mycursor.close()

if mysql:
    mysql.close()


print('-------------------------------- Done -------------------------------------')
import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.conn import DBConnection as db
from utils.tools import ask_to_continue
from utils.applogger import AppLogger
logger = AppLogger().get_logger()

#MySQL
""" 
# Create and Store UNIQUE Clinical Trial into  clinical_trial_unique table

CREATE TABLE clinical_trial_unique (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nctid VARCHAR(255) NOT NULL,
    studies MEDIUMTEXT NULL
);
 
#
"""

class ClinicalTrialInitializer:

    def __init__(self): 
        self.mysqldb = db().mysql_conn() 
        

    def do_init(self):
        #1. 
        query = 'SELECT DISTINCT CONCAT(\'\\\'\', nctid, \'\\\'\') AS nct_id FROM clinical_trial WHERE nctid is not null ORDER BY nctid'

        cursor = self.mysqldb.cursor()
        cursor.execute(query)
        results = cursor.fetchall()

        unqi_sorted_nctid_list = [ row[0] for row in results]
        print(f'unqi_sorted_nctid_list.size = {len(unqi_sorted_nctid_list)}')

        #2.
        batch_size = 50
        list_size = len(unqi_sorted_nctid_list) 
        
        insert = 'INSERT INTO clinical_trial_unique (nctid, studies) VALUES (%s,%s)'

        for i in range(0, list_size, batch_size):
            
            batch = unqi_sorted_nctid_list[i: min(i+batch_size, list_size)]
            print(f'\nbatch idx ={i}')
            id_INs = f"{','.join(batch)}"
            fetch = f'''
                SELECT nctid, studies
                FROM (
                    SELECT nctid, studies, 
                        ROW_NUMBER() OVER (PARTITION BY nctid ORDER BY nctid DESC) AS rn
                    FROM clinical_trial 
                    WHERE nctid IN ({id_INs}) 
                ) t
                WHERE rn = 1
            '''

            cursor.execute(fetch)
            nct_rows = cursor.fetchall()

            val_list = []
            for row in nct_rows:
                val_list.append((row[0], row[1]))
            
            cursor.executemany(insert, val_list)

            self.mysqldb.commit()

        self.mysqldb.commit()
      
        if cursor:
            cursor.close()
        if self.mysqldb:
            self.mysqldb.close()


if __name__ == '__main__':

    ok = ask_to_continue('Create and Store UNIQUE Clinical Trial into  clinical_trial_unique table?')
    if not ok:
        sys.exit('------Stopped ------')


    initlzr = ClinicalTrialInitializer()

    initlzr.do_init()

    print('\n\n---------------- Done ----------------\n\n')
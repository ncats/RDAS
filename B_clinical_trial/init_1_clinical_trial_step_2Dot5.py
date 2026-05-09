import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

#
import mysql.connector
#
import json
import time
from baseclass.conn import DBConnection as db
from utils.tools import _is_english, _is_under_char_threshold, ask_to_continue
from utils.applogger import AppLogger
logger = AppLogger().get_logger()
# Initialize colorama for Windows compatibility
from colorama import init, Fore, Style
init()

#MySQL
""" 
# Add two extra columns: brief_title and brief_summary into the clinical_trial and clinical_trial_unique table
#
"""

class ClinicalTrialDataInitializer:
  
    def __init__(self):
        
        self.mysql = db().mysql_conn() 

 
    def update(self, batch_size=20):

        count = 0
        batch_num = 0
        last_id = 0
        
        try:
            # Fetch results in batches
            while True:

                chunks = []
                cursor = self.mysql.cursor(dictionary=True, buffered=True)         

                query = f'''SELECT id, nctid, studies 
                    FROM clinical_trial_unique 
                    WHERE brief_title IS NULL AND id > {last_id} 
                    ORDER BY id 
                    LIMIT {batch_size}
                '''
    
                cursor.execute(query)
                rows = cursor.fetchall()
                if not rows:
                    break
                
                last_id = rows[-1]['id'] 
            
                print(f'\n{Fore.BLUE}====== batch# = {batch_num} ======{Style.RESET_ALL}')
                batch_num += 1

                for row in rows:
                    nctid = row['nctid']
                    study = row['studies']

                    try:
                        study = json.loads(study)

                        protocol_section = study.get('protocolSection', {})
                        brief_title = protocol_section.get('identificationModule', {}).get('briefTitle', 'N/A')
                        brief_summary = protocol_section.get('descriptionModule', {}).get('briefSummary', 'N/A')

                        chunks.append((brief_title, brief_summary, nctid))

                        print(f'NCTID = {nctid}')
                        
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON for ID {nctid}: {Fore.RED}{e}{Style.RESET_ALL}")
                        
                        chunks.append(('N/A', 'N/A', nctid))
                        continue
                
                cursor.close()  # free memory immediately
                
                if len(chunks) > 0:
                    self._save(chunks)
                    count += len(rows)

                    self.mysql.commit()
                    print(f'Total clinical trials processed: {count}')

                
        except Exception as err:
            print(f"Error: {Fore.RED}{err}{Style.RESET_ALL}")
            return None
        
        finally:
            # Ensure resources are closed
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'connection' in locals() and self.mysql.is_connected():
                self.mysql.close()

        
    def _save(self, chunks):

        insert_1_sql = 'UPDATE clinical_trial set brief_title=%s, brief_summary=%s WHERE nctid=%s'
        insert_2_sql = 'UPDATE clinical_trial_unique set brief_title=%s, brief_summary=%s WHERE nctid=%s'

        try:
            cursor = self.mysql.cursor()
            cursor2 = self.mysql.cursor()
            cursor.executemany(insert_1_sql, chunks)
            self.mysql.commit() 
            cursor2.executemany(insert_2_sql, chunks)
            self.mysql.commit() 

            cursor.close()
            cursor2.close()
            
        except Exception as e:
            print(e)
            raise
        



if __name__ == '__main__':

    ok = ask_to_continue('Add two extra columns: brief_title and brief_summary into the clinical_trial and clinical_trial_unique table?')
    if not ok:
        sys.exit('------Stopped ------')

    # Total in memgraph database is: 15,323
    initlzr = ClinicalTrialDataInitializer()

    initlzr.update()

    print('\n\n---------------- Done ----------------\n\n')

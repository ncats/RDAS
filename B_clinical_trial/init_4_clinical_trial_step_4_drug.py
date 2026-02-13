import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mysql
import mysql.connector

from utils.tools import ask_to_continue
from utils.conn import DBConnection as db

#Memgraph
"""
# Step 4: Create Intervention - Drug relationship 'mapped_to_rxnorm' in Memgraph

# Fetch Drug property from MySQL database, and create "Drug" by RNormID, and map Intervention to Drug

    #
    # 2. conda activate rdas
    # 3. python 2_clinical_trial/init_4_clinical_trial_step_4_drug.py  
    #
"""

#
#
#
# Depracted: see init_ClinicalTrail_all.py
#
#
#

class DrugSplIdInitializer:

    def __init__(self):

        self._conn = db().memgraph_conn()
        self.mysql = db().mysql_conn()
        print('\nDrugSplIdInitializer init...\n')


    def do_init(self, skip, limit):

        # 0.
        print('\n\nCREATE INDEX ON :Intervention(InterventionName)')

        self._conn.execute_and_fetch('CREATE INDEX ON :Intervention(InterventionName)')
 

        # 1.
        query = f'''

           SELECT 
                RxNormID, intervention, wspacy, 
                GROUP_CONCAT(CONCAT('x.', property_key, '=', property_val)) AS props
            FROM 
                (SELECT DISTINCT 
                    RxNormID, intervention, wspacy, property_key, property_val 
                FROM 
                    rdas_db.clinical_trail_intervention_drug 
                WHERE 
                    property_key != 'RxNormID' 
                ORDER BY 
                    RxNormID, intervention, wspacy, property_key, property_val) AS deduped
            GROUP BY 
                RxNormID, intervention, wspacy 
             
            {f"LIMIT {limit}" if limit else ""}
            {f"OFFSET {skip}" if skip else ""}
            '''
        
        try:
            cursor = self.mysql.cursor()
            cursor.execute(query)

            count = 0
            for row in cursor.fetchall():
                RxNormID = row[0]
                intervention_name = row[1]
                wspacy = row[2]
                props = row[3] 

                wspacyy = "true" if wspacy == 1 else "false"

                query = f'''
                    MERGE (x:Drug {{RxNormID: {RxNormID}}}) 
                    ON CREATE SET {props}
                    WITH x MATCH (y:Intervention {{InterventionName: "{intervention_name}" }}) 
                    MERGE (y)-[:mapped_to_rxnorm {{WITH_SPACY: {wspacyy} }}]->(x)
                    RETURN TRUE
                ''' 
                try: 
                    list(self._conn.execute_and_fetch(query))  # wait to complete    

                    # show progress
                    count += 1
                    if count%200 != 0:
                        print('.', end=" ")
                    else:
                        print('.')     

                except Exception as e:
                    print(f'\n---------------\n{e}\nRxNormID = {RxNormID}')    

        except mysql.connector.Error as err:
            print(f"Error: {err}")

        finally:
            # Clean up and close connections
            if cursor:
                cursor.close()
            if self.mysql:
                self.mysql.close()


if __name__ == '__main__':
    #
    #
    #
    # Depracted: see init_ClinicalTrail_all.py
    #
    #
    #

    ok = ask_to_continue('Create Intervention - Drug relationship "mapped_to_rxnorm"?')
    if not ok:
        sys.exit('------Stopped ------')


    initlzr = DrugSplIdInitializer()
 
    print('-------------------------------------------------------------------------------------')
    print('\nCREATE INDEX ON :Intervention(InterventionName)')
    print('SHOW INDEX INFO')
    print('DROP INDEX ON :Intervention(InterventionName)')
    print('MATCH ()-[r:mapped_to_rxnorm]->()  RETURN COUNT(r) AS relationship_count\n')

    print('\n\n 4 RxNormID with large SPL_SET_ID:')
    print('448, 4910, 11295, 1000577')
    print('-------------------------------------------------------------------------------------')


    initlzr.do_init(None, None)

    print('\n Next step, run init_4_clinical_trial_step_4_followup_drug.py\n')
import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import ast
import mysql
import mysql.connector

from utils.tools import ask_to_continue
from utils.conn import DBConnection as db

#Memgraph
""" 
# Fellow up with step 4: init_4_clinical_trial_step_4_drug.py
# Create Intervention - Drug relationship 'mapped_to_rxnorm'

# These RxNormIDs are failed in Step 4, caused by the too large size of 'SPL_SET_ID'
# 448, 4910, 11295, 1000577
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
        self.cursor = self.mysql.cursor()
        print('\nDrugSplIdInitializer init...\n')


    def custom_insert_missing_nodes(self, RxNormID_list):
        
        RxNormIDs = ', '.join(map(str, RxNormID_list))
        print(f'Missing nodes: {RxNormIDs}')

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
                AND 
                    property_key != 'SPL_SET_ID'
                AND 
                    RxNormID in ({RxNormIDs}) 

                ORDER BY 
                    RxNormID, intervention, wspacy, property_key, property_val
                ) AS deduped

            GROUP BY 
                RxNormID, intervention, wspacy 
            ''' 

        try: 
            self.cursor.execute(query)
 
            for row in self.cursor.fetchall():
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
                    print(f'\nRxNormID = {RxNormID}')    

                except Exception as e:
                    print(f'\n---------------\n{e}\nRxNormID = {RxNormID}')    
        except mysql.connector.Error as err:
            print(f"Error: {err}")
         


    def update_nodes_with_SPL_SET_ID_list(self, RxNormID_list):
        
        RxNormIDs = ', '.join(map(str, RxNormID_list))

        # 1.
        query = f'''
            SELECT  
                RxNormID, property_key, property_val 
            FROM 
                rdas_db.clinical_trail_intervention_drug 
            WHERE  
                property_key = 'SPL_SET_ID'
            AND 
                RxNormID in ({RxNormIDs}) 
            GROUP BY 
                RxNormID, property_key, property_val
            '''
        try:
            self.cursor.execute(query)
 
            for row in self.cursor.fetchall():
                RxNormID = row[0] 

                SPL_SET_ID_str = row[2]  
                # Convert to list
                SPL_SET_ID_list = ast.literal_eval(SPL_SET_ID_str)

                query = f'''
                    MERGE (x:Drug {{RxNormID: {RxNormID}}}) 
                    SET x.SPL_SET_ID = {SPL_SET_ID_list}
                    RETURN TRUE
                ''' 
                try: 
                    list(self._conn.execute_and_fetch(query))  # wait to complete    
                    print(f'\nSPL_SET_ID: RxNormID = {RxNormID}')    

                except Exception as e:
                    print(f'{e}')    
        except mysql.connector.Error as err:
            print(f"Error: {err}")
         


    def do_init(self):

        print('Start do_init() ......')
        #1. Insert the missing/Error nodes in 'init_4_clinical_trial_step_4_drug.py'
        RxNormID_list = [448, 4910, 11295, 1000577]
        self.custom_insert_missing_nodes(RxNormID_list)
        
        #2. update nodes with SPL_SET_ID value
        self.update_nodes_with_SPL_SET_ID_list(RxNormID_list)

        #3. 
        if self.cursor:
            self.cursor.close()
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

    ok = ask_to_continue('Follow Up: Create Intervention - Drug relationship "mapped_to_rxnorm"?')
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

    # Ask the user if they have created the index
    user_response = input("Insert the missing nodes 448, 4910, 11295, 1000577 ? (Yes/Y or No/N): ").strip().lower()

    if user_response not in ['yes', 'y']:
        sys.exit()

    initlzr.do_init()
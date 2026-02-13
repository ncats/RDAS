import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
import json
from dotenv import load_dotenv
load_dotenv()
 
from utils.conn import DBConnection as db 
from utils.tools import _id_range_generator, ask_to_continue
from cypher_helpers import _get_GARD_names_syns, _update_gard_node, _add_gard_node, \
     cypher_ClinicalTrial_contacts_mapping, cypher_Condition, cypher_Condition_mapping, \
     cypher_AssociatedEntity, cypher_Location, cypher_Location_contacts_mapping, cypher_Investigator, \
     cypher_StudyDesign, cypher_PrimaryOutcome, cypher_Participant, cypher_Reference, cypher_Intervention, \
     cypher_IndividualPatientData, init_cypher_create_ClinicalTrial_node, unwind_init_cypher_create_ClinicalTrial_node


from utils.applogger import AppLogger
logger = AppLogger().get_logger()

#MySQL
"""
# (Step 1: See init_clinical_trial_step_1.py)
# Step 2: Insert the Clinical-Trials from MySQL database into Memgraph database
# On MySQL: 
#   1. create index clinical_trial_nctid_idx on rdas_db.clinical_trial  (nctid);
#   2. create index clinical_trial_nctid_idx on rdas_db.clinical_trial  (gardId, nctid);
#   3. create index ct_gard_disease_nctid_idx on rdas_db.clinical_trial  (gardid, disease, nctid);

# create index on :GARD(GardId)
# create index on :ClinicalTrial(NCTId)
# DROP INDEX ON :ClinicalTrial(NCTId) 

#
# 0. Docker Desktop(Mac desktop): run container "mariadb"
# 1. conda activate rdas
# 2. python clinical_trial/init_clinical_trial_step_2.py 
#
"""

#
#
#
# Depracted: see init_ClinicalTrail_all.py
#
#
#

class ClinicalTrialInitializer:

    def __init__(self): 
        self.gard_names_dict = None

        self.last_update= '01/01/1970'
        print(f'\n### Last update: {self.last_update} ###\n')

        self._conn = db().memgraph_conn()
        self.mysql = db().mysql_conn()

        self.ct_min_id = 0
        self.ct_max_id = 0
        self.ct_step = 0

        self.ct_uniq_min_id = 0
        self.ct_uniq_max_id = 0
        self.ct_uniq_step = 0
   

    def get_gard_id_and_names_dict(self):
        if not self.gard_names_dict:
            self.gard_names_dict = _get_GARD_names_syns(self._conn)

        return self.gard_names_dict
      

    def _get_unique_clinical_trial(self, start_id, end_id):         
        # 1. Get distinct Clinical Trial nodes (simple)  
        query = f'''
            SELECT id, nctid, studies 
            FROM 
                clinical_trial_unique
            WHERE nctid IS NOT NULL
            AND id BETWEEN {start_id} AND {end_id}
            ORDER BY id
        '''
        return self.mysql_query(query)  


    def mysql_query(self, query):
        #print(query)

        try:
            #cursor = self.mysql.cursor()
            _conn = db().mysql_conn()
            cursor = _conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall() 
            return rows
        except Exception as err:
            print(f"Error: {err}")
            return None    
        
        finally:
            if cursor:
                cursor.close()

            if _conn:
                _conn.close()

            """
            # Ensure resources are closed
            if 'cursor' in locals() and cursor:
                cursor.close()
           
            if 'connection' in locals() and self.mysql.is_connected():
                self.mysql.close()
            """
    #
    # Initialize the Clinical Trial and the relations
    #
  
    def do_init(self):

        # Step 0: create index on GARD.GardId
        #self.execute_and_fetch_no_results('create index on :GARD(GardId)')

        # Step 1: 
        #self.step_1_create_all_ClinicalTrial_nodes()

        # Step 2: create index on NCTId
        #self.execute_and_fetch_no_results('CREATE INDEX ON :ClinicalTrial(NCTId)')

        # Step 3: create ClinicalTrail & GARD mapping
        #self.step_2_create_ClinicalTrial_GARD_mapping()


        # Step 4: sub steps -- map to ClinicalTrial
        # Starting and ending IDs in clinical_trial_unique table
        batch_size = 200 
        id_ranges = _id_range_generator(self.ct_uniq_min_id, self.ct_uniq_max_id, self.ct_uniq_step, batch_size)
 
        # For loop over ID ranges
        for start_id, end_id in id_ranges: 
            self.sub_steps(start_id, end_id) 
            self.set_sleep(1)     


        # Step 5: Create index on Contact.ContactName first, see why in cypher_Investigator() method.
        self.execute_and_fetch_no_results('create index on :Contact(ContactName)')

        # Step 6: Investigator (Since it needs match with 'Contact' on 'ContactName', so need create index first)
        self.substeps_cypher_Investigator()



    def sub_steps(self, start_id, end_id):

        # 0. Get the id, nctid and study from the MySQl database
        rows = self._get_unique_clinical_trial(start_id, end_id)
   
        for idx, row in enumerate(rows):
            id = row[0]
            nctid = row[1] 
            full_study = json.loads(row[2])
            print(f"\n# 4. Sub steps::  [{start_id} - {end_id}]: Id: {id}, NCTId: {nctid}")


            # 4.2: create ClinicalTrial and contacts mapping

            print(f'\t4. sub_steps: init_cypher_ClinicalTrial_contacts_mapping: {nctid}')
            for query in cypher_ClinicalTrial_contacts_mapping(nctid, full_study):
                self.show_progress()
                try:                    
                    list(self._conn.execute_and_fetch(query)) # wait to complete
                except Exception as e:
                    logger.error(e) 
      
            # 4.3: Set ClinicalTrial & Location mapping
            print(f'\t4. sub_steps: cypher_Location: {nctid}')
            for r_in_locations, location in cypher_Location(full_study): 
                self.show_progress()
                result = list(self._conn.execute_and_fetch(r_in_locations))
                location_interal_id = result[0]['location_id'] 
               
                print(f'\t\tcypher_Location_contacts_mapping: location_interal_id={location_interal_id}')
                for query in cypher_Location_contacts_mapping(nctid, location_interal_id, location):
                    try:
                        self.show_progress()
                        list(self._conn.execute_and_fetch(query))
                    except Exception as e:
                        logger.error(e)
  
         
            # 4.4 Set condition 
            gard_id_names_dict =  self.get_gard_id_and_names_dict()

            print(f'\t4. sub_steps: cypher_Condition: {nctid}')
            for query, condition_normalized in cypher_Condition(full_study):
                self.show_progress()
                if query:  
                    results = list(self._conn.execute_and_fetch(query))  # wait to complete
                    cond_internal_id = results[0]['cond_id']

                    print(f'\t\tcypher_Condition_mapping: cond_internal_id={cond_internal_id}')
                    for query in cypher_Condition_mapping(cond_internal_id, condition_normalized, gard_id_names_dict):
                        self.execute_and_fetch_no_results(query) 
                        self.show_progress()
            # After inserting Contact completed: create index on :Contact(ContactName)
            # for 'cypher_Investigator()'
            # self.execute_and_fetch_no_results('create index on :Contact(ContactName)')
            ###

           
            # 4.5.1
            # AssociatedEntity info
            print(f'\t4. sub_steps: AssociatedEntity info: {nctid}')
            for query in cypher_AssociatedEntity(full_study): 
                self.execute_and_fetch_no_results(query)
                self.show_progress()
            

            # 4.5.2
            # Investigator info
            # See substeps_cypher_Investigator()
            '''
            print(f'\tInvestigator info: {nctid}')
            for query in cypher_Investigator(full_study):
                self.execute_and_fetch_no_results(query)
                self.show_progress()
            '''
            
            # 4.5.3
            # StudyDesign info
            print(f'\t4. sub_steps: StudyDesign info: {nctid}')
            query = cypher_StudyDesign(full_study)
            self.execute_and_fetch_no_results(query)

            
            # 4.5.4
            # IndividualPatientData
            print(f'\t4. sub_steps: IndividualPatientData: {nctid}')
            query = cypher_IndividualPatientData(full_study)
            self.execute_and_fetch_no_results(query)
           
            # 4.5.5
            # PrimaryOutcome info
            print(f'\t4. sub_steps: PrimaryOutcome info: {nctid}')
            for query in cypher_PrimaryOutcome(nctid, full_study):
                self.execute_and_fetch_no_results(query)
                self.show_progress()

            # 4.5.6
            # Participant info
            print(f'\t4. sub_steps: Participant info: {nctid}')
            query = cypher_Participant(nctid, full_study) 
            self.execute_and_fetch_no_results(query)

            # 4.5.7
            # Intervention info
            print(f'\t4. sub_steps: Intervention info: {nctid}')
            for query in cypher_Intervention(nctid, full_study):
                self.execute_and_fetch_no_results(query)
                self.show_progress()
 
            # 4.5.8
            # Reference info
            print(f'\t4. sub_steps: Reference info: {nctid}')
            for query in cypher_Reference(nctid, full_study):
                self.execute_and_fetch_no_results(query)
                self.show_progress()    
  

    def substeps_cypher_Investigator(self):

        # Step 4: sub steps -- map to ClinicalTrial
        # Starting and ending IDs in clinical_trial_unique table
        batch_size = 200

        # Calculate number of rows (total steps)
        #total_rows = (self.ct_uniq_max_id - self.ct_uniq_min_id) // self.ct_uniq_step + 1 

        # Number of IDs per batch (200 rows * ct_uniq_step)
        id_range_per_batch = batch_size * self.ct_uniq_step  # How many ID units per batch

        # For loop over ID ranges
        for start_id in range(self.ct_uniq_min_id, self.ct_uniq_max_id + 1, id_range_per_batch):
            end_id = min(start_id + id_range_per_batch - 1, self.ct_uniq_max_id)

            rows = self._get_unique_clinical_trial(start_id, end_id)

            for idx, row in enumerate(rows):
                id = row[0]
                nctid = row[1] 
                full_study = json.loads(row[2])
                print(f"\n# 6. Investigator:: [{start_id} - {end_id}]: Id: {id}, NCTId: {nctid}")

                for query in cypher_Investigator(full_study):
                    self.execute_and_fetch_no_results(query)
                    self.show_progress()
 
            self.set_sleep(1)


    def step_2_create_ClinicalTrial_GARD_mapping(self):
        # SELECT count(distinct gardId, disease, nctid) FROM rdas_db.clinical_trial;

        # Starting and ending IDs in clinical_trial table
        batch_size = 1000

        # Calculate number of rows (total steps)
        total_rows = (self.ct_max_id - self.ct_min_id) // self.ct_step + 1  # 499,263 rows
        print(f"Total rows to fetch: {total_rows}")

        # Number of IDs per batch (1000 rows * step of 3)
        id_range_per_batch = batch_size * self.ct_step  # 3000 ID units per batch

        # For loop over ID ranges
        for start_id in range(self.ct_min_id, self.ct_max_id + 1, id_range_per_batch):

            end_id = min(start_id + id_range_per_batch - 1, self.ct_max_id)
            print(f"Fetching batch: id {start_id} to {end_id}")

            query = f'''
                SELECT gardid, disease, nctid, id
                FROM clinical_trial 
                WHERE nctid IS NOT NULL
                AND id BETWEEN {start_id} AND {end_id}
                ORDER BY id
            ''' 
            rows = self.mysql_query(query)
 
            for idx, row in enumerate(rows):
                gard_id = row[0]
                disease = row[1]
                nctid = row[2]  
                id = row[3]
                print(f"# 2. ClinicalTrial_GARD_mapping:: [{start_id} - {end_id}] Id: {id}, {gard_id} - {nctid} - {disease}")

                cypher = f'''
                    MATCH (x:GARD {{GardId: "{gard_id}"}})
                    MATCH (y:ClinicalTrial {{NCTId: "{nctid}"}})
                    MERGE (x)<-[:mapped_to_gard {{MatchedTermRDAS: "{disease}"}}]-(y)
                '''

                self.execute_and_fetch_no_results(cypher)

            self.set_sleep(1)
            


    def step_1_create_all_ClinicalTrial_nodes(self):

        batch_size = 500

        # Calculate number of rows (total steps)
        #total_rows = (self.ct_uniq_max_id - self.ct_uniq_min_id) // self.ct_uniq_step + 1  

        # Number of IDs per batch (200 rows * ct_uniq_step)
        id_range_per_batch = batch_size * self.ct_uniq_step  # How many of ID units per batch

        # For loop over ID ranges
        for start_id in range(self.ct_uniq_min_id, self.ct_uniq_max_id + 1, id_range_per_batch):
            end_id = min(start_id + id_range_per_batch - 1, self.ct_uniq_max_id)

            batch_chunks = []
            rows = self._get_unique_clinical_trial(start_id, end_id)

            # 2. Add ClinicalTrial nodes
            for idx, row in enumerate(rows):
                id = row[0]
                nctid = row[1] 
                full_study = json.loads(row[2])
                #print(f"# 1. Create ClinicalTrial:: [{start_id} - {end_id}]: Id: {id}, NCTId: {nctid}")

                #query = init_cypher_create_ClinicalTrial_node(nctid, full_study)
                #self.execute_and_fetch_no_results(query)
                ct_obj = unwind_init_cypher_create_ClinicalTrial_node(nctid, full_study)
                batch_chunks.append(ct_obj)

            batch_query = '''
                UNWIND $batch_chunks AS ct_properties
                CREATE (n:ClinicalTrial)
                SET n = ct_properties
            ''' 

            self._conn.execute(batch_query, {"batch_chunks": batch_chunks})
            
            print(f'Id range: [{start_id} - {end_id}]: total = {len(batch_chunks)}')

            #self.set_sleep(1)     

 
    def execute_and_fetch_no_results(self, query):
        if not query:
            return         
        try: 
            list(self._conn.execute_and_fetch(query))  # wait to complete, must use 'list' 
        except Exception as e:
            logger.error(e)   
            logger.error(query)  


    def show_progress(self):
        print('.', end=' ')


    def set_sleep(self, t):
        print(f'sleep {t} seconds')
        for t in range(t):
            print('.')
            time.sleep(1)
    
  
if __name__ == '__main__':

    #
    #
    #
    # Depracted: see init_ClinicalTrail_all.py
    #
    #
    #
   
    ok = ask_to_continue('Insert the Clinical-Trials from MySQL database into Memgraph database?')
    if not ok:
        sys.exit('------Stopped ------')


    # Total distinct Clinical Trial: 125675 
    initlzr = ClinicalTrialInitializer()

    # From clinical_trail table
    initlzr.ct_min_id = 3
    initlzr.ct_max_id = 1497792
    initlzr.ct_step = 3

    # From clinical_trail_unique table
    initlzr.ct_uniq_min_id = 364524 #3003 #1203 #603 #3
    initlzr.ct_uniq_max_id = 378099 
    initlzr.ct_uniq_step = 3

    initlzr.do_init()  

    print('\n\n=============== Done ======================\n\n')

# docker run -p 7687:7687 -v /Users/zhaot3/DATA/memgraph-data:/var/lib/memgraph memgraph/memgraph

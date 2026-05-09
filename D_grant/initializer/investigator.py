# Add the project root to the Python path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from dotenv import load_dotenv
load_dotenv()
  
from utils.tools import _id_range_generator, _append_to_file, _format_dollars, _val, _arr, _curr_timestamp
from baseclass.init_base import InitBase

# 1. Create Project
class PrincipalInvestigatorInitializer(InitBase):

    def __init__(self): 

        super().__init__('grant_gard_project_relation','Project')
        
        self.create_indexes('Project', ['application_id']) 
        self.create_indexes('PrincipalInvestigator', ['PI_id', 'PI_name']) 
 

    def create_investigator(self, pid, name, isContact, org_data):
        return {
            "PI_id": pid,
            "PI_name": name,
            "contact": 'YES' if isContact else 'NO',
            "org_city": org_data["city"],
            "org_country": org_data["country"],
            "org_dept": org_data["dept"],
            "org_district": org_data["district"],
            "org_duns": org_data["duns"],
            "org_fips": org_data["fips"],
            "org_name": org_data["org_name"],
            "org_state": org_data["state"],
            "org_zipcode": org_data["zipcode"]
        }


    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200):         

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  DISTINCT
                p.application_id,
                p.ORG_CITY, p.ORG_COUNTRY, p.ORG_DEPT, p.ORG_DISTRICT, p.ORG_DUNS, 
                p.ORG_FIPS, p.ORG_NAME, p.ORG_STATE, p.ORG_ZIPCODE,
                p.PI_IDS, p.PI_NAMES

                FROM  {self.table_name} gpr

                LEFT JOIN grant_project p
                ON gpr.application_id=p.application_id
 
                WHERE (gpr.id BETWEEN {start_id} AND {end_id}) 
            ''' 

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            #SELECT * FROM rdas_db.grant_project where PI_IDS is not null and PI_IDS like'%;%';
            #SELECT * FROM rdas_db.grant_project where PI_IDS is  null and PI_NAMEs like'%;%';
            #SELECT * FROM rdas_db.grant_project where PI_IDS is  null and PI_NAMEs is null;

            batch_chunks = []
            for row in rows:
                total += 1

                # Extract organization data
                org_data = {
                    "city": _val(row['ORG_CITY']),
                    "country": _val(row['ORG_COUNTRY']),
                    "dept": _val(row['ORG_DEPT']),
                    "district": _val(row['ORG_DISTRICT']),
                    "duns": _val(row['ORG_DUNS']),
                    "fips": _val(row['ORG_FIPS']),
                    "org_name": _val(row['ORG_NAME']),
                    "state": _val(row['ORG_STATE']),
                    "zipcode": _val(row['ORG_ZIPCODE'])
                }

                investigators = []
                pi_ids = _arr(row['PI_IDS']) or []  # Ensure empty list if None
                pi_names = _arr(row['PI_NAMES']) or []  # Ensure empty list if None

                if not pi_ids and not pi_names:
                    continue  # Skip rows with no investigators

                if not pi_ids:
                    # Only names provided
                    if len(pi_names) == 1:
                        investigators.append(self.create_investigator(None, pi_names[0], True, org_data))
                    else:
                        for name in pi_names:
                            isContact = False
                            if '(contact)' in (name or ''): 
                                isContact = True
                                name = name.replace('(contact)', '') if name else name
                                
                            investigators.append(self.create_investigator(None, name, isContact, org_data))                             
                else:
                    if len(pi_ids) == 1:
                        investigators.append(self.create_investigator(pi_ids[0], pi_names[0], True, org_data))
                    else:
                        for pid, name in zip(pi_ids, pi_names + [None] * (len(pi_ids) - len(pi_names))):
                            isContact = False
                            if '(contact)' in (pid or '') or '(contact)' in (name or ''):
                                isContact = True
                                pid = pid.replace('(contact)', '') if pid else pid
                                name = name.replace('(contact)', '') if name else name

                            investigators.append(self.create_investigator(pid, name, isContact, org_data))

                batch_chunks.append({
                    "application_id": row['application_id'],
                    "investigators": investigators
                })
            
            # Create Investigator and relationship to Project
            batch_create = '''
                UNWIND $batch_chunks AS chunk
                WITH chunk
                WHERE chunk.application_id IS NOT NULL
                UNWIND chunk.investigators AS investigator
                WITH chunk, investigator
                WHERE investigator.PI_name IS NOT NULL
                CALL {
                    WITH chunk, investigator
                    WHERE investigator.PI_id IS NOT NULL
                    MERGE (pi:PrincipalInvestigator {PI_id: investigator.PI_id})
                    ON CREATE SET pi = investigator
                    ON MATCH SET pi += investigator
                    RETURN pi
                UNION
                    WITH chunk, investigator
                    WHERE investigator.PI_id IS NULL
                    MERGE (pi:PrincipalInvestigator {PI_name: investigator.PI_name})
                    ON CREATE SET pi = investigator
                    ON MATCH SET pi += investigator
                    RETURN pi
                }
                WITH chunk, pi
                MERGE (p:Project {application_id: chunk.application_id})
                MERGE (pi)-[:INVESTIGATED]->(p)
            '''

            
            self.memgraph.execute(batch_create, {"batch_chunks": batch_chunks})   

            message = f'{_curr_timestamp()}\t[total: {total}], [Id range: [{start_id} - {end_id}], #Projects = {len(batch_chunks)}'

            print(f'PrincipalInvestigatorInitializer:: {message}\n')
            _append_to_file('logs/PrincipalInvestigatorInitializer.log', f'{message}')

        self._close_conn()

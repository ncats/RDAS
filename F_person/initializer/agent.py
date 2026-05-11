import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from colorama import init, Fore, Style
init()
 
from collections import defaultdict
from person_worker import PersonWorker
from baseclass.init_base import InitBase
from utils.file_appender import FileAppender
from utils.tools import _make_hash_key, _curr_timestamp, _date_string, _remove_parentheses, _clean

# Create Agent nodes and relationships
class AgentInitializer(InitBase):


    def __init__(self): 

        super().__init__('person_of_all_sources', 'Agent')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self.create_indexes('Agent', ['firstName', 'lastName', '_idx_key'])

    
    # Override the abstract method
    def init_nodes(self):   
        self.populate_nodes(None, None)


    # Override
    def populate_nodes(self, min_id, max_id, step=0, batch_size = 0):
      
        publication = 'Publication'
        grant_project = 'GrantProject'
        clinical_trial = 'ClinicalTrial'

        ''' Agent node properties: fullName, firstName, lastName, orc_id, pi_id, [contactEmail] '''
        # Relationship types must be literal strings - they cannot be dynamic variables or properties.
        # Can NOT use variable like: MERGE (ct)-[r: {{relation.relation_type}}]->(a)
        batch_create = f'''
            UNWIND $chunks AS chunk
            
            MERGE (a: Agent {{_idx_key: chunk._idx_key}})
            ON CREATE SET 
                a.fullName = chunk.fullName,
                a.firstName = chunk.firstName,
                a.lastName = chunk.lastName,
                a.orc_id = chunk.orc_id,
                a.pi_id = chunk.pi_id,
                a.contactEmail = chunk.contactEmail

            WITH a, chunk
            UNWIND chunk.relations AS relation
            
            CALL {{
                WITH a, relation
                WHERE relation.source = '{clinical_trial}' AND relation.relation_type = 'has_investigator'
                MERGE (ct: ClinicalTrial {{nctId: relation.nctId}})
                MERGE (ct)-[r:has_investigator]->(a) 
            }}

            CALL {{
                WITH a, relation
                WHERE relation.source = '{clinical_trial}' AND relation.relation_type = 'has_contact'
                MERGE (ct: ClinicalTrial {{nctId: relation.nctId}})
                MERGE (ct)-[r:has_contact]->(a) 
            }}
            
            CALL {{
                WITH a, relation
                WHERE relation.source = '{grant_project}' AND relation.relation_type = 'has_investigator'
                MERGE (p: Project {{applicationId: relation.applicationId}})
                MERGE (p)-[r:has_investigator]->(a) 
            }}

            CALL {{
                WITH a, relation
                WHERE relation.source = '{grant_project}' AND relation.relation_type = 'has_contact'
                MERGE (p: Project {{applicationId: relation.applicationId}})
                MERGE (p)-[r:has_contact]->(a) 
            }}
            
            CALL {{
                WITH a, relation
                WHERE relation.source = '{publication}'
                MATCH (t: Article {{pubmedId: relation.pubmedId}})
                MERGE (t)-[r:has_author]->(a) 
            }}
            
            WITH a, chunk
            UNWIND chunk.organizations AS org
            MERGE (o:Organization {{_idx_key: org._idx_key}})
            ON CREATE SET 
                o.name = org.name
            MERGE (a)-[:has_affiliation]->(o)            
        '''

        # init
        total = 0
        worker = PersonWorker()

        while True:

            ''' SELECT DISTINCT last_name  FROM rdas_db.person_of_all_sources  
                WHERE processed !='0_devZ_Agent'  ORDER BY last_name ASC  LIMIT 0,100; 
            '''
            last_names_list = worker.get_last_names_for_graph_update_2(self.processed_flag, batch_size = 100)

            if not last_names_list:
                self.appender.log_stdout(f"\n\n{_curr_timestamp()} {'#'*50} No more last names to process. {'#'*50}\n\n")

                break

            # example: ['Zhang', 'Zhao', 'Zhou', 'Zhu']           
            for original_last_name in last_names_list:

                # check special last_names 
                is_real_last_name = worker.normalize_last_name(original_last_name)

                if not is_real_last_name:
                    # update the processed flag with the ORIGINAL last_name
                    worker.update_processed_flag_for_graph_update(original_last_name, self.processed_flag)

                    self.appender.log_stdout(f'{Fore.RED}The last_name = {original_last_name} is invalid. Skip it.{Style.RESET_ALL}')
                    continue

                print(f"\n\n{'-'*100}")
                print(f'Last name: {original_last_name}')

                # fetch person with ORIGINAL last_name
                person_list = worker.fetch_person_by_last_name_for_graph_update(original_last_name, self.processed_flag)

                if not person_list:
                    continue
                    
                # Group the person which have same last_name by its rdas_group_id
                grouped_by_rdas_group_id = defaultdict(list)
                
                for person in person_list:  

                    total += 1
                    rdas_group_id = person['rdas_group_id']
                    grouped_by_rdas_group_id[rdas_group_id].append(person)

                # Convert to regular dict if needed
                grouped_by_rdas_group_id = dict(grouped_by_rdas_group_id)

                # Iterate the dic which keys are rdas_group_id's
                chunks = []

                for rdas_group_id, person_list in grouped_by_rdas_group_id.items():

                    # each person has a _idx_key which is a hashed value by rdas_group_id
                    # sample person will have same _idx_key
                    _idx_key = _make_hash_key(rdas_group_id)
                    
                    relations = []  
                    email_set = set()
                    affiliation_set = set()
                    first_name = last_name = full_name = orc_id = pi_id = ''                        

                    # person_list: list of persons who have the same rdas_group_id
                    for person in person_list:
                        
                        first_name = person.get('first_name', '')
                        if not first_name:
                            continue

                        original_last_name = person.get('last_name', '') 
                        # use normalized last_name in graph database
                        normalized_last_name = worker.normalize_last_name(original_last_name)

                        # "JOHN" -> "John", "mary" -> "Mary", "O'BRIEN" -> "O'Brien"
                        first_name = first_name.strip().title()
                        normalized_last_name = normalized_last_name.strip().title()                            

                        full_name = f'{first_name} {normalized_last_name}'

                        orc_id = _clean(person.get('orcid'))
                        pi_id = _clean(person.get('PI_id'))


                        ''' SELECT associate_type, source, role FROM rdas_db.person_of_all_sources group by associate_type, source, role order by source; '''
                        '''
                        associate_type,	source,	role
                        contact,	ClinicalTrial,	SUB_INVESTIGATOR
                        contact,	ClinicalTrial,	
                        contact,	ClinicalTrial,	CONTACT
                        PI,	        ClinicalTrial,	PRINCIPAL_INVESTIGATOR
                        PI,	        ClinicalTrial,	SPONSOR_INVESTIGATOR
                        contact,	ClinicalTrial,	STUDY_DIRECTOR
                        contact,	ClinicalTrial,	STUDY_CHAIR
                        contact,	ClinicalTrial,	PRINCIPAL_INVESTIGATOR
                        Grant_PI,	GrantProject,	contact
                        Grant_PI,	GrantProject,	
                        author,	    Publication,	author                            
                        '''
                        
                        associate_id = _clean(person.get('associate_id'))

                        # associate_type: 'author','Grant_PI','contact', 'PI'
                        associate_type = _clean(person.get('associate_type'))

                        # source: 'ClinicalTrial', 'GrantProject', 'Publication'
                        source = _clean(person.get('source'))

                        # role: 'author', 'contact', 'PRINCIPAL_INVESTIGATOR', 'SPONSOR_INVESTIGATOR', 'STUDY_CHAIR', 'STUDY_DIRECTOR', 'SUB_INVESTIGATOR'
                        role = _clean(person.get('role'))

                        # relations for ClincalTrial, Publication, CoreProject
                        if source == clinical_trial:
                            relation_type = 'has_investigator' if associate_type == 'PI' else 'has_contact'
                            relations.append({'nctId': associate_id, 'relation_type': relation_type, 'source': clinical_trial})

                        elif source == grant_project:
                            relation_type = 'has_contact' if role == 'contact' else 'has_investigator'
                            relations.append({'applicationId': associate_id, 'relation_type': relation_type, 'source': grant_project})

                        elif source == publication:
                            try:
                                pubmed_id = int(associate_id)
                                relations.append({'pubmedId': pubmed_id, 'relation_type': 'has_author', 'source': publication})
                            except (TypeError, ValueError):
                                self.appender.log_stdout(f'Invalid PubMed ID skipped: {associate_id}')
                    
                        # emails
                        email = _clean(person.get('email'))
                        email_set.add(email)

                        # organizations
                        ''' See B_clinical_trial/initializer/organization_location.py '''
                        affiliation = _clean(person.get('affiliation'))
                        affiliation_set.add(affiliation)

                    # organizations                                         
                    orgs = list(affiliation_set)
                    org_list = [
                        {
                            'name': org,
                            # _remove_parentheses(org)
                            # "National Eye Institute (NEI)" -> "National Eye Institute"
                            '_idx_key': _make_hash_key(_remove_parentheses(org))                             
                        } 
                        for org in orgs if org and org.strip()                     
                    ]

                        
                    # emails
                    emails = sorted(list(email_set))
                    
                    # chunks
                    chunk = {
                        '_idx_key': _idx_key,
                        'fullName': full_name,
                        'firstName': first_name,
                        'lastName': normalized_last_name,
                        'orc_id': orc_id,
                        'pi_id': pi_id,
                        'contactEmail': emails,
                        'organizations':org_list,
                        'relations': relations                            
                    }

                    chunks.append(chunk)
                    print(f'rdas_group_id = {rdas_group_id}')
 
                if chunks: 
                    try:                        
                        # Add timeout or check
                        self.memgraph.execute(batch_create, {"chunks": chunks})
                        
                    except Exception as e:  
                        self.appender.log_stdout(f'{Fore.RED}Exception while insert: {e}{Style.RESET_ALL}') 
                        # Don't let it silently fail - re-raise or continue
                        raise  # to stop on error
                    
                    finally:
                        # Always log what happened
                        self.appender.log_stdout(f'Finished processing chunks for last_name: {original_last_name}')

                # This should ALWAYS execute now
                worker.update_processed_flag_for_graph_update(original_last_name, self.processed_flag)
                self.appender.log_stdout(f'Last name: {original_last_name}, total: {total}, #Agents = {len(chunks)}') 

        worker.close_conn()                

        self.appender.log_stdout(f'\n\n{_curr_timestamp()} {"="*50} Done Total = {total} {"="*50}\n\n')
        self.appender.close()
        



    

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _format_dollars, _curr_timestamp, _time_hms, _date_string, _set_value_for_none


# 1. Initialize the Grant Project nodes
class ProjectInitializer(InitBase):

    def __init__(self): 

        super().__init__('grant_gard_project_relation_unique_application_id', 'Project')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/4-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
 
        self.create_indexes('Project', ['applicationId']) 


    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)

 
    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200):         

        # 
        batch_create = '''
            UNWIND $batch_chunks AS props
            MERGE (n:Project {applicationId: props.applicationId})
            ON CREATE SET 
                n = props 
        '''   

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)
        
        total = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  
                    gpru.id, 

                    p.application_id, p.application_type, p.project_title, p.project_terms,
                    p.ACTIVITY, p.FY, p.PHR, p.TOTAL_COST, p.SUPPORT_YEAR,
                    p.FOA_NUMBER, p.FULL_PROJECT_NUM, p.CORE_PROJECT_NUM, p.CFDA_CODE, p.SERIAL_NUMBER,
                    p.STUDY_SECTION, p.STUDY_SECTION_NAME, p.FUNDING_MECHANISM,

                    a.abstract_text

                FROM  {self.table_name} gpru  

                LEFT JOIN grant_project p
                ON gpru.application_id=p.application_id

                LEFT JOIN grant_abstract a
                ON gpru.application_id=a.application_id

                WHERE (gpru.id BETWEEN {start_id} AND {end_id}) AND (gpru.processed IS NULL OR gpru.processed != '{self.processed_flag}')
                
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            batch_chunks = []
            for row in rows:

                total += 1
                row = _set_value_for_none(row)

                total_cost = row['TOTAL_COST']
                
                batch_chunks.append({
                    "applicationId": row['application_id'], #convert to string
                    "abstract": row['abstract_text'],
                    "activity": row['ACTIVITY'],                    
                    "applicationType": row['application_type'],
                    "cfdaCode": row['CFDA_CODE'], 
                    "coreProjectNumber": row['CORE_PROJECT_NUM'],
                    "dateCreatedRDAS": _date_string(),
                    "foaNumber": row['FOA_NUMBER'],
                    "fullProjectNumber": row['FULL_PROJECT_NUM'],
                    "fundingMechanism": row['FUNDING_MECHANISM'],
                    "fundingYear": row['FY'], 
                    "phr": row['PHR'],
                    "serialNumber": row['SERIAL_NUMBER'],
                    "studySection": row['STUDY_SECTION'],
                    "studySectionName": row['STUDY_SECTION_NAME'],
                    "supportYear": row['SUPPORT_YEAR'],
                    "terms": row['project_terms'],
                    "title": row['project_title'],
                    "totalCost": _format_dollars(total_cost) if total_cost not in (None, '') and int(total_cost) > 0  else ''
                })

            
            if len(batch_chunks) > 0:
                self.memgraph.execute(batch_create, {"batch_chunks": batch_chunks}) 

            else:
                msg = f'{start_id} - {end_id} has no rows'
                self.appender.log_stdout(msg)  

            self.update_processed_flag(start_id, end_id, self.processed_flag)

            self.appender.log_stdout(f'{_curr_timestamp()}\t[total: {total}], [flag={self.processed_flag}], [Id range: [{start_id} - {end_id}], #Projects = {len(batch_chunks)}')


        self.close_mysql_conn() 
        
        self.appender.log_stdout(f'\n{"="*50} Done Total = {total} {"="*50}\n\n')


        '''
        # Create indexes here
        start_time = time.time()
        self.appender.log_stdout(f'\n{"="*50} \'CREATE INDEX ON :Project(application_id)\' after uploaing the data {"="*50}\n')

        self.create_indexes('Project', ['applicationId']) 

        self.appender.log_stdout(f'\n{"*"*30} {_curr_timestamp()} The index on application_id has been created {"*"*30}\n')

        # Log the total time elapsed
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, minutes, seconds = _time_hms(elapsed_time)

        self.appender.log_stdout(f'{"="*50} {_curr_timestamp()} All done! Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds {"="*50}\n\n') 
        ''' 
        self.appender.close()


       




            
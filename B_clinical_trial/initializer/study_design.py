import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _id_range_generator, _clean, _curr_timestamp, _date_string

# Create StudyDesign nodes
class StudyDesignInitializer(InitBase):


    def __init__(self): 

        super().__init__('clinical_trial_unique', 'StudyDesign')
        
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/2-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)    


    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size=300):

        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (x: ClinicalTrial {nctId: chunk.nctId}) 
            CREATE (y:StudyDesign)
            SET
                y.designAllocation = chunk.allocation,
                y.designInterventionModel = chunk.interventionModel,
                y.designInterventionModelDescription = chunk.interventionModelDescription,
                y.designMasking = chunk.masking,
                y.designObservationalModel = chunk.observationalModel,
                y.designPrimaryPurpose = chunk.primaryPurpose,
                y.designTimePerspective = chunk.timePerspective,
                y.detailedDescription = chunk.description,
                y.hasExpandedAccess = chunk.hasExpandedAccess,
                y.studyType = chunk.studyType               

            MERGE (x)-[:has_study_design]->(y) 
        '''                  
        
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        total  = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT id, nctid, studies  
                FROM {self.table_name}
                WHERE nctid IS NOT NULL AND (id BETWEEN {start_id} AND {end_id})
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = [] 

            for row in rows:

                total += 1
                nctid = row['nctid'] 
                study = json.loads(row['studies'])
 
                design_module = study.get('protocolSection', {}).get('designModule', {})
                desc_module = study.get('protocolSection', {}).get('descriptionModule', {})
                status_module = study.get('protocolSection','').get('statusModule',{})
                
                designInfo = design_module.get('designInfo', {})                
                maskingInfo = designInfo.get('maskingInfo',{}) 
                expandedAccessInfo = status_module.get('expandedAccessInfo',{}) 

                if not (designInfo or maskingInfo or expandedAccessInfo):
                    continue

                chunks.append(
                    {   
                        "nctId": nctid,
                        "studyType": design_module.get('studyType', ''),
                        "observationalModel":  _clean(designInfo.get('observationalModel','')),
                        "interventionModel":  _clean( designInfo.get('interventionModel','')),
                        "interventionModelDescription":  _clean(designInfo.get('interventionModelDescription','')),
                        "timePerspective":  _clean(designInfo.get('timePerspective','')), 
                        "allocation":  _clean(designInfo.get('allocation','')),
                        "primaryPurpose":  _clean(designInfo.get('primaryPurpose','')),
                        "masking":  _clean(maskingInfo.get('masking','')),
                        "description":  _clean(desc_module.get('detailedDescription','')),
                        "hasExpandedAccess":  _clean(expandedAccessInfo.get('hasExpandedAccess',''))
                    }
                )
                 
            if chunks: 
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks})                    

                except Exception as e:
                    self.appender.log_stdout(f"Error executing batch create: {e}")  
                    raise
            
            self.update_processed_flag(start_id, end_id, self.processed_flag)
            
            self.appender.log_stdout(f'{_curr_timestamp()} [total: {total}], Id range: [{start_id} - {end_id}], #StudyDesign = {len(chunks)}')


        self.close_mysql_conn()  
  
        self.appender.log_stdout(f'\n{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close()
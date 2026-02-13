import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import time
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _id_range_generator, _clean, _curr_timestamp, _date_string

# Create Participant nodes
class ParticipantInitializer(InitBase):

    def __init__(self): 

        super().__init__('clinical_trial_unique','Participant')

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
            CREATE (y: Participant)
            SET
                y.eligibilityCriteria = chunk.eligibilityCriteria,
                y.healthyVolunteers = chunk.healthyVolunteers,
                y.stdAges = chunk.stdAges,
                y.minimumAge = chunk.minimumAge,
                y.maximumAge = chunk.maximumAge,
                y.enrollmentCount = chunk.enrollmentCount,
                y.enrollmentType = chunk.enrollmentType

            MERGE (x)-[:has_participant_info]->(y) 
        '''                  
        
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        total  = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT id, nctid, studies  FROM {self.table_name}
                WHERE nctid IS NOT NULL 
                AND id BETWEEN {start_id} AND {end_id}
            '''

            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = [] 
            for row in rows:
                total += 1
                nctid = row['nctid'] 
                study = json.loads(row['studies'])
 
                design_module = study.get('protocolSection', {}).get('designModule', {})
                '''
                "designModule": {
                    ........
                    "enrollmentInfo": {
                        "count": 301,
                        "type": "ACTUAL"
                    }
                }
                '''

                eligibility_module = study.get('protocolSection', {}).get('eligibilityModule', {})
                '''
                "eligibilityModule": {
                    "eligibilityCriteria": "Inclusion Criteria:\n\n* Male and female subjects who the investigator believes that their parent(s)/ Legally Acceptable Representative (LAR(s)) can and will comply with the requirements of the protocol.\n* Written or oral, signed or thumb-printed and witnessed informed consent obtained from the subject's parent(s)/LAR(s).\n* Subjects who received their birth dose of Bacille Calmette Guerrin.\n* Healthy subjects as established by medical history and clinical examination before entering into the study.\n\nFor the 'Outside Expanded Programme on Immunisation' cohort:\n\n* Must have documented evidence that he/she has completed the primary Expanded Programme on Immunisation regimen at least 1 month prior to planned vaccination with investigational vaccination regimen.\n* Aged between 5 and 7 months at the time of the first study vaccination.\n\nFor the 'Within EPI' cohort:\n\n* Must have received the birth dose of Bacille Calmette Guerrin, oral polio vaccine and Hepatitis B vaccine but NO further Expanded Programme on Immunisation vaccines.\n* Aged between 2 and 4 months at the time of the first study vaccination with diphtheria, tetanus, whole cell pertussis/ Haemophilus influenzae type b vaccine + pneumococcal conjugate vaccine + oral polio vaccine.\n\nExclusion Criteria:\n\n* Child in care\n* Acute or chronic, clinically significant pulmonary, cardiovascular, hepatic or renal abnormality, as determined by physical examination and/or laboratory screening tests.\n* Laboratory screening tests out of range, which in the investigator's opinion affects the ability of the child to take part in the study.\n* Any confirmed or suspected immunosuppressive or immunodeficient condition, based on medical history and physical examination.\n* A family history of congenital or hereditary immunodeficiency.\n* Major congenital defects.\n* History of any neurological disorders or seizures.\n* Any condition or illness or medication, which in the opinion of the investigator might interfere with the evaluation of the safety or immunogenicity of the study vaccine.\n* Any other findings that the investigator feels would increase the risk of having an adverse outcome from participation in the trial.\n* Acute disease and/or fever at the time of enrolment.\n* Use of any investigational or non-registered product other than the study vaccines within 30 days preceding the first dose of study vaccine, or planned use during the study period.\n* For the 'Within Expanded Programme on Immunisation' Cohort only: Previous vaccination with diphtheria, tetanus, pertussis, Haemophilus influenzae type b and pneumococcal conjugate vaccine.\n* History of previous administration of experimental Mycobacterium tuberculosis vaccines.\n* Administration of immunoglobulins, blood transfusions and/or other blood products since birth to the first dose of study vaccine or planned administration during the study period.\n* Chronic administration of immunosuppressants or other immune-modifying drugs within six months prior to the first vaccine dose.\n* Planned participation or concurrently participating in another clinical study at any time during the study period, in which the subject has been or will be exposed to an investigational or a non-investigational product.\n* Any chronic drug therapy to be continued during the study period, with the exception of vitamins and/or dietary supplements\n* History of allergic reactions or anaphylaxis to any vaccine.\n* History of any reaction or hypersensitivity likely to be exacerbated by any component of the study vaccines.\n* Severe malnutrition at screening defined as weight-for-age Z-score \\< -3 standard deviation.\n* Children will not be enrolled if any maternal, obstetrical or neonatal event that has occurred might, in the judgment of the investigator, result in increased neonatal/infant morbidity.",
                    "healthyVolunteers": true,
                    "sex": "ALL",
                    "minimumAge": "2 Months",
                    "maximumAge": "7 Months",
                    "stdAges": [
                        "CHILD"
                    ]
                }
                '''
                if not (eligibility_module  or design_module):
                    return None 

                chunks.append(
                    {   
                        "nctId": nctid, 
                        "eligibilityCriteria": _clean(eligibility_module.get('eligibilityCriteria', '')),
                        "healthyVolunteers":  _clean(eligibility_module.get('healthyVolunteers', '')),
                        "stdAges":  eligibility_module.get('stdAges', []),
                        "minimumAge":  _clean(eligibility_module.get('minimumAge', '')), 
                        "maximumAge":  _clean(eligibility_module.get('maximumAge', '')),
                        "enrollmentCount": _clean(design_module.get('enrollmentInfo', dict()).get('count', '')),
                        "enrollmentType":  _clean(design_module.get('enrollmentInfo', dict()).get('type', '')) 
                    }
                )
                            
            if chunks:          

                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks}) 
                
                except Exception as e:
                    self.appender.log_stdout(f"Error executing batch create: {e}")
                    raise

            self.update_processed_flag(start_id, end_id, self.processed_flag)
            
            self.appender.log_stdout(f'{_curr_timestamp()} [total: {total}], Id range: [{start_id} - {end_id}], #Participant = {len(chunks)}')

        self.close_mysql_conn()   

        self.appender.log_stdout(f'\n{_curr_timestamp()} {"="*50} Done! Total = {total} {"="*50}\n\n')
        self.appender.close()
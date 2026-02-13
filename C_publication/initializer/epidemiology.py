import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import hashlib
from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _set_value_for_none, _date_string

# 1. Create EpidemiologyAnnotation nodes
class EpidemiologyAnnotationInitializer(InitBase):


    def __init__(self): 

        super().__init__('publication_article', 'EpidemiologyAnnotation')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        # Create index on pubmedId if it doesn't exist
        self.create_indexes('Article', ['pubmedId']) 

        self.create_indexes('EpidemiologyAnnotation',['epidemiologyType', 'studyLocation', 'ethnicity', '_composite_key'])
 

    # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=3, batch_size = 1000): 

        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (a: Article {pubmedId: chunk.pubmedId})
            MERGE (n: EpidemiologyAnnotation {_composite_key: chunk._composite_key})
            ON CREATE SET
                n.epidemiologyType = chunk.epidemiologyType,
                n.epidemiologyRate = chunk.epidemiologyRate,
                n.studyDate = chunk.date,
                n.studyLocation = chunk.location,
                n.ethnicity = chunk.ethnicity,
                n.sex = chunk.sex,
                n.dateCreatedByRDAS = chunk.dateCreatedByRDAS, 
                n.lastUpdatedByRDAS = chunk.lastUpdatedByRDAS
            MERGE (a) -[r:has_epidemiological_annotation {epidemiology_probability: chunk.epiProbability}]-> (n)
        '''
  
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        _count = 0
        for start_id, end_id in id_ranges:
            
            query = f'''
                    SELECT  id, pubmed_id, is_epi, epi_probability, epi_extract
                    FROM  {self.table_name}
                    WHERE (id BETWEEN {start_id} AND {end_id}) 
                    AND is_epi=1 
                    AND (processed is null OR processed != \'{self.processed_flag}\')
                '''
            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = []
            for row in rows: 

                _count += 1
                pubmed_id = row['pubmed_id']
                epi_extract = row['epi_extract']

                # realtionship property: epidemiology_probability
                epi_probability = str(row['epi_probability'])
                # gqlalchemy.exceptions.GQLAlchemyDatabaseError: value of type 'decimal.Decimal' can't be used as query parameter

                if not epi_extract:
                    continue

                epiObj = json.loads(epi_extract)

                # Get all the fields
                epidemiology_type = epiObj['EPI'] or []
                epidemiology_rate = epiObj['STAT'] or []
                study_date = epiObj['DATE'] or []
                study_location = epiObj['LOC'] or []
                ethnicity = epiObj['ETHN'] or []
                sex = epiObj['SEX'] or []

                # Create composite key string from all fields
                composite_key_str = f"{'_'.join(sorted(epidemiology_type))}_{'_'.join(sorted(epidemiology_rate))}_{'_'.join(sorted(study_date))}_{'_'.join(sorted(study_location))}_{'_'.join(sorted(ethnicity))}_{'_'.join(sorted(sex))}"
                composite_key_str = "_".join(composite_key_str.split())  # Replace whitespaces

                # Hash the composite key
                composite_key_hash = hashlib.sha256(composite_key_str.encode()).hexdigest()

                chunks.append({
                    'pubmedId': pubmed_id,
                    'epiProbability': epi_probability,
                    "epidemiologyType": epidemiology_type,
                    "epidemiologyRate": epidemiology_rate,
                    "date": study_date,
                    "location": study_location,
                    "ethnicity": ethnicity,
                    "sex": sex,
                    "_composite_key": composite_key_hash,
                    "dateCreatedByRDAS": self.formatted_today,
                    "lastUpdatedByRDAS": self.formatted_today
                })

            if chunks:
                try:
                    self.memgraph.execute(batch_create, {"chunks": chunks})    
                except Exception as e:
                    self.appender.append_and_print(f'Exception while insert: {e}')
                    raise 

            self.update_processed_flag(start_id, end_id, self.processed_flag)

            self.appender.log_stdout(f'{_curr_timestamp()} [total: {_count}], [flag: {self.processed_flag}], Id range: [{start_id} - {end_id}] #EpidemiologyAnnotation = {len(chunks)}')
            
        self.close_mysql_conn() 

        self.appender.log_stdout(f'\n{"="*50}{_curr_timestamp()} Done! Total = {_count} {"="*50}\n')
        self.appender.close()
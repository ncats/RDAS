import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _date_string
 
#
# GARD - Article relationship
#
class GARDToArticleRelationshipInitializer(InitBase):


    def __init__(self): 
        
        super().__init__('publication_gard_searchterm_pubmed_mapping', 'GARD - Publication relationship')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)

        self.create_indexes('GARD', ['gardId'])
        self.create_indexes('Article', ['pubmedId'])
 

    #Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200): 
      
        # SHOW INDEX INFO;
        batch_create = '''                
            UNWIND $relations AS rel
            MATCH(p: Article {pubmedId: rel.pubmedId})
            MATCH (g: GARD {gardId: rel.gardId})
            MERGE (g)-[:has_mention_in]->(p)
        '''

        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        _count = 0
        for start_id, end_id in id_ranges:

            query = f'''
                SELECT  id, gard_id, pubmed_id
                FROM  {self.table_name}
                WHERE (id BETWEEN {start_id} AND {end_id}) 
                AND (is_valid is null OR is_valid = 1) 
                AND (processed IS NULL OR processed != \'{self.processed_flag}\') 
            '''
           
            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            relations = []
            for row in rows:

                _count += 1
                gard_id = row['gard_id']
                pubmed_id = row['pubmed_id'] 

                relations.append({"gardId": gard_id, "pubmedId": pubmed_id})
 
            if relations:
                try:
                    self.memgraph.execute(batch_create, {"relations": relations})   

                except Exception as e:
                    self.appender.append_and_print(f'Exception while insert: {e}')
                    raise 

            self.update_processed_flag(start_id, end_id, self.processed_flag)
 
            self.appender.log_stdout(f'{_curr_timestamp()} [total: {_count}],[flag: {self.processed_flag}], Id range: {start_id} - {end_id}, #GARD - Publication relationship = {len(relations)}')


        self.close_mysql_conn()  

        self.appender.log_stdout(f'\n{"="*50}{_curr_timestamp()} Done! Total = {_count} {"="*50}\n')
        self.appender.close()

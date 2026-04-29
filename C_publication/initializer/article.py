import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.init_base import InitBase
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import _id_range_generator, _curr_timestamp, _set_value_for_none, _date_string


# --- Delete duplicate rows before running the PublicationInitializer.     (Do NOT delete this sql script) ---
'''
USE rdas_db;

DELETE t1 FROM rdas_db.publication_article AS t1
WHERE EXISTS (
    SELECT 1
    FROM rdas_db.publication_article AS t2
    WHERE t2.pubmed_id = t1.pubmed_id
      AND t2.id < t1.id
);

'''
 
# 1. Create Article nodes
class ArticleInitializer(InitBase):


    def __init__(self): 

        super().__init__('publication_article', 'Article')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
        
        self.create_indexes('Article', ['pubmedId', 'publicationYear'])

     # Override the abstract method
    def init_nodes(self):   

        min_id, max_id = MinMaxIdLoader().get_min_max_ids_by_flag(self.table_name, self.processed_flag) 
        print(f'populate_nodes_by_flag: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)


    def _1_true(self, value):
        return True if value =='1' else False


    # Override
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 300):    

        batch_create =  '''
            UNWIND $chunks AS chunk
            MERGE (a: Article {pubmedId: chunk.pubmedId})
            ON CREATE SET
                a.doi = chunk.doi,
                a.title = chunk.title,
                a.abstractText = chunk.abstractText,
                a.firstPublicationDate = chunk.firstPublicationDate,
                a.publicationYear = chunk.publicationYear,
                a.citationCount = chunk.citationCount,
                a.isOpenAccess = chunk.isOpenAccess,
                a.inEPMC = chunk.inEPMC,
                a.inPMC = chunk.inPMC,
                a.isEpidemiologicalStudy = chunk.isEpidemiologicalStudy,
                a.isNaturalHistoryStudy = chunk.isNaturalHistoryStudy,
                a.hasPDF = chunk.hasPDF,
                a.pubType = chunk.pubType,
                a.dateCreatedByRDAS = chunk.dateCreatedByRDAS,
                a.lastUpdatedDateByRDAS = chunk.lastUpdatedDateByRDAS,
                a.fullTextUrls = chunk.fullTextUrls,
                a.issue = chunk.issue,
                a.volume = chunk.volume,
                a.isGeneReview = false            
        '''

        # Handle the duplicates --- DON NOT DELETE
        '''
        batch_create =  """
            UNWIND $chunks AS props
            MERGE (n: Article {pubmedId: props.pubmedId})
            ON CREATE SET n = props,
                        n.dateCreatedByRDAS = props.dateCreatedByRDAS,
                        n.lastUpdatedDateByRDAS = props.lastUpdatedDateByRDAS
            ON MATCH SET n += props,
                        n.lastUpdatedDateByRDAS = props.lastUpdatedDateByRDAS
        """
        '''
        
        search_source = 'Pubmed' # Default, the data is from pubmed
        id_ranges = _id_range_generator(min_id, max_id, step, batch_size)

        _count  = 0

        for start_id, end_id in id_ranges:        
                
            query = f'''
                SELECT 
                    id, pubmed_id, doi, title, abstract_text,
                    first_publication_date, publication_year, cited_by_count, is_open_access,
                    in_EPMC, in_PMC, is_EPI, is_NHS, has_PDF, pub_type
                FROM  {self.table_name}
                WHERE (id BETWEEN {start_id} AND {end_id}) 
                AND (processed IS NULL or processed !=\'{self.processed_flag}\')
            '''
            self.dict_cursor.execute(query)
            rows = self.dict_cursor.fetchall()

            chunks = [] 

            for row in rows:

                _count += 1
                row = _set_value_for_none(row)
                 
                chunks.append({
                    "pubmedId": int(row['pubmed_id']),
                    "doi": row['doi'],
                    "title": row['title'],
                    "abstractText": row['abstract_text'],
                    "firstPublicationDate": row['first_publication_date'],
                    "publicationYear": row['publication_year'],
                    "citationCount": row['cited_by_count'],
                    "isOpenAccess": self._1_true(row['is_open_access']),
                    "inEPMC": self._1_true(row['in_EPMC']),
                    "inPMC": self._1_true(row['in_PMC']),
                    "isEpidemiologicalStudy": self._1_true(row['is_EPI']),
                    "isNaturalHistoryStudy": self._1_true(row['is_NHS']),
                    "hasPDF": self._1_true(row['has_PDF']),
                    "pubType": row['pub_type'],                    
                    "dateCreatedByRDAS": self.formatted_today,
                    "lastUpdatedDateByRDAS": self.formatted_today,

                    # Will updated by articl_attrs.py later
                    "fullTextUrls": [], 
                    "issue": "",
                    "volume": ""
                })

            try:
                self.memgraph.execute(batch_create, {"chunks": chunks}) 
            except Exception as e:  
                self.appender.log_stdout(f'Exception while insert: {e}')

            self.update_processed_flag(start_id, end_id, self.processed_flag)  
            
            self.appender.log_stdout( f'{_curr_timestamp()} [total: {_count}], [flag: {self.processed_flag}], Id range: [{start_id} - {end_id}], #Articles = {len(chunks)}')


        self.close_mysql_conn() 


        # Create indexes on properties ['pubmedId', 'title'] after initialization
        self.appender.log_stdout(f'\n{"-"*30} {_curr_timestamp()} Create indexes on properties ["pubmedId", "title"] after initialization {"-"*30}\n')  

        self.create_indexes('Article', ['pubmedId', 'title']) 

        self.appender.log_stdout(f'{_curr_timestamp()} Indexes have been created.\n')

        self.appender.log_stdout(f'\n{"*"*30} {_curr_timestamp()} All done! Total = {_count} {"*"*30}\n\n') 
        self.appender.close()


            
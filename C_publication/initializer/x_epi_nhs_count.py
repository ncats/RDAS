import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.init_base import InitBase
from utils.file_appender import FileAppender
from utils.tools import _curr_timestamp, _date_string 
 

''' 
Update GARD nodes with countEpiArticles & countNhsArticles
'''
class EpiAndNhsCountsInitializer(InitBase):

    def __init__(self): 

        super().__init__('publication_article', 'publication EPI and NHS counts')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/3-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    def init_nodes(self):   

        cypher = """
            UNWIND $chunks AS chunk
            MATCH (d:GARD {gardId: chunk.gardId})
            SET 
                d.countEpiArticles = chunk.countEpiArticles,
                d.countNhsArticles = chunk.countNhsArticles
        """

        try:
            # query the mysql database with batch size 300
            batch_size = 300
            get_gard_ids_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            get_counts_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            
            query = 'SELECT  distinct gard_id from publication_gard_searchterm_pubmed_mapping'
            
            get_gard_ids_cursor.execute(query)
            
            total = 0
            while True:

                rows = get_gard_ids_cursor.fetchmany(batch_size)
 
                if not rows:
                    break

                gard_ids = [row['gard_id'] for row in rows]
                placeholders = ",".join(["%s"] * len(gard_ids))

                get_counts_query = f'''
                    SELECT
                        pgspm.gard_id,
                        COUNT(a.pubmed_id)              AS total_articles,
                        SUM(a.is_EPI = 1)               AS countEpiArticles,
                        SUM(a.is_NHS = 1)               AS countNhsArticles
                    FROM publication_gard_searchterm_pubmed_mapping pgspm
                    LEFT JOIN publication_article a
                        ON pgspm.pubmed_id = a.pubmed_id
                    WHERE pgspm.gard_id IN ({placeholders})
                    GROUP BY pgspm.gard_id
                '''
    
                get_counts_cursor.execute(get_counts_query, gard_ids)
                results = get_counts_cursor.fetchall()  

                chunks = []

                for result in results:
                    gard_id = result['gard_id']
                    total_articles = result['total_articles']
                    countEpiArticles = result['countEpiArticles']
                    countNhsArticles = result['countNhsArticles']

                    chunks.append({
                        "gardId": gard_id,
                        #"totalArticles": int(total_articles or 0),
                        "countEpiArticles": int(countEpiArticles or 0),
                        "countNhsArticles": int(countNhsArticles or 0)
                    })
                    self.appender.log_stdout(f'gard_id: {gard_id}, total_articles: {total_articles}, countEpiArticles: {countEpiArticles}, countNhsArticles: {countNhsArticles}')

                if chunks:
                    total += len(chunks) 
                    try:
                        self.memgraph.execute(cypher, {"chunks": chunks})
                        self.appender.log_stdout(f'Total = {total}, batch updated memgraph successfully\n')
                    
                    except Exception as e:
                        self.appender.log_stdout(f"Batch insert into Memgraph failed: {e}") 
                        raise

        except Exception as e:
            self.appender.log_stdout(f"Batch insert into Memgraph failed: {e}") 
            raise
        finally: 
            get_gard_ids_cursor.close()
            get_counts_cursor.close()
            self.close_mysql_conn()  

        self.appender.log_stdout(f'\n{"="*50}{_curr_timestamp()} Total GARD IDs = {total}, Done! {"="*50}\n')
        self.appender.close()  

        
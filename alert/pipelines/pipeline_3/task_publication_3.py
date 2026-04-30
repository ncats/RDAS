import os
import sys 
_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Retrieve GARD id & OMIM id from gard table, and insert into table publication_gard_omim_mapping.

If there are no changes in GARD table for Label_Xref field, so no [gard_id - omim_id] pairs should be added into publication_gard_omim_mapping table
"""
# Reference: C_publication/init_5_publication-gard-omim-mapping.py

class PublicationTask_3(PipelineBase):

    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=False)


    # Not implemented
    def find_new_data(self) -> None:

        raise NotImplementedError("PublicationTask_3 does not implement find_new_data().")
   

    # implement
    def process_new_data(self) -> None:
        
        fetch_query = 'SELECT GardID, group_concat(Label_Xref) AS xrefs FROM gard GROUP BY gardid ORDER BY GardID'

        insert_sql = '''
            INSERT INTO publication_gard_omim_mapping (gard_id, omim_id, is_new)
            SELECT %s, %s, 1
            WHERE NOT EXISTS (
                SELECT 1
                FROM publication_gard_omim_mapping
                WHERE gard_id = %s
                AND omim_id = %s
            )
        '''

        count = 0
        batch_num = 0
        batch_size = 500

        insert_cursor = None
        fetch_cursor = None

        try:
            
            insert_cursor = self.mysql.cursor()
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)

            fetch_cursor.execute(fetch_query)
             
            while True:

                rows = fetch_cursor.fetchmany(batch_size)

                batch_num += 1
                self.logger.info(f'\n--- batch# = {batch_num} ---')

                if not rows:
                    self.logger.info(f"No more rows to fetch.")
                    break


                val_list = []
                
                for row in rows:

                    gard_id = row['GardID']
                    xrefs = row['xrefs']                   

                    if xrefs and 'OMIM' in xrefs:
                        
                        omims = [item.split(':')[1] for item in xrefs.split(',') if item.strip().startswith('OMIM')]
                        unique_omims = list(set(omims))
                        
                        for omim in unique_omims:

                            if not omim.isdigit():
                                continue

                            val_list.append((gard_id, omim, gard_id, omim))


                if len(val_list) > 0:
                    insert_cursor.executemany(insert_sql, val_list)
                    self.mysql.commit()

                    #log
                    rowcount = insert_cursor.rowcount
                    count += rowcount
                    self.logger.info(f"{rowcount} [gard_id - omim_id] pairs have been added into publication_gard_omim_mapping table.")

            if count > 0:
                self.logger.info(f"{count} [gard_id - omim_id] pairs have been added into publication_gard_omim_mapping table.")
            else:
                self.logger.info('No changes in GARD table for Label_Xref field.\nSo, no [gard_id - omim_id] pairs have been added into publication_gard_omim_mapping table')

        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            if insert_cursor:
                insert_cursor.close()

            # Explicitly close the all the db connections
            self.close()

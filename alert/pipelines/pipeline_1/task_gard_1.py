import os
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterator, List, Sequence

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase


class GardNodeNamesTask(PipelineBase):

 
    def __init__(self):

        super().__init__(init_mysql=True, init_memgraph=False)
      
        self.SYNONYM_SEPARATOR = "$$$"


    # Not implemented
    def find_new_data(self) -> None:
        raise NotImplementedError("GardNodeNamesTask does not implement find_new_data().")
    
    def process_new_data(self, gard_node) -> None:
        raise NotImplementedError("GardNodeNamesTask does not implement process_new_data().")

    '''
    Generator
    Fetch GARD id, names and synonyms from MySQL in batches.
    Default batch size is 100.
    '''
    def get_gard_nodes(self, batch_size: int = 100) -> Iterator[List[Dict[str, Any]]]:
        """
        Fetch GARD names and synonyms from MySQL in batches.
        """ 

        total = 0
        # Remove 'updated IS NULL' for PRODUCTION
        GARD_QUERY = """
            SELECT 
                GardID, MONDO_ID, 
                MAX(CASE WHEN Label_Predicate_Type = 'Name' THEN Label END) AS gardName,
                GROUP_CONCAT(CASE WHEN Label_Predicate_Type = 'Synonym' THEN Label END SEPARATOR '$$$') AS `synonyms`,
                Label_Source, 
                MIN(updated) AS min_updated
            FROM  gard
            WHERE 
                -- for PRODUCTION
                -- (updated IS NULL OR updated != CURDATE())

                -- For testing/init, remove for production
                updated IS NULL

                AND Label_Predicate_Mapping != 'DEPRECATED' 
                AND LENGTH(Label) > 3
            GROUP BY 
                GardID, MONDO_ID, Label_Source
            ORDER BY GardID ASC, MONDO_ID ASC, Label_Source ASC
            LIMIT %s
        """

        while True:

            cursor = self.mysql.cursor(dictionary=True)
            cursor.execute(GARD_QUERY, (batch_size,))
            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                self.logger.info(f"{datetime.now().strftime('%Y-%m-%d')}: No more rows to fetch.")
                break

            batch = []
            gard_id_set = set()
            total += len(rows)

            for row in rows:
                gardId = row["GardID"] 
                gard_id_set.add(gardId) 

                batch.append({
                    "gardId": gardId,
                    "gardName": row["gardName"],
                    "synonyms": self._split_values(row["synonyms"], self.SYNONYM_SEPARATOR),
                    "updated": row["min_updated"],
                })

            # log
            self.logger.info(f'\n---Fetched # of GARD IDs: {total} ---\n')
            
            for item in batch:
                self.logger.info(f"{item}")

            ''' Update the 'updated' column in rdas_db.gard table with value CURDATE() '''
            self._set_updated_flag(gard_id_set)

            yield batch

        # close all 
        self.close()



    """ Set the 'updated' column in rdas_db.gard table with value CURDATE() """
    def _set_updated_flag(self, gard_id_set):
        """
        Mark all rows for a set of GardIDs as updated.
        """
        if not gard_id_set:
            return

        gard_ids_list = list(gard_id_set)             
        placeholders = ",".join(["%s"] * len(gard_ids_list))

        update_sql = """
            UPDATE gard
            SET updated = CURDATE()
            WHERE GardID IN ({placeholders})
        """.format(placeholders = placeholders)
        
        cursor = self.mysql.cursor()
        cursor.execute(update_sql, gard_ids_list)
        self.mysql.commit()



    ''' utility method '''    
    def _split_values(self, value: str, separator: str = ",") -> List[str]:
        if not value:
            return []

        return [item for item in value.split(separator) if item]

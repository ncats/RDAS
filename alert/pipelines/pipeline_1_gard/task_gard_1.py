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
from utils.tools import _is_english, _is_under_char_threshold

class GardNodeNamesTask(PipelineBase):
    """
    Read GARD disease names that should drive downstream alert searches.

    This task is the first discovery step: it pulls GARD IDs, preferred names,
    and synonyms from MySQL, filters the names into search-friendly terms, and
    marks the processed GARD rows as updated.
    """

 
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

        # check the Label_Predicate_Mapping values
        '''
        SELECT
            ROW_NUMBER() OVER (ORDER BY Label) AS `#`,
            g.*
        FROM rdas_db.gard AS g
        WHERE LENGTH(g.Label) <= 4
        AND g.Label_Predicate_Mapping != 'ABBREVIATION'
        AND g.Label_Predicate_Mapping != 'DEPRECATED'
        AND g.Label_Predicate_Mapping != 'AMBIGUOUS'
        ORDER BY g.Label
        LIMIT 0, 1000;
        '''
        total = 0 
 
        #The query returns one row per GARD/MONDO/source group and combines all synonym labels into a single separated string for local filtering. 

        GARD_QUERY = """
            SELECT id,
                GardID, MONDO_ID, 
                MAX(CASE WHEN Label_Predicate_Type = 'Name' THEN Label END) AS gardName,
                GROUP_CONCAT(CASE WHEN Label_Predicate_Type = 'Synonym' THEN Label END SEPARATOR '$$$') AS `synonyms`,
                Label_Source, 
                MIN(updated) AS min_updated
            FROM  gard
            WHERE  
                (updated IS NULL OR updated != CURDATE())

                AND Label_Predicate_Mapping != 'DEPRECATED' 
                AND Label_Predicate_Mapping != 'AMBIGUOUS'
                AND LENGTH(Label) > 3
            GROUP BY 
                GardID, MONDO_ID, Label_Source
            ORDER BY GardID ASC, MONDO_ID ASC, Label_Source ASC
            LIMIT %s
        """

        while True:

            # Each loop returns one batch of GARD rows, then marks those GardIDs as updated before yielding to the caller.
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
                id = row["id"]                
                gardId = row["GardID"] 
                gard_id_set.add(gardId) 

                gardName = row["gardName"]
                synonyms = self._split_values(row["synonyms"], self.SYNONYM_SEPARATOR)

                # filtered_names is the disease-name list used by clinical trial and publication discovery.
                batch.append({
                    "id": id,
                    "gardId": gardId,
                    "gardName": gardName,
                    "synonyms": synonyms,
                    "updated": row["min_updated"],
                    "filtered_names": self._get_filtered_gard_names(gardName, synonyms)
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




    def _get_filtered_gard_names(self, name, synonyms) -> list:
        """Build the disease search names used by the first trial/publication tasks."""

        # Keep English synonyms, but skip very short synonym strings because they tend to create noisy publication/trial searches.
        english_synonyms = [syn for syn in synonyms if _is_english(syn)]
        short_synonyms = [syn for syn in synonyms if _is_under_char_threshold(syn)]

        filtered_synonyms = [syn for syn in synonyms if syn in english_synonyms]
        filtered_synonyms = [syn for syn in filtered_synonyms if syn not in short_synonyms]

        return [name] + filtered_synonyms


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
        """Split grouped database values and discard empty parts."""

        if not value:
            return []

        return [item for item in value.split(separator) if item]

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
from utils.tools import _gard_text_normalize, _is_english, _is_under_char_threshold

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
    def get_gard_nodes(self, batch_size: int = 100, mark_updated_before_yield: bool = True, only_unprocessed: bool = False) -> Iterator[List[Dict[str, Any]]]:

        """
        Fetch GARD names and synonyms from MySQL in batches.

        Set only_unprocessed=True for a GARD rebuild that resumes from NULL
        updated dates. The default selects NULL dates or dates at least seven
        days before the Alert discovery run's fixed start date.
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

        '''
        A GARD rebuild uses updated as a durable completion checkpoint: any
        non-NULL date means that row has already been processed, even when the
        run crosses midnight or is restarted on another day. The recurring
        Alert runner revisits nodes at least seven days after their saved
        update date. Fix that eligibility cutoff for the whole discovery run
        so crossing midnight does not change which dates qualify.
        Both SQL predicates are fixed strings, independent of source data.
        '''
        updated_condition = "updated IS NULL" if only_unprocessed else "(updated IS NULL OR updated <= %s)"
        query_parameters = (batch_size,)

        if not only_unprocessed:
            '''
            Capture MySQL's date once before fetching any batches. Completion
            markers are also written with MySQL CURDATE(), so this uses the
            same clock even if Python and the database have different time
            zones. Derive a separate cutoff seven days earlier and keep it
            unchanged for the whole generator. The inclusive comparison also
            selects dates exactly seven days before the run, while dates only
            six days old remain ineligible even if processing crosses midnight.
            '''
            cursor = self.mysql.cursor()
            try:
                cursor.execute("SELECT CURDATE()")
                run_start_date = cursor.fetchone()[0]
            finally:
                cursor.close()

            cutoff_date = run_start_date - timedelta(days=7)
            query_parameters = (cutoff_date, batch_size)
            self.logger.info(
                f"Using fixed GARD discovery run_start_date={run_start_date}; "
                f"inclusive updated cutoff_date={cutoff_date}."
            )

        GARD_QUERY = f"""
            SELECT id,
                GardID, MONDO_ID, 
                MAX(CASE WHEN Label_Predicate_Type = 'Name' THEN Label END) AS gardName,
                GROUP_CONCAT(CASE WHEN Label_Predicate_Type = 'Synonym' THEN Label END SEPARATOR '$$$') AS `synonyms`,
                Label_Source, 
                MIN(updated) AS min_updated
            FROM  gard
            WHERE  
                {updated_condition}

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
            cursor.execute(GARD_QUERY, query_parameters)
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

            # The GARD update runner marks each ID after its downstream steps.
            # The regular alert runner retains its existing batch marking.
            if mark_updated_before_yield:

                # Reset the 'update' flag to NULL for a re-run the whole GARD nodes.
                '''
                UPDATE gard_rdas_db.gard
                SET updated = NULL
                WHERE updated IS NOT NULL
                '''
                self._set_updated_flag(gard_id_set)

            yield batch

        # close all 
        self.close()




    def _get_filtered_gard_names(self, name, synonyms) -> list:
        """Build preferred and synonym search terms for the trial/publication tasks.

        The preferred GARD name is always kept as the first search term. A
        synonym is included only when ``_is_english`` recognizes it as a single
        English word and ``_is_under_char_threshold`` does not classify it as
        a single word shorter than four characters. A synonym is also excluded
        when it is already one of the words in the preferred name, since that
        would add a less specific duplicate search term. Multiword synonyms are
        excluded by the single-word requirement in ``_is_english``.

        The comments below use GARD:0024496, ``optic papillitis``, from
        ``GARD_Nomenclature_eng_2_10_2025_WithOrphanetData.csv``. Its 39 source
        synonyms demonstrate how the actual filters narrow a long list.
        """

        # Real CSV example: GARD:0024496, preferred name "optic papillitis".
        # Source synonyms, in their original order:
        # [
        #   "Choked disks", "Edemas, optic disc", "Edemas, retinal",
        #   "increased intracranial pressure associated papilledema",
        #   "inflammation of optic disc", "oedema of the optic disc",
        #   "optic nerve papillitis", "papilledema", "retinal oedema",
        #   "Choked disc", "edema of the optic disk", "edema, retinal",
        #   "optic papilla oedema", "papillitis, optic", "papilloedema",
        #   "retinal edema", "decreased intraocular pressure associated papilledema",
        #   "disk, Choked", "edema, optic disc", "optic discitis",
        #   "papilledema associated with decreased intraocular pressure",
        #   "papilledema associated with increased intraocular pressure",
        #   "papillitis", "Choked discs", "Choked disk",
        #   "decreased intraocular pressure-associated papilledema",
        #   "disks, Choked", "edema, optic disk", "edema, optic papilla",
        #   "Edemas, optic disk", "increased intracranial pressure-associated papilledema",
        #   "inflammation of optic disk", "optic disc inflammation",
        #   "optic disc oedema", "optic disk edema", "optic disk inflammation",
        #   "optic papilla edema", "papillitis, optic nerve", "retinal Edemas"
        # ]

        # Compare normalized words so case, punctuation, and extra whitespace
        # do not prevent detecting a synonym already present in the name.
        preferred_name_words = set(_gard_text_normalize(name).split())

        # Keep recognized English single words that are at least four
        # characters long and are not already words in the preferred name.
        # For the CSV example, the English/length checks pass these synonyms:
        # ["papilledema", "papilloedema", "papillitis"]

        # "papillitis" is then excluded because it is already in "optic papillitis".
        ''' For name="child leukemia " and synonyms=["child (disease) leukemia", "Leukemia"],
          "Leukemia" is excluded because it is already a word in the preferred name.
        '''
        filtered_synonyms = [
            syn for syn in synonyms
            if _is_english(syn)
            and not _is_under_char_threshold(syn)
            and _gard_text_normalize(syn) not in preferred_name_words
        ]

        # Keep the preferred name first, followed by passing synonyms in their
        # original source order. The CSV example result is:
        ''' ["optic papillitis", "papilledema", "papilloedema"] '''
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

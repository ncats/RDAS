import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
    os.path.abspath(os.path.join(_dir, '../..'))
])
 
from colorama import init, Fore, Style
init()

import time
from utils.file_appender import FileAppender
from baseclass.init_base import InitBase
from collections import defaultdict
from typing import Any, Optional, Sequence, Mapping, Union, List, Dict
from utils.tools import _make_hash_key, ask_to_continue, _date_string, _make_hash_key, _curr_time_diff

"""

This script should be executed after the person_of_all_sources table has been created or updated.

"""
class PersonExpertiseInitializer(InitBase):


    def __init__(self):
 
        super().__init__('person_of_all_sources', 'PersonExpertise')
        
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/E-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)



    def update(self):
        
        batch_num = 0
        batch_size = 20
        last_id = 0 

        start_time = time.time()

        while True:

            query = f'SELECT id, gardid FROM rdas_db.gard where id>{last_id} order by id  limit 0, {batch_size}'

            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            cursor.execute(query)

            rows = cursor.fetchall()

            if not rows:
                print('No more rows to fetch. \nExiting.')
                break


            batch_num += 1           
            self.appender.log_stdout(f'\n{Fore.BLUE}====== batch# = {batch_num} ======{Style.RESET_ALL}') 

            for row in rows:

                _sub_start = time.time()
                _one_gard_id_person_tatal = 0
                
                last_id = row['id']
                gard_id = row['gardid']

                self.appender.log_stdout(f'Prossessing row: id = {last_id}, gard_id = {gard_id}')

                articles_of_gard_id = self._articles(gard_id)
                #print(json.dumps(articles_of_gard_id, indent=2))
                _one_gard_id_person_tatal += len(articles_of_gard_id["person"])

                clinical_trials_of_gard_id = self._clinical_trials(gard_id)
                #print(json.dumps(clinical_trials_of_gard_id, indent=2))
                _one_gard_id_person_tatal += len(clinical_trials_of_gard_id["person"])

                grants_of_gard_id = self._grants(gard_id)
                #print(json.dumps(grants_of_gard_id, indent=2))
                _one_gard_id_person_tatal += len(grants_of_gard_id["person"])


                merged = self._merge_person_sources(
                    articles_of_gard_id,
                    clinical_trials_of_gard_id,
                    grants_of_gard_id
                )

                _merged_person_total = len(merged["person"])

                # logs ---
                if _merged_person_total < _one_gard_id_person_tatal:
                    self.appender.log_stdout(f'{Fore.RED}GARD ID: {gard_id} has merged person{Style.RESET_ALL}')


                self.appender.log_stdout(f'Merged person / total person = {_merged_person_total}/{_one_gard_id_person_tatal}')
                self.appender.log_stdout(f'The last id processed: last_id = {last_id}, gard_id = {gard_id}')

                hours, minutes, seconds = _curr_time_diff(_sub_start )
                self.appender.log_stdout(f'- time used: {hours} hours, {minutes} minutes, {seconds} seconds')
               
                hours, minutes, seconds = _curr_time_diff(start_time)
                self.appender.log_stdout(f'Total time elisped: {hours} hours, {minutes} minutes, {seconds} seconds')
                #print(json.dumps(merged, indent=2))
                #write_json_to_file(merged, gard_id.replace(':', '_') + ".json")
                
            cursor.close()



    def _merge_person_sources(self, *sources: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge multiple expertise sources (articles, clinical trials, grants)
        by rdas_group_id and combine full_abstract lists.
        """

        if not sources:
            return {"gardId": None, "person": []}

        gard_id = sources[0].get("gardId")

        merged: Dict[Any, Dict[str, Any]] = {}

        for source in sources:

            for person in source.get("person", []):
                group_id = person.get("rdas_group_id")
                if not group_id:
                    continue

                if group_id not in merged:
                    # shallow copy to avoid mutating original structures
                    merged[group_id] = {
                        "author_id": person.get("author_id"),
                        "rdas_group_id": group_id,
                        "first_name": person.get("first_name"),
                        "last_name": person.get("last_name"),
                        "author_name": person.get("author_name"),
                        "full_abstract": list(person.get("full_abstract", [])),
                    }
                else:
                    existing = merged[group_id]

                    # Prefer first non-null values
                    if not existing.get("first_name") and person.get("first_name"):
                        existing["first_name"] = person.get("first_name")

                    if not existing.get("last_name") and person.get("last_name"):
                        existing["last_name"] = person.get("last_name")

                    if not existing.get("author_name") and person.get("author_name"):
                        existing["author_name"] = person.get("author_name")

                    # Merge abstracts
                    existing["full_abstract"].extend(person.get("full_abstract", []))

        return {
            "gardId": gard_id,
            "person": list(merged.values())
        }


    def _query(self, sql_query: str, params: Optional[Union[Sequence[Any], Mapping[str, Any]]] = None, commit: bool = False) -> Union[List[dict], int, None]:
        """
        Execute a SQL query, with optional parameters.

        Args:
            sql_query: SQL statement. Use parameter placeholders appropriate for your MySQL driver:
                    - Connector/Python / PyMySQL: positional %s placeholders (e.g. "WHERE id = %s")
                    - For named parameters with some drivers: "%(name)s"
            params: Optional sequence (tuple/list) or mapping (dict) of parameters.
            commit: If True, call self.mysql.commit() after execution (useful for INSERT/UPDATE/DELETE).

        Returns:
            - For queries that return rows (SELECT): a list of dict rows (cursor with dictionary=True).
            - For non-SELECT statements: the cursor.rowcount (number of affected rows).
            - None only if the driver behaves unusually.
        """ 

        cursor = None
        try:
            cursor = self.mysql.cursor(dictionary=True, buffered=True)
            if params is None:
                cursor.execute(sql_query)
            else:
                cursor.execute(sql_query, params)

            # If the statement produced rows, fetch them
            if cursor.description is not None:
                rows = cursor.fetchall()
                return rows

            # Non-select (INSERT/UPDATE/DELETE etc.)
            if commit:
                self.mysql.commit()
            return cursor.rowcount
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    raise


    def _build_author_name(self, first: Optional[str], last: Optional[str]) -> Optional[str]:
        return " ".join(part for part in (first, last) if part) or None
    

    def _get_expertise_by_source(self, gard_id: str, query: str, source_type: str, id_field: str, title_field: str, abstract_field: str) -> Dict[str, Any]:
        """
        A generic method to fetch and structure expertise data from different sources.
        """
        # 1. query database
        rows: List[Dict[str, Any]] = self._query(query, (gard_id,)) or []

        # 1.1 Quick path: no rows
        if not rows:
            return {"gardId": gard_id, "person": []}

        # 2. Group rows by rdas_group_id
        grouped: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)

        # 2.1
        for row in rows:

            group_id = row.get("rdas_group_id")
            if not group_id:
                continue

            grouped[group_id].append(row)

        # 3. 
        persons: List[Dict[str, Any]] = []

        # 3.1
        for rdas_group_id, group_rows in grouped.items():

            first_name = next((r.get("first_name") for r in group_rows if r.get("first_name")), None)
            last_name = next((r.get("last_name") for r in group_rows if r.get("last_name")), None)
            author_name = self._build_author_name(first_name, last_name)

            # 3.2 Build abstracts list (preserve ordering in the SQL result)
            abstracts: List[Dict[str, Any]] = [
                {
                    "type": source_type,
                    "abstract_id": r.get(id_field),
                    "title": r.get(title_field),
                    "abstract": r.get(abstract_field),
                    "table_row_id": r.get("table_row_id")
                } for r in group_rows
            ]

            # 3.3
            person_obj = {
                "author_id": _make_hash_key(rdas_group_id),
                "rdas_group_id": rdas_group_id,
                "first_name": first_name,
                "last_name": last_name,
                "author_name": author_name,
                "full_abstract": abstracts,
            }

            # 3.4
            persons.append(person_obj)

        return {"gardId": gard_id, "person": persons}


    def _articles(self, gard_id):
        """
            Return structured article/author data for a GARD id.

            Output format:
            {
                "gardId": <gard_id>,
                "person": [
                    {
                        "author_id": <hash>,
                        "rdas_group_id": <rdas_group_id>,
                        "first_name": <first_name or None>,
                        "last_name": <last_name or None>,
                        "author_name": <author_name or None>,
                        "full_abstract": [
                            {
                                "type": "Article",
                                "abstract_id": <pubmed_id>,
                                "title": <title>,
                                "abstract": <abstract_text>,
                                "table_row_id": <row_id>
                            }, ...
                        ]
                    }, ...
                ]
            }
        """

        # Example gardId = 'GARD:0000005'
        query = '''
            SELECT pg.gard_id, pg.pubmed_id, p.id AS table_row_id, p.first_name, p.last_name, p.rdas_group_id, pa.title, pa.abstract_text
            FROM rdas_db.publication_gard_searchterm_pubmed_mapping pg
            LEFT JOIN rdas_db.publication_article pa 
                ON pa.pubmed_id=pg.pubmed_id
            LEFT JOIN rdas_db.person_of_all_sources p 
                    ON p.associate_id_int = pg.pubmed_id AND p.source = 'Publication'
            WHERE pg.gard_id = %s
        '''
        return self._get_expertise_by_source(
            gard_id,
            query,
            source_type="Article",
            id_field="pubmed_id",
            title_field="title",
            abstract_field="abstract_text"
        )
       

    def _clinical_trials(self, gard_id): 

        # Example gardId = 'GARD:0000005'
        query = f''' SELECT DISTINCT
            d.gardid, d.nctid,
            p.id AS table_row_id, p.first_name, p.last_name, p.rdas_group_id,
            u.brief_title, u.brief_summary
        FROM (
            -- dedupe just the nctid values for this GARD
            SELECT DISTINCT gardId, nctid
            FROM rdas_db.clinical_trial
            WHERE gardId = %s
            AND nctid IS NOT NULL
        ) AS d

        JOIN rdas_db.person_of_all_sources AS p
            ON p.associate_id = d.nctid
            AND p.source = 'ClinicalTrial'

        LEFT JOIN rdas_db.clinical_trial_unique AS u
            ON d.nctid = u.nctid
        '''
 
        return self._get_expertise_by_source(
            gard_id,
            query,
            source_type="ClinicalTrial",
            id_field="nctid",
            title_field="brief_title",
            abstract_field="brief_summary"
        )


    def _grants(self, gard_id):

        # Example gardId = 'GARD:0000005'
        query = f'''
            SELECT 
                ggpr.gard_id, ggpr.application_id, 
                p.id AS table_row_id, p.first_name, p.last_name, p.rdas_group_id, 
                ga.abstract_text, gp.project_title

            FROM rdas_db.grant_gard_project_relation ggpr

            LEFT JOIN rdas_db.person_of_all_sources p
                ON p.associate_id_int = ggpr.application_id
            AND p.source = 'GrantProject'

            LEFT JOIN rdas_db.grant_abstract ga
                ON ga.application_id=ggpr.application_id

            LEFT JOIN rdas_db.grant_project gp
                on ggpr.application_id=gp.application_id

            WHERE ggpr.gard_id = %s
                AND p.rdas_group_id is not null
        '''

        return self._get_expertise_by_source(
            gard_id,
            query,
            source_type="Grant",
            id_field="application_id",
            title_field="project_title",
            abstract_field="abstract_text"
        )



if __name__ == '__main__':

    ok = ask_to_continue('Generate Expertises by GARD ID and save to person_of_all_sources table?')

    if not ok:
        sys.exit(f'{Fore.RED}------Stopped------{Style.RESET_ALL}')
 
    initializer = PersonExpertiseInitializer()

    initializer.update()
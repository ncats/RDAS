import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import re
from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from baseclass.conn import DBConnection as db

class PersonWorker:

    def __init__(self):

        self.processed_flag = '20260105'

        self.mysql = db().mysql_conn()
        self.person_table = 'person_of_all_sources'    
        self.grant_project_table = 'grant_project'
        self.publication_article_table = 'publication_article'
 

    def close_conn(self):        
        self.mysql.close()


    def get_last_names_for_group_id_update(self):
        return self._get_last_names_for_update('group_id_processed')


    def get_last_names_for_graph_update(self):
        return self._get_last_names_for_update('processed')
    
    
    def get_last_names_for_graph_update_2(self, processed_flag, batch_size=500):

        query = f"""
            SELECT DISTINCT last_name 
            FROM {self.person_table} 
            WHERE processed !='{processed_flag}' 
            ORDER BY last_name ASC 
            LIMIT 0, {batch_size}
        """

        fetch_cursor = self.mysql.cursor(buffered=True, dictionary=True)
        fetch_cursor.execute(query)
        
        rows = fetch_cursor.fetchall()        
        last_names = [row['last_name'] for row in rows]
        
        fetch_cursor.close()
        
        return last_names
    

    def _get_last_names_for_update(self, processed_column):
        """
        Get distinct last names where processed column is NULL.
        
        Args:
            processed_column: Column name ('group_id_processed' or 'processed')
        """
        query = f"""
            SELECT DISTINCT last_name 
            FROM {self.person_table} 
            WHERE {processed_column} IS NULL 
            ORDER BY last_name
        """
        
        fetch_cursor = self.mysql.cursor(buffered=True, dictionary=True)
        fetch_cursor.execute(query)
        
        rows = fetch_cursor.fetchall()        
        last_names = [row['last_name'] for row in rows]
        
        fetch_cursor.close()
        
        return last_names


    def get_last_names_by_prefix_for_group_id_update(self, prefix, processed_flag):
        return self._get_last_names_by_prefix(prefix, 'group_id_processed', processed_flag)


    def get_last_names_by_prefix_for_graph_update(self, prefix, processed_flag):
        return self._get_last_names_by_prefix(prefix, 'processed', processed_flag)


    # Use UNION (Best for composite index)
    # Composite index: last_name + processed
    def _get_last_names_by_prefix(self, prefix, processed_column, processed_flag):
        """
        Generic method to get last names by prefix for different processed columns.
        Uses UNION for composite index optimization (last_name + processed).
        
        Args:
            prefix: The prefix to match last names against
            processed_column: Column name ('group_id_processed' or 'processed')
            processed_flag: The processed flag value to exclude
        """
        query = f"""
            SELECT last_name FROM {self.person_table} 
            WHERE last_name LIKE %s AND {processed_column} IS NULL
            UNION
            SELECT last_name FROM {self.person_table} 
            WHERE last_name LIKE %s AND {processed_column} != %s
            ORDER BY last_name ASC
        """
        
        fetch_cursor = self.mysql.cursor(buffered=True, dictionary=True)
        fetch_cursor.execute(query, (prefix + '%', prefix + '%', processed_flag))
        
        rows = fetch_cursor.fetchall()
        last_names = [row['last_name'] for row in rows]
        
        fetch_cursor.close()
        
        return last_names


    # dataframes
    def update_rdas_group_id(self, df, last_name, processed_flag):

        update_sql = f"UPDATE {self.person_table} SET rdas_group_id = %s, group_id_processed = %s WHERE id = %s"

        #1. add a extra processed_flag column & value
        df = df[['id', 'final']].copy()
        df['processed_flag'] = processed_flag

        #2. Set missing final values to last_name+"_none_<index>"
        last_name = re.sub(r'\W+', '', last_name)
        df.loc[df['final'].isna(), 'final'] = (last_name+ '_x_' + df.index[df['final'].isna()].astype(str))

        #3. Reorder columns to desired tuple order
        df = df[['final', 'processed_flag', 'id']]
 
        #4. convert to tuples for MySQL update
        tuples = list(df.itertuples(index=False, name=None))

        self.update_rdas_group_id_with_tuples(tuples)


    # tuples
    def update_rdas_group_id_with_tuples(self, tuples):

        update_sql = f"UPDATE {self.person_table} SET rdas_group_id = %s, group_id_processed = %s WHERE id = %s"
  
        #5. update
        update_cursor = self.mysql.cursor(buffered=True)
        update_cursor.executemany(update_sql, tuples)

        #6. commit & close cursor
        self.mysql.commit()
        update_cursor.close()
      

    def update_processed_flag_for_graph_update(self, last_name, processed_flag):

        update_sql = f"UPDATE {self.person_table} SET processed = %s WHERE last_name = %s"

        update_cursor = self.mysql.cursor(buffered=True)
        update_cursor.execute(update_sql, (processed_flag, last_name))
        
        update_cursor.close()
        self.mysql.commit()

         
    def get_person_by_first_last_name(self, first_name, last_name):

        query = f"SELECT * FROM {self.person_table} WHERE first_name = %s AND last_name = %s"
        
        fetch_cursor = self.mysql.cursor(buffered=True, dictionary=True)
        fetch_cursor.execute(query, (first_name, last_name))
        results = fetch_cursor.fetchall()

        fetch_cursor.close()

        return results


    def fetch_person_by_last_name_for_graph_update(self, last_name, processed_flag=None):

        query = f'''
            SELECT 
                id, associate_id, associate_type, source,  
                first_name, last_name, affiliation, orcid, email, 
                rdas_group_id, PI_id, role
            FROM {self.person_table} 
            WHERE last_name = %s AND (processed IS NULL OR processed != %s)
            '''

        person_list = self._fetch_person(query, (last_name, processed_flag))

        return person_list
    
    
    # Results headers
    #['id','associate_id', 'associate_type', 'source',  'first_name', 'last_name', 'affiliation', 'orcid', 'email',
    # 'rdas_group_id', 'first_publication_date', 'title', 'abstract_text', 'author_list','first_author', 'last_author']
    def fetch_person_by_last_name_for_group_id_update(self, last_name, processed_flag=None):
        """
        Fetch people info from all sources using UNION        
        Args:
            last_name: The last name to search for
            processed_flag: Optional. If provided, filters by processed status.
                        If None, returns all records regardless of processed status.
        """
        
        # Build the processed condition based on whether processed_flag is provided
        if processed_flag is not None:
            processed_condition = f"AND (ps.group_id_processed IS NULL OR ps.group_id_processed != %s)"
            params = (last_name, processed_flag, last_name, processed_flag, last_name, processed_flag)
        else:
            processed_condition = ""
            params = (last_name, last_name, last_name)
        
        # Single unified query with UNION ALL
        query = f'''
            -- Publication Data
            WITH publication_data AS (
                SELECT
                    ps.id, ps.associate_id, ps.associate_type, ps.source,
                    ps.first_name, ps.last_name, ps.affiliation,
                    ps.orcid, ps.email, ps.rdas_group_id,
                    pa.first_publication_date, pa.title, pa.abstract_text,
                    (
                        SELECT GROUP_CONCAT(sub.first_name, ' ', sub.last_name SEPARATOR ', ')
                        FROM {self.person_table} AS sub
                        WHERE
                            sub.source = 'Publication'
                            AND sub.associate_type = 'Author'
                            AND sub.associate_id = ps.associate_id
                    ) AS author_list
                FROM {self.person_table} AS ps
                JOIN {self.publication_article_table} AS pa ON ps.associate_id = pa.pubmed_id
                WHERE
                    ps.last_name = %s
                    AND ps.source = 'Publication'
                    {processed_condition}
            ),
            
            -- Clinical Trial Data
            clinical_trial_data AS (
                SELECT
                    ps.id, ps.associate_id, ps.associate_type, ps.source,
                    ps.first_name, ps.last_name, ps.affiliation,
                    ps.orcid, ps.email, ps.rdas_group_id,
                    '' AS first_publication_date, 
                    '' AS title, 
                    '' AS abstract_text,
                    (
                        SELECT GROUP_CONCAT(sub.first_name, ' ', sub.last_name SEPARATOR ', ')
                        FROM {self.person_table} AS sub
                        WHERE
                            sub.source = 'ClinicalTrial'
                            AND sub.associate_id = ps.associate_id
                    ) AS author_list
                FROM {self.person_table} AS ps
                WHERE
                    ps.last_name = %s
                    AND ps.source = 'ClinicalTrial'
                    {processed_condition}
            ),
            
            -- Grant Project Data
            grant_project_data AS (
                SELECT
                    ps.id, ps.associate_id, ps.associate_type, ps.source,
                    ps.first_name, ps.last_name, ps.affiliation,
                    ps.orcid, ps.email, ps.rdas_group_id,
                    '' AS first_publication_date, 
                    gp.project_title AS title, 
                    '' AS abstract_text,
                    (
                        SELECT GROUP_CONCAT(sub.first_name, ' ', sub.last_name SEPARATOR ', ')
                        FROM {self.person_table} AS sub
                        WHERE
                            sub.source = 'GrantProject'
                            AND sub.associate_id = ps.associate_id
                    ) AS author_list
                FROM {self.person_table} AS ps
                JOIN {self.grant_project_table} AS gp ON ps.associate_id = gp.application_id
                WHERE
                    ps.last_name = %s
                    AND ps.source = 'GrantProject'
                    {processed_condition}
            )
            
            -- Combine all three sources
            SELECT
                *,
                TRIM(SUBSTRING_INDEX(author_list, ',', 1)) AS first_author,
                TRIM(SUBSTRING_INDEX(author_list, ',', -1)) AS last_author
            FROM publication_data
            
            UNION ALL
            
            SELECT
                *,
                TRIM(SUBSTRING_INDEX(author_list, ',', 1)) AS first_author,
                TRIM(SUBSTRING_INDEX(author_list, ',', -1)) AS last_author
            FROM clinical_trial_data
            
            UNION ALL
            
            SELECT
                *,
                TRIM(SUBSTRING_INDEX(author_list, ',', 1)) AS first_author,
                TRIM(SUBSTRING_INDEX(author_list, ',', -1)) AS last_author
            FROM grant_project_data
            
            ORDER BY source, last_name, first_name
        '''
        
        # Single query execution with parameters
        results = self._fetch_person(query, params)
        
        return results


    def _fetch_person(self, query, params):
        
        """
        ✅ SECURE: Accepts parameters separately from query
        
        Args:
            query: SQL query with %s placeholders
            params: Tuple of parameters to safely insert
        """

        fetch_cursor = self.mysql.cursor(buffered=True, dictionary=True)

        # Execute with parameters - MySQL driver handles escaping
        fetch_cursor.execute(query, params) 

        result_list = []
        batch_size = 500
        while True:

            batch = fetch_cursor.fetchmany(batch_size) 

            if not batch:
                break

            result_list.extend(batch)

        fetch_cursor.close()
        #print(f'The params: {params}, total = {len(result_list)}')

        return result_list
    

    def normalize_last_name(self, last_name):

        # To check the special cases
        '''
        -- Fetch all invalid last names using pattern matching
        SELECT DISTINCT last_name, COUNT(*) as record_count
        FROM rdas_db.person_of_all_sources
        WHERE 
            -- Starts with # 
            last_name LIKE '#%'
            
            -- Starts with ( or )
            OR last_name LIKE '(%'
            OR last_name LIKE ')%'
            OR last_name = ')'
            
            -- Starts with -
            OR last_name LIKE '-%'
            OR last_name = '-'
            
            -- Exactly . or .Null
            OR last_name = '.'
            OR last_name = '.Null'
            
            -- Starts with number
            OR last_name REGEXP '^[0-9]'
            
            -- Starts with special characters
            OR last_name LIKE '<%'
            OR last_name LIKE '?%'
            OR last_name LIKE '@%'
            
            -- Starts with apostrophe (all variations)
            OR last_name LIKE '\'%'
            
        GROUP BY last_name
        ORDER BY last_name ASC;
        '''
        # OR
        '''
        SELECT DISTINCT last_name, COUNT(*) as record_count
        FROM rdas_db.person_of_all_sources
        WHERE last_name REGEXP '^[#\(\)\-\.0-9<>?@\']'
        OR last_name IN ('-', '.', '.Null', ')')
        GROUP BY last_name
        ORDER BY last_name ASC;
        '''

        ###
        """
        Normalize last names by removing invalid characters and patterns.        
        Args:
            last_name (str): The last name to normalize            
        Returns:
            str or None: Normalized last name or None if invalid
        """
       
        # Handle None or empty strings
        if not last_name or not isinstance(last_name, str):
            return None
        
        # Strip whitespace
        last_name = last_name.strip()
        
        # Handle empty string after strip
        if not last_name:
            return None
        
        # Rule: If starts with '#', return None (including '#.')
        if re.match(r'^#', last_name):
            return None
        
        # Rule: If starts with '(' or ends with ')' or is just ')', return None
        if re.match(r'^[\(\)]|[\)\']$', last_name):
            return None
        
        # Rule: If exactly '-', '.', or '.Null', return None
        if re.match(r'^(-|\.|\.Null)$', last_name):
            return None
        
        # Rule: If starts with number (including trailing ')' or "'"), return None
        if re.match(r'^\d', last_name):
            return None
        
        # Rule: If starts with special characters like '?', '<', '@', etc., return None
        if re.match(r'^[?<>@\[\]{}]', last_name):
            return None
        
        # Rule: Handle Dutch prefixes - both with and without space
        # Pattern 1: 'n Puff, 't Hart, 's Jongers (with space after prefix)
        # Pattern 2: 'tHart, 'tJong, 's-Gravenmade (no space or hyphen after prefix)
        
        # First check for pattern WITH space: 'X <name>
        match = re.match(r"^'([ntsNTS])\s+(.+)$", last_name)
        if match:
            prefix = match.group(1).lower()
            remainder = match.group(2).strip()
            if remainder:
                # Return properly formatted Dutch name with space
                return f"'{prefix} {remainder}"
            else:
                return None
        
        # Then check for pattern WITHOUT space but followed by letter or hyphen: 'X<name>
        # This handles cases like 'tHart, 'tJong, 's-Gravenmade
        match = re.match(r"^'([ntsNTS])([A-Za-z-].*)$", last_name)
        if match:
            prefix = match.group(1).lower()
            remainder = match.group(2).strip()
            if remainder:
                # Return with space added for consistency: 'tHart -> 't Hart
                return f"'{prefix} {remainder}"
            else:
                return None
        
        # Rule: If starts with "'" (non-Dutch cases), remove the "'" 
        # (e.g., 'Aho -> Aho, 'Neill -> Neill)
        last_name = re.sub(r"^'", '', last_name)
        
        # Rule: If starts with "-", remove the "-" (e.g., -Ahmad -> Ahmad)
        last_name = re.sub(r'^-', '', last_name)
        
        # Final cleanup - strip again
        last_name = last_name.strip()
        
        # Return None if empty after all processing
        if not last_name:
            return None
        
        return last_name
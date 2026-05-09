# Add the project root to the Python path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()


from dotenv import load_dotenv
load_dotenv()
import mysql.connector
from datetime import datetime
from baseclass.conn import DBConnection as db
from utils.minmaxid import MinMaxIdLoader
from utils.tools import ask_to_continue

class BaseClass:

    def __init__(self, table_name, label_name='Data'):

        self.log_dir = 'logs'
        self.label_name = label_name

        '''
        ok = ask_to_continue(f'\n\nInsert the {label_name} from MySQL database into Memgraph database?')
        if not ok:
            sys.exit('------Stopped ------')
        '''

        self.mysql = db().mysql_conn()
        self.update_cursor = self.mysql.cursor(buffered=True)
        self.dict_cursor = self.mysql.cursor(dictionary=True, buffered=True)

        self.memgraph = db().memgraph_conn()

        self.table_name = table_name
        self.formatted_today = datetime.today().strftime("%Y-%m-%d") # Format as YYYY-MM-DD



    def create_indexes(self, label, fields: list):

        for field in fields:

           if not self._is_index_field_exists(label, field):
               self._create_index(label, field)



    def _create_indexes(self, label, fields_list: list):
       for field in fields_list:
           self._create_index(label, field)



    def _create_index(self, label, field):
        command = f"CREATE INDEX ON :{label}({field});"
        self.memgraph.execute(command)

        print(f'\n*** Created index:\n{command}')



    def _get_index_info(self):
        return list(self.memgraph.execute_and_fetch("SHOW INDEX INFO"))


    def _is_index_exists(self, lable_name):

        results = self._get_index_info()
        '''
        [
            {'index type': 'label+property', 'label': 'Article', 'property': 'pubmed_id', 'count': 300002},
            {'index type': 'label+property', 'label': 'ClinicalTrial', 'property': 'NCTId', 'count': 125033},
            {'index type': 'label+property', 'label': 'Contact', 'property': 'ContactName', 'count': 0}
        ]
        '''
        return any(row['label'] == lable_name for row in results)



    def _is_index_field_exists(self, label_name, field):

        results = self._get_index_info()
        '''
        [
            {'index type': 'label+property', 'label': 'Article', 'property': 'pubmed_id', 'count': 300002},
            {'index type': 'label+property', 'label': 'ClinicalTrial', 'property': 'NCTId', 'count': 125033},
            {'index type': 'label+property', 'label': 'Contact', 'property': 'ContactName', 'count': 0}
        ]
        '''
        return any(row['label'] == label_name and row['property'] == field for row in results)



    def populate_all_nodes(self):

        min_id, max_id = MinMaxIdLoader().get_min_max_ids(self.table_name)
        print(f'populate_all_nodes: id range: {min_id} - {max_id}')

        self.populate_nodes(min_id, max_id)


    def populate_nodes(self, min_id, max_id, step=3, batch_size = 200):
        print(f'populate_nodes: id range: {min_id} - {max_id}, step = {step}, batch_size = {batch_size}')



    # Not for ClincialTrial, for Publication
    def update_processed_flag(self, start_id, end_id, flag):
        update_v0 = f'''
            UPDATE  {self.table_name}
            SET processed = \'{flag}\',
                processed_flags = CONCAT(IFNULL(processed_flags, \'\'), \'{flag}\')
            WHERE id BETWEEN {start_id} AND {end_id}
        '''

        update = f'''
            UPDATE  {self.table_name}
            SET processed = \'{flag}\'
            WHERE id BETWEEN {start_id} AND {end_id}
        '''

        self.update_cursor.execute(update)
        self.mysql.commit()  # Commit each chunk



    def update_processed_flag_fulltext_idx(self, start_id, end_id, flag):
        """
            1. Create FULLTEXT index on column 'processed'
                CREATE FULLTEXT INDEX idx_fulltxt_processed ON publication_article(processed);

            2. MySQL's default minimum word length for full-text indexing (ft_min_word_len) is 3 (InnoDB) or 4 (MyISAM).

            3. If the column 'processed' is null, add leading string 'zzzzz'
        """

        update = f'''
            UPDATE  {self.table_name}
            SET processed = CONCAT(IFNULL(processed, \'zzzzz\'), \'{flag}\')
            WHERE id BETWEEN {start_id} AND {end_id}
        '''

        self.update_cursor.execute(update)
        self.mysql.commit()  # Commit each chunk



    def _close_conn(self):
        # Close update cursor if it exists
        if hasattr(self, 'update_cursor') and self.update_cursor is not None:
            try:
                self.update_cursor.close()
            except mysql.connector.Error as e:
                print(f"Error closing update cursor: {e}")

        # Close dictionary cursor if it exists
        if hasattr(self, 'dict_cursor') and self.dict_cursor is not None:
            try:
                self.dict_cursor.close()
            except mysql.connector.Error as e:
                print(f"Error closing dict cursor: {e}")

        # Close MySQL connection if it exists
        if hasattr(self, 'mysql') and self.mysql is not None:
            try:
                self.mysql.close()
            except mysql.connector.Error as e:
                print(f"Error closing MySQL connection: {e}")

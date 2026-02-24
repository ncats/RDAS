# Add the project root to the Python path
import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
    os.path.abspath(os.path.join(_dir, '../..'))
])
 
from dotenv import load_dotenv
load_dotenv()

from colorama import init, Fore, Style
init()

import time
import mysql.connector
from gqlalchemy.exceptions import GQLAlchemyDatabaseError
from datetime import datetime
from baseclass.conn import DBConnection as db 
from utils.minmaxid import MinMaxIdLoader
from utils.file_appender import FileAppender
from utils.tools import  _date_string
from abc import ABC, abstractmethod

class InitBase:


    def __init__(self, table_name, label_name='Data'):
        
        self.log_dir = 'logs'
        self.label_name = label_name
        self.processed_flag = ''
        self.no_column_named_processed = False
        

        self.mysql = db().mysql_conn() 
        self.update_cursor = self.mysql.cursor(buffered=True)
        self.dict_cursor = self.mysql.cursor(dictionary=True, buffered=True)

        self.memgraph = db().memgraph_conn()

        self.table_name = table_name
        self.formatted_today = datetime.today().strftime("%Y-%m-%d") # Format as YYYY-MM-DD

        '''
        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/0-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)
        '''

    @abstractmethod
    def init_nodes(self):
        pass


    def update(self):
        pass

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

        self.appender.log_stdout(f'\n*** Created index:\n{command}')
 

    def _get_index_info(self):
        return list(self.memgraph.execute_and_fetch("SHOW INDEX INFO"))
    

    def _is_index_exists(self, lable_name):   
        results = self._get_index_info()
        '''
        [
            {'index type': 'label+property', 'label': 'Article', 'property': 'pubmed_id', 'count': 300002}, 
            {'index type': 'label+property', 'label': 'ClinicalTrial', 'property': 'NCTId', 'count': 125033}, 
        ]
        '''
        return any(row['label'] == lable_name for row in results) 
    

    def _is_index_field_exists(self, label_name, field):   
        results = self._get_index_info() 
        '''
        [
            {'index type': 'label+property', 'label': 'Article', 'property': 'pubmed_id', 'count': 300002}, 
            {'index type': 'label+property', 'label': 'ClinicalTrial', 'property': 'NCTId', 'count': 125033}, 
        ]
        '''
        return any(row['label'] == label_name and row['property'] == field for row in results)
        
        

    def populate_all_nodes(self):
        
        min_id, max_id = MinMaxIdLoader().get_min_max_ids(self.table_name) 
        self.appender.log_stdout(f'populate_all_nodes: id range: {min_id} - {max_id}')
        
        self.populate_nodes(min_id, max_id)
         
 
 
    def populate_nodes(self, min_id, max_id, step=1, batch_size = 200): 
        pass

 
    # Not for ClincialTrial, for Publication
    def update_processed_flag(self, start_id, end_id, flag): 

        update = f'''
            UPDATE  {self.table_name}
            SET processed = \'{flag}\'
            WHERE id BETWEEN {start_id} AND {end_id}
        ''' 
        
        self.update_cursor.execute(update) 
        self.mysql.commit()  # Commit each chunk


    def get_current_processed_flag(self):

        if not self.table_name or self.no_column_named_processed:
            return None

        query = f'SELECT  processed FROM {self.table_name} limit 0, 1'

        self.dict_cursor.execute(query)
        row = self.dict_cursor.fetchone()

        return row['processed'] if row else None    
    
    
    def get_min_processed_flag(self):
        if not self.table_name or self.no_column_named_processed:
            return None
        
        query = f'SELECT min(processed) as processed FROM {self.table_name}'

        self.dict_cursor.execute(query)
        rows = self.dict_cursor.fetchone()
        
        min_flag = rows['processed'] if rows else None
        
        return min_flag
    

    def close_mysql_conn(self):

        # Close update cursor if it exists
        if hasattr(self, 'update_cursor') and self.update_cursor is not None:
            try:
                self.update_cursor.close()
            except mysql.connector.Error as e:
                self.appender.log_stdout(f"{Fore.RED}Error closing MySQL update cursor: {e}{Style.RESET_ALL}")

        # Close dictionary cursor if it exists
        if hasattr(self, 'dict_cursor') and self.dict_cursor is not None:
            try:
                self.dict_cursor.close()
            except mysql.connector.Error as e:
                self.appender.log_stdout(f"{Fore.RED}Error closing MySQL dict cursor: {e}{Style.RESET_ALL}")

        # Close MySQL connection if it exists
        if hasattr(self, 'mysql') and self.mysql is not None:
            try:
                self.mysql.close()
            except mysql.connector.Error as e:
                self.appender.log_stdout(f"{Fore.RED}Error closing MySQL connection: {e}{Style.RESET_ALL}")


    def memgraph_execute_with_retry(self, cypher_query, params, max_retries=3, delay=2):
        """
        Execute a query with automatic retry on connection failures.
        
        Args:
            cypher_query: Cypher query string
            params: Query parameters dict
            max_retries: Maximum number of retry attempts
            delay: Delay in seconds between retries
            
        Returns:
            Query result or raises exception after max retries
        """
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                return self.memgraph.execute(cypher_query, params)
            
            except GQLAlchemyDatabaseError as e:
                # Capture the last exception
                last_exception = e
                error_msg = str(e).lower()
                
                # Check for connection-related errors
                if any(err in error_msg for err in [ "failed to receive chunk size", "connection", "broken pipe", "lost connection" ]):
                    
                    if attempt < max_retries - 1:

                        self.appender.log_stdout(f"{Fore.RED}Memgraph connection failed: {e}{Style.RESET_ALL}")
                        self.appender.log_stdout(f"Retrying in {delay}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(delay)
                        
                        # Try to reconnect
                        try:
                            self.memgraph = db().memgraph_conn()
                            self.appender.log_stdout("Reconnected successfully")
                        except Exception as conn_error:
                            self.appender.log_stdout(f"{Fore.RED}Memgraph reconnection failed: {conn_error}{Style.RESET_ALL}")
                            # Continue to next retry attempt
                        continue
                    else:
                        self.appender.log_stdout(f"{Fore.RED}\n!!! Max retries ({max_retries}) reached. Giving up. !!!\n{Style.RESET_ALL}")
                
                # Re-raise if it's not a connection error or we're out of retries
                raise
        
        # If we exit the loop without success, raise the last exception
        if last_exception:
            raise last_exception
        
    
# Add the project root to the Python path
import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from utils.applogger import AppLogger
from baseclass.conn import DBConnection as db
from utils.tools import _date_string

class PipelineBase(ABC):

    def __init__(self, table_name='data', init_mysql=True, init_memgraph=False):
        
        self.mysql = None
        self.memgraph = None

        if init_mysql:
            self.mysql = db().mysql_conn() 

        if init_memgraph:
            self.memgraph = db().memgraph_conn() 

        self.formatted_today = datetime.today().strftime("%Y-%m-%d")

        ''' 
        Python file APIs do not expand "~" automatically, so resolve it to
        the current user's home directory before creating the log folder.
        '''
        self.log_dir = os.path.expanduser('~/rdas-memgraph-alert-logs')
        os.makedirs(self.log_dir, exist_ok=True)

        class_name = type(self).__name__
        self.log_file = f"{self.log_dir}/alert-{class_name}.log"
        #self.log_file = f"{self.log_dir}/alert-{class_name}-{_date_string()}.log"

        self.logger = AppLogger(class_name, self.log_file).get_logger()
        self.logger.info(f'\n\n{"*" * 20} The {class_name} is initialized. {"*" * 20}\n')


    @abstractmethod
    def find_new_data(self, gard_node)-> None:
        pass

    @abstractmethod
    def process_new_data(self)-> None:
        pass

    
    def close(self) -> None:

        if self.mysql is not None and self.mysql.is_connected():
            print(f"Closing MySQL connection...")
            self.mysql.close()

        self.mysql = None
        self.memgraph = None

        print('MySQL connection closed')
        print('Memgraph connection closed')

        if hasattr(self, "logger") and self.logger is not None:
            for handler in list(self.logger.handlers):
                handler.flush()
                handler.close()
                self.logger.removeHandler(handler)

            self.logger = None
            print('Logger closed')

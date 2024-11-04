import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append('/home/leadmandj/RDAS/')
sys.path.append(os.getcwd())
import sysvars
from AlertCypher import AlertCypher
from subprocess import *
from time import sleep

class Transfer:
    def __init__ (self, mode=None):
        if mode in ['test','prod']:
            self.mode = mode
        else:
            raise Exception
        
        self.db = AlertCypher('system')
        self.isSeeded = False

    def get_isSeeded(self):
        return self.isSeeded

    def set_isSeeded(self,boolean):
        self.isSeeded = boolean

    def seed(self, dump_name, dump_folder):
        # dump_name = name of database (rdas.ctkg)
        # dump_folder = path to database files directory (RDAS/transfer/)
        try:
            self.db.run(f'DROP DATABASE {dump_name}')
            print('Dropped database...')

            p = Popen(['sudo', '/opt/neo4j/bin/neo4j-admin', 'database', 'load', f'{dump_name}', f'--from-path={dump_folder}', '--overwrite-destination'], encoding='utf8')
            p.wait()
            sleep(10)

            server_id = self.db.run(f"SHOW servers YIELD * WHERE name = \'{self.mode}1\' RETURN serverId").data()[0]['serverId']
            print(f'SERVER ID LOCATED:: {server_id}')

            seed_query = f'CREATE DATABASE {dump_name} OPTIONS {{existingData: \'use\', existingDataSeedInstance: \'{server_id}\'}}'
            print(seed_query)
            self.db.run(seed_query)
            print('Waiting 20 seconds for database load')
            sleep(20)

            self.set_isSeeded(True)

        except Exception as e:
            print('Did not drop or load database...')
            print(e)

    def detect(self, path):
        server = None
        config_title = None
        transfer_detection = list()
        last_updates = {k:str() for k in sysvars.dump_dirs}

        if self.mode == 'test':
            server = 'TEST'
            config_title = 'TRANSFER'
        elif self.mode == 'prod':
            server = 'PROD'
        else:
            raise Exception

        if path == sysvars.transfer_path:
            config_title = 'TRANSFER'
        elif path == sysvars.approved_path:
            config_title = 'APPROVED'
        else:
            raise Exception

        for db_name in sysvars.dump_dirs:
            try:
                # Gets current last update timestamp from config
                last_mod_date = self.db.getConf(f'{server}_{config_title}_DETECTION', db_name)
                # stores value to return later
                last_updates[db_name] = last_mod_date
                # Gets current modification timestamp of the targeted database dump file
                cur_mod_date = os.path.getmtime(f"{path}{db_name}.dump")
                cur_mod_date = str(cur_mod_date)

                # If the config file last update and the current last modification date is different, detect as new transfer and update the config
                if not cur_mod_date == last_mod_date:
                    transfer_detection.append(db_name)
                    self.db.setConf(f'{server}_{config_title}_DETECTION', db_name, cur_mod_date)
            except Exception as e:
                print(e)

        return [transfer_detection,last_updates]
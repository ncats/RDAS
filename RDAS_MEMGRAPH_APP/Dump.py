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
from datetime import datetime

class Dump ():
    def __init__(self, mode='dev'):
        if mode in ['dev','test','prod']:
            self.mode = mode
        else:
            raise Exception
        
        self.db = AlertCypher('system')

    def dump_file (self, path, db_name):
        try:
            self.db.run(f'STOP DATABASE {db_name}')
            print(f'DATABASE {db_name} STOPPED')
            sleep(10)

            p = Popen(['sudo', '/opt/neo4j/bin/neo4j-admin', 'database', 'dump', f'{db_name}', f'--to-path={path}', '--overwrite-destination'], encoding='utf8')
            p.wait()
            print(f'DATABASE {db_name} DUMPED AT {path}')

            self.db.run(f'START DATABASE {db_name}')
            print(f'DATABASE {db_name} RESTARTED')

            print('Waiting 10 seconds for database to update changes')
            sleep(10)

        except Exception as e:
            print(e)

    def generate_backup_name (self, dump_name):
        cur_date = datetime.now().strftime("%m-%d-%y")
        return f'{self.mode}-{dump_name}-{cur_date}'

    def copy_to_backup(self, dump_name):
        filename = self.generate_backup_name(dump_name)
        p = Popen(['sudo', 'cp', f'{sysvars.transfer_path}{dump_name}.dump', f'{sysvars.backup_path}{dump_name}/{filename}.dump'])
        p.wait()
        print(f'DATABASE DUMP PUT INTO BACKUP FOLDER AT {sysvars.backup_path}{dump_name}.dump')
import glob
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
import sysvars
import datetime
from AlertCypher import AlertCypher
from subprocess import *
from time import sleep
from RDAS_MEMGRAPH_APP.Dump import Dump


dump_module = Dump('test')
while True:
    for db_name in sysvars.dump_dirs:
        db = AlertCypher(db_name)
        
        try:
            update = db.run('MATCH (x:UserTesting) RETURN x.Approved as update').data()[0]['update']
            print(f'{db_name}:: {update}')
        
            if update == 'True':
                print(f'Database dump approved for {db_name}')
                db.run('MATCH (x:UserTesting) DETACH DELETE x')

                dump_module.dump_file(sysvars.approved_path, db_name)

                p = Popen(['sudo', 'chmod', '777', f'{sysvars.approved_path}{db_name}.dump'], encoding='utf8')
                p.wait()

                

        except Exception:
            print(f'{db_name} read error')


    sleep(5)


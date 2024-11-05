import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
import sysvars
from datetime import datetime
from subprocess import *
from time import sleep
from RDAS_MEMGRAPH_APP.Update import Update
from RDAS_MEMGRAPH_APP.Mapping import Mapping
from RDAS_MEMGRAPH_APP.Alert import Alert
from RDAS_MEMGRAPH_APP.Dump import Dump
from RDAS_CTKG import update

update_module = Update(mode=sysvars.ct_db)
mapping_module = Mapping(mode=sysvars.ct_db)
alert_module = Alert(mode='dev')
dump_module = Dump(mode='dev')
while True:
    if update_module.check_update()[0]:
        print('Update Initiated... Starting in 10 seconds')
        sleep(10)
        try:
            current_step = 'while running the update scripts'
            update.start_update()

            current_step = 'while refreshing GARD node counts'
            update_module.refresh_node_counts()

            current_step = 'while generating its mapping file'
            mapping_module.generate_mapping_file()

            current_step = 'while attempting to generate a dump files for the database'
            dump_module.dump_file(sysvars.transfer_path, sysvars.ct_db)
            dump_module.copy_to_backup(sysvars.ct_db)

            current_step = 'while attempting to send database over to the TEST server'
            send_url = sysvars.rdas_urls['test']
            p = Popen(['scp', f'{sysvars.approved_path}{sysvars.ct_db}.dump', f'{sysvars.current_user}@{send_url}:{sysvars.transfer_path}{sysvars.ct_db}.dump'], encoding='utf8')
            p.wait()

            current_step = 'while attempting to send notification of database transfer to the TEST server'
            sub = '[RDAS] NOTICE - CTKG UPDATE SUCCESSFUL'
            msg = f'An update cycle for {sysvars.ct_db} has completed'
            html = f'''An update cycle for {sysvars.ct_db} has completed and has been sent to the TEST server</p>
                <p>database effected: {sysvars.ct_db}</p>
                '''
            alert_module.send_email(sub,html,sysvars.contacts['dev'])

        except Exception as e:
            sub = '[RDAS] WARNING - CTKG UPDATE FAILED'
            msg = f'Update script start_ctkg_update.py has failed {current_step}'
            html = f'''<p>Update script start_ctkg_update.py has failed {current_step}</p>
                <p>database effected: {sysvars.ct_db}</p>
                <p>The following error occured:</p>
                <p>{e}</p>
                '''
            alert_module.send_email(sub,html,sysvars.contacts['dev'])

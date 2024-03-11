import glob
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
from AlertCypher import AlertCypher
print(sys.path)
import sysvars
import datetime
from subprocess import *
from time import sleep
print(os.getcwd())
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-dir", "--dump_dir", dest = "dump_dir", help="directory name within the directory that stores the dump files")
args = parser.parse_args()

print('DUMPING DATABASE ON REMOTE SERVER')
p = Popen(['sudo', '/opt/neo4j/bin/neo4j-admin', 'database', 'dump', f'{args.dump_dir}', f'--to-path={sysvars.transfer_path}', '--overwrite-destination'], encoding='utf8')
p.wait()

curdate = str(datetime.today().strftime('%m-%d-%y'))
filename = f'rdas-prod-{args.dump_dir}-{curdate}.dump'
print('COPIED DUMP FILE TO BACKUP FOLDER IN REMOTE SERVER')
p = Popen(['cp', f'{sysvars.transfer_path}/{args.dump_dir}.dump', f'{sysvars.backup_path}/{args.dump_dir}/{filename}'], encoding='utf8')
p.wait()

print('TRANSFERED REMOTE DUMP FILE TO NEO4J-TEST SERVER TRANSFER FOLDER')
p = Popen(['scp', f'{sysvars.transfer_path}/{args.dump_dir}.dump', f'{sysvars.current_user}@{sysvars.rdas_urls['test']}:{sysvars.transfer_path}/{args.dump_dir}.dump'], encoding='utf8')
p.wait()
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
parser.add_argument("-a", "--all", dest = "dump_all", action='store_true', help="dump all of the databases on current server")
parser.add_argument("-t", "--transfer", dest = "transfer", action='store_true', help="Dump to transfer folder")
parser.add_argument("-qc", "--approved", dest = "approved", action='store_true', help="Dump to approved folder")
args = parser.parse_args()

def copy_to_backup(path, filename, dump_name):
    p = Popen(['sudo', 'cp', f'{path}{dump_name}.dump', f'{sysvars.backup_path}{dump_name}/{filename}'])
    p.wait()

def dump_file (path, db_name):
    db = AlertCypher('system')
    db.run(f'STOP DATABASE {db_name}')
    
    #p = Popen(['sudo', '/opt/neo4j/bin/neo4j', 'stop'], encoding='utf8')
    #p.wait()

    p = Popen(['sudo', '/opt/neo4j/bin/neo4j-admin', 'database', 'dump', f'{db_name}', f'--to-path={path}', '--overwrite-destination'], encoding='utf8')
    p.wait()

    db.run(f'START DATABASE {db_name}')

    #p = Popen(['sudo', '/opt/neo4j/bin/neo4j', 'start'], encoding='utf8')
    #p.wait()
    
today = datetime.datetime.today()
today = today.strftime('%m-%d-%Y')

if args.dump_all:
    for dump_name in sysvars.dump_dirs:
        backup_name = f'rdas-{dump_name}-{today}.dump'
        transfer_name = f'{dump_name}.dump'
        approved_name = f'{dump_name}.dump'

        if args.transfer:
            dump_file(sysvars.transfer_path, dump_name)
            print('DUMP FILE ADDED TO TRANSFER FOLDER')
            copy_to_backup(sysvars.transfer_path, transfer_name, dump_name)
            print('DUMP BACKUP CREATED')
        if args.approved:
            dump_file(sysvars.approved_path, approved_name, dump_name)
            print('DUMP FILE ADDED TO APPROVED FOLDER')

else:
    backup_name = f'rdas-{args.dump_dir}-{today}.dump'
    transfer_name = f'{args.dump_dir}.dump'
    approved_name = f'{args.dump_dir}.dump'

    if args.transfer:
        dump_file(sysvars.transfer_path, args.dump_dir)
        print('DUMP FILE ADDED TO TRANSFER FOLDER')
        copy_to_backup(sysvars.transfer_path, backup_name, args.dump_dir)
        print('DUMP BACKUP CREATED')
    if args.approved:
        dump_file(sysvars.approved_path, args.dump_dir)
        print('DUMP FILE ADDED TO APPROVED FOLDER')



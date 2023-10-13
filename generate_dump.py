import glob
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
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
parser.add_argument("-b", "--backup", dest = "backup", action='store_true', help="Dump to backup folder")
parser.add_argument("-t", "--transfer", dest = "transfer", action='store_true', help="Dump to transfer folder")
parser.add_argument("-qc", "--approved", dest = "approved", action='store_true', help="Dump to approved folder")
args = parser.parse_args()

def dump_file (path, filename, dump_name, dump_dir=None):
    if dump_dir:
        p = Popen(['sudo', 'neo4j-admin', 'database', 'dump', f'{dump_name}', f'--to-path={path}{dump_dir}/{filename}', '--overwrite-destination'], encoding='utf8')
    else:
        p = Popen(['sudo', 'neo4j-admin', 'database', 'dump', f'{dump_name}', f'--to-path={path}{filename}', '--overwrite-destination'], encoding='utf8')
    p.wait()
    
today = datetime.datetime.today()
today = today.strftime('%m-%d-%Y')

if args.dump_all:
    for dump_name in sysvars.dump_dirs:
        backup_name = f'rdas-{dump_name}-{today}.dump'
        transfer_name = f'{dump_name}.dump'
        approved_name = f'{dump_name}.dump'

        if args.backup:
            dump_file(sysvars.backup_path, backup_name, dump_name, dump_dir=dump_name)
        if args.transfer:
            dump_file(sysvars.transfer_path, transfer_name, dump_name)
        if args.approved:
            dump_file(sysvars.approved_path, approved_name, dump_name)

else:
    backup_name = f'rdas-{args.dump_dir}-{today}.dump'
    transfer_name = f'{args.dump_dir}.dump'
    approved_name = f'{args.dump_dir}.dump'

    if args.backup:
        dump_file(sysvars.backup_path, backup_name, args.dump_dir, dump_dir=args.dump_dir)
    if args.transfer:
        dump_file(sysvars.transfer_path, transfer_name, args.dump_dir)
    if args.transfer:
        dump_file(sysvars.approved_path, approved_name, args.dump_dir)


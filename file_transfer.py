import glob
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
print(sys.path)
import sysvars
from subprocess import *
from time import sleep
print(os.getcwd())
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-dir", "--dump_dir", dest = "dump_dir", help="directory name within the directory that stores the dump files")
parser.add_argument("-a", "--all", dest = "transfer_all", action='store_true', help="transfer all of most recent dumps")
parser.add_argument("-s", "--server", dest = "server", help="Server name {test/prod}")
args = parser.parse_args()

def get_latest_file (path):
    list_of_files = glob.glob(path)
    latest_file = max(list_of_files, key=os.path.getmtime)
    return latest_file

def transfer (dump_path=None,dump_dir=None,server=None):
    print(dump_path, dump_dir)
    p = Popen(['sudo', 'scp', f'{dump_path}', f'{sysvars.current_user}@{server}:{sysvars.transfer_path}{dump_dir}.dump'], encoding='utf8')
    #WRITE CODE THAT RESPONDS TO PROMPTS TO AUTOMATE TRANSFER
    p.wait()

try:
    server = sysvars.rdas_urls[args.server]
except KeyError:
    print('REQUIRED SERVER ARGUMENT [use -s {test/prod} or --server {test/prod}]')

if server == sysvars.rdas_urls['test']:
    dump_path = sysvars.backup_path
elif server == sysvars.rdas_urls['prod']:
    dump_path = sysvars.transfer_path

if args.transfer_all: # IF TRANSFERING ALL DUMP FILES
    print('TRANSFERING MOST RECENTLY CREATED DUMP FILE FOR EACH DATABASE')
    if args.server == 'prod':
        for dump_dir in sysvars.dump_dirs:
            from_path = f'{dump_path}{dump_dir}.dump'
            transfer(dump_path=from_path,dump_dir=dump_dir,server=server)
    else:
        for dump_dir in sysvars.dump_dirs:
            path = f'{dump_path}{dump_dir}/*.dump'
            print(path)
            transfer_path = get_latest_file(path)
            transfer(dump_path=transfer_path,dump_dir=dump_dir,server=server)

else: # IF TRANSFERING A SINGLE DUMP FILE
    if args.dump_dir:
        print('TRANSFERING LATEST DUMP FILE FOR SPECIFIC DATABASE')
        if args.server == 'prod':
            path = f'{dump_path}{args.dump_dir}.dump'
            transfer(dump_path=transfer_path,dump_dir=args.dump_dir,server=server)

        else:
            path = f'{dump_path}{args.dump_dir}/*.dump'
            transfer_path = get_latest_file(path)
            transfer(dump_path=transfer_path,dump_dir=args.dump_dir,server=server)

    else:
        print('ERRORS WITHIN ARGUMENTS')

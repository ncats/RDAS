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
parser.add_argument("-f", "--file", dest = "dump_filename", help="Specific file name")
parser.add_argument("-a", "--all", dest = "transfer_all", action='store_true', help="transfer all of most recent dumps")
parser.add_argument("-s", "--server", dest = "server", help="Server name {test/prod}")
args = parser.parse_args()

def get_latest_file (path):
    list_of_files = glob.glob(path)
    latest_file = max(list_of_files, key=os.path.getmtime)
    return latest_file

def transfer (dump_path=None,dump_dir=None,server=None):
    print(dump_path, dump_dir)
    p = Popen(['sudo', 'scp', f'{dump_path}', f'{sysvars.current_user}@{server}:/home/{sysvars.current_user}/alert/transfer/{dump_dir}.dump'], encoding='utf8') #WILL BE CHANGED TO TRANSFER FOLDER WITHIN RDAS CODE BASE

    #WRITE CODE THAT RESPONDS TO PROMPTS TO AUTOMATE TRANSFER
    p.wait()

try:
    server = sysvars.rdas_urls[args.server]
except KeyError:
    print('REQUIRED SERVER ARGUMENT [use -s {test/prod} or --server {test/prod}]')

if server == 'test':
    dump_dirs = sysvars.dump_dirs_test
    dump_path = sysvars.dump_path_test
elif server == 'prod':
    dump_dirs = sysvars.dump_dirs_prod
    dump_path = sysvars.dump_path_prod

if args.transfer_all: # IF TRANSFERING ALL DUMP FILES
    if args.dump_filename:
        print('[ERROR] Cannot manually select files to transfer if transfering all')
    else:
        print('TRANSFERING MOST RECENTLY CREATED DUMP FILE FOR EACH DATABASE')
        if args.server == 'prod':
            for dump_dir in dump_dirs:
                from_path = f'{dump_path}{dump_dir}'
                transfer(dump_path=from_path,dump_dir=dump_dir,server=server)
        else:
            for from_dump_dir, to_dump_dir in dump_dirs.items():
                path = f'{sysvars.dump_path}{from_dump_dir}/*.dump'
                transfer_path = get_latest_file(path)
                transfer(dump_path=transfer_path,dump_dir=to_dump_dir,server=server)

else: # IF TRANSFERING A SINGLE DUMP FILE
    if args.dump_dir and args.dump_filename:
        print('TRANSFERING SPECIFIC DUMP FILE')
        to_dump_dir = dump_dirs[args.dump_dir]
        transfer_path = f'{sysvars.dump_path}{args.dump_dir}/{args.dump_filename}'
        print(transfer_path)
        transfer(dump_path=transfer_path,dump_dir=to_dump_dir,server=server)

    elif args.dump_dir and not args.dump_filename:
        print('TRANSFERING LATEST DUMP FILE FOR SPECIFIC DATABASE')
        to_dump_dir = dump_dirs[args.dump_dir]
        path = f'{sysvars.dump_path}{args.dump_dir}/*.dump'
        transfer_path = get_latest_file(path)
        print(transfer_path)
        transfer(dump_path=transfer_path,dump_dir=to_dump_dir,server=server)

    else:
        print('ERRORS WITHIN ARGUMENTS')

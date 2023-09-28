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
import generate_dump

parser = argparse.ArgumentParser()
parser.add_argument("-db", "--database", dest = "db", help="name of database to be approved")
parser.add_argument("-a", "--all", dest = "transfer_all", action='store_true', help="approve all of the databases")
args = parser.parse_args()

print('STARTING APPROVAL PROCESS; THIS SHOULD ONLY BE RAN ON THE TEST SERVER; APPROVE? (y/n)')
res = input()

if res == 'y':    
    if args.transfer_all: # IF TRANSFERING ALL DUMP FILES
        for dump_dir in sysvars.dump_dirs:
            generate_dump.dump(sysvars.approved_path, f'{dump_dir}.dump', '/opt/neo4j/bin/', dump_dir)
    elif args.db: # IF TRANSFERING A SINGLE DUMP FILE
        generate_dump.dump(sysvars.approved_path, f'{args.db}.dump', '/opt/neo4j/bin/', args.db)
else:
    print('APPROVAL PROCESS ABORTED')

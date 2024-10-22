import glob
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
import sysvars
from AlertCypher import AlertCypher
from subprocess import *
from time import sleep
import argparse

# Unused function: Just here for reference, no longer needed with neo4j 5
def migrate(dump_folder, dump_name):
    p = Popen(['sudo', '/opt/neo4j/bin/neo4j-admin', 'database', 'load', f'{dump_name}', f'--from-path={dump_folder}', '--overwrite-destination=true'], encoding='utf8')
    p.wait()

    p = Popen(['sudo', '/opt/neo4j/bin/neo4j-admin', 'database', 'migrate', f'{dump_name}', '--force-btree-indexes-to-range'], encoding='utf8')
    p.wait()

    p = Popen(['sudo', '/opt/neo4j/bin/neo4j-admin', 'database', 'dump', f'{dump_name}', f'--to-path={sysvars.migrated_path}', '--overwrite-destination=true'], encoding='utf8')
    p.wait()

def seed(dump_name, dump_folder, server):
    ac = AlertCypher('system')

    try:
        res = ac.run(f'DROP DATABASE {dump_name}')
        print('Dropped database...')
    except Exception as e:
        print('Did not drop database...')
        print(e)

    p = Popen(['sudo', '/opt/neo4j/bin/neo4j-admin', 'database', 'load', f'{dump_name}', f'--from-path={dump_folder}', '--overwrite-destination'], encoding='utf8')
    p.wait()
    
    p = Popen(['sudo', '/opt/neo4j/bin/neo4j', 'restart'], encoding='utf-8')
    p.wait()

    server_id = ac.run(f"SHOW servers YIELD * WHERE name = \'{server}01\' RETURN serverId").data()[0]['serverId']
    print(f'SERVER ID LOCATED:: {server_id}')
    


    seed_query = f'CREATE DATABASE {dump_name} OPTIONS {{existingData: \'use\', existingDataSeedInstance: \'{server_id}\'}}'
    print(seed_query)
    try:
        res = ac.run(seed_query)
    except Exception as e:
        print(e)

    p = Popen(['sudo', '/opt/neo4j/bin/neo4j', 'restart'], encoding='utf-8')
    p.wait()
    
dump_path = sysvars.migrated_path
dump_filenames = sysvars.dump_dirs

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--db", dest = "db", help="Specific database name if just migrating one database")
parser.add_argument("-a", "--all", dest = "migrate_all", action='store_true', help="migrate all dump files")
parser.add_argument("-s", "--server", dest = "server", help="current server in which code is being ran {test/prod}")
args = parser.parse_args()

if args.migrate_all:
    for dump_filename in dump_filenames: 
        seed(dump_path, dump_filename, args.server)
elif args.db in dump_filenames and not args.migrate_all:
    seed(dump_path, args.db, args.server)

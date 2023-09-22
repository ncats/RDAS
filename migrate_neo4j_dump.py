import glob
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
print(sys.path)
import sysvars
from AlertCypher import AlertCypher
from subprocess import *
from time import sleep
print(os.getcwd())
import argparse

def migrate(dump_folder, dump_name):

    p = Popen(['sudo', '/opt/neo4j/bin/neo4j-admin', 'database', 'load', f'{dump_name}', f'--from-path={dump_folder}', '--overwrite-destination=true'], encoding='utf8')
    p.wait()

    p = Popen(['sudo', '/opt/neo4j/bin/neo4j-admin', 'database', 'migrate', f'{dump_name}', '--force-btree-indexes-to-range'], encoding='utf8')
    p.wait()

    p = Popen(['sudo', '/opt/neo4j/bin/neo4j-admin', 'database', 'dump', f'{dump_name}', f'--to-path={sysvars.migrated_path}', '--overwrite-destination=true'], encoding='utf8')
    p.wait()

def copy_to_cluster(dump_name):
    p = Popen(['sudo', '/opt/neo4j/bin/neo4j', 'start'], encoding='utf8')
    p.wait()

    ac = AlertCypher('neo4j')

    server_id = ac.run(f"SHOW servers YIELD * WHERE name = 'test01' RETURN serverId").data()['serverId']

    ac.run(f"CREATE DATABASE {dump_name} OPTIONS {{existingData: \'use\', existingDataSeedInstance: \'{server_id}\'}}")

dump_path = sysvars.transfer_path
dump_filenames = sysvars.dump_dirs

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--db", dest = "db", help="Specific database name if just migrating one database")
parser.add_argument("-a", "--all", dest = "migrate_all", action='store_true', help="migrate all dump files")

args = parser.parse_args()

ac = AlertCypher('neo4j')
if args.migrate_all:
    for dump_filename in dump_filenames:
        response = ac.run(f'DROP DATABASE {dump_filename}')
elif args.db in dump_filenames and not args.migrate_all:
    response = ac.run(f'DROP DATABASE {args.db}')
    
p = Popen(['sudo', '/opt/neo4j/bin/neo4j', 'stop'], encoding='utf8')
p.wait()

if args.migrate_all:
    for dump_filename in dump_filenames: 
        migrate(dump_path, dump_filename)
        copy_to_cluster(dump_filename)
elif args.db in dump_filenames and not args.migrate_all:
    migrate(dump_path, args.db)
    copy_to_cluster(args.db)

p = Popen(['sudo', '/opt/neo4j/bin/neo4j', 'start'], encoding='utf8')
p.wait()

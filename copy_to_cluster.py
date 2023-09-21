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

def copy_to_cluster(server):
    ac = AlertCypher('neo4j')

    server_id = ac.run(f"SHOW servers YIELD * WHERE name = \'{server}01\' RETURN serverId").data()[0]['serverId']

    ac.run(f"CREATE DATABASE {dump_name} OPTIONS {existingData: \'use\', existingDataSeedInstance: \'{server_id}\'")

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--server", dest = "server", help="current server name you are currently running the script from")
args = parser.parse_args()

if args.server in ['prod','test']:
    copy_to_cluster(args.server)
else:
    print('Invalid Server Destination, use "test" or "prod"')

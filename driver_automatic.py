import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import argparse
import clinical.init, pubmed.init, grant.init

parser = argparse.ArgumentParser()
parser.add_argument("-db", "--database", dest = "db", help="Database name")
args = parser.parse_args()

if args.db == 'ct':
    clinical.update.main()
elif args.db == 'pm':
    pubmed.update.main()
elif args.db == 'gnt':
    grant.update.main()
else:
    print(r'Invalid arguments/flags [TRY: python3 driver_automatic.py -db {ct/pm/gnt}]')


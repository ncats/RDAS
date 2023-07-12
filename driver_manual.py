import os
import sys
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import argparse
from AlertCypher import AlertCypher
import clinical.init, pubmed.init, grant.init, gard.init

parser = argparse.ArgumentParser()
parser.add_argument("-db", "--database", dest = "db", help="Database name")
parser.add_argument("-m", "--mode", dest = "mode", help="Database mode")
args = parser.parse_args()

if args.db == 'ct':
    if args.mode == 'create':
        clinical.init.main()
    elif args.mode == 'update':
        # Not created yet
        clinical.update.main()
elif args.db == 'pm':
    if args.mode == 'create':
        # Not created yet
        pubmed.init.main()
    elif args.mode == 'update':
        # Not created yet
        pubmed.update.main()
elif args.db == 'gnt':
    if args.mode == 'create':
        # Not created yet
        grant.init.main()
    elif args.mode == 'update':
        # Not created yet
        grant.update.main()
elif args.db == 'gard':
    if args.mode == 'create':
        gard.init.main()
    else:
        print('No update command for database; try using "create"')
else:
    print(r'Invalid arguments/flags [TRY: python3 driver_manual.py -db {ct/pm/gnt} -m {create/update}')


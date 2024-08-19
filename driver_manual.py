import os
import sys
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import argparse
from AlertCypher import AlertCypher
import RDAS_CTKG.init, RDAS_PAKG.init, RDAS_GFKG.init, RDAS_GARD.init
import RDAS_CTKG.update, RDAS_PAKG.update, RDAS_GFKG.update

parser = argparse.ArgumentParser()
parser.add_argument("-db", "--database", dest = "db", help="Database name")
parser.add_argument("-m", "--mode", dest = "mode", help="Database mode")
args = parser.parse_args()

print(args.db)
db = AlertCypher('system')

if args.db == 'ct':
    if args.mode == 'create':
        RDAS_CTKG.init.main()
    elif args.mode == 'update':
        RDAS_CTKG.update.main()

elif args.db == 'pm':
    if args.mode == 'create':
        RDAS_PAKG.init.main()
    elif args.mode == 'update':
        db = AlertCypher(sysvars.pm_db)
        last_updated = db.getConf('DATABASE','pubmed_update')
        RDAS_PAKG.update.main(last_updated)

elif args.db == 'gnt':
    if args.mode == 'create':
        RDAS_GFKG.init.main()
    elif args.mode == 'update':
        db.getConf('UPDATE_PROGRESS','grant_progress', 'True')
        RDAS_GFKG.update.main()

elif args.db == 'gard':
    if args.mode == 'create':
        RDAS_GARD.init.main()
    else:
        print('No update command for database; try using "create"')
else:
    print(r'Invalid arguments/flags [TRY: python3 driver_manual.py -db {ct/pm/gnt} -m {create/update}')


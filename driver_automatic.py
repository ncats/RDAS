import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import argparse
import clinical.init, pubmed.init, grant.init
from datetime import date

parser = argparse.ArgumentParser()
parser.add_argument("-db", "--database", dest = "db", help="Database name")
parser.add_argument("-u", "--update-from", dest="date", help="Set date to update from")
args = parser.parse_args()

if args.db == 'ct':
    if args.date:
        print('Clinical Trial Update does not require --update-from date')
    else:
        update_from = date.today().strftime("%d/%m/%Y")
        while True:
            clinical.update.main()
            #add to config
if args.db == 'pm':
    if args.date:
        pubmed.update.main(update_from=args.date)
    else:
        pubmed.update.main(update_from=date.today().strftime("%d/%m/%Y"))
if args.db == 'gnt':
    if args.date:
        print('Grant Update does not require --update-from date')
    else:
        grant.update.main()

print(r'Invalid arguments/flags [TRY: python3 driver_automatic.py -db {ct/pm/gnt} -u 06/30/23]')


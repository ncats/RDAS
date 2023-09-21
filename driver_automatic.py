import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import argparse
import clinical.update, pubmed.update, grant.update
from datetime import date,datetime
from AlertCypher import AlertCypher
from time import sleep
from gard.methods import get_node_counts
from subprocess import *

def check_update(db_type):
    db = AlertCypher('gard')
    today = datetime.now()

    config_selection = {'ct':['clinical_update', 'ct_interval'], 'pm':['pubmed_update', 'pm_interval'], 'gnt':['grant_update', 'gnt_interval']}
    selection = config_selection[db_type]

    last_update = db.getConf('DATABASE',selection[0])
    last_update = datetime.strptime(last_update,"%m/%d/%y")

    delta = today - last_update
    interval = db.getConf('DATABASE',selection[1])
    interval = int(interval)

    last_update = datetime.strftime(last_update,"%m/%d/%y")

    if delta.days > interval:
        print('UPDATE TRIGGERED')
        return [True,last_update]
    else:
        print('UPDATE NOT TRIGGERED')
        return [False,last_update]

parser = argparse.ArgumentParser()
parser.add_argument("-db", "--database", dest = "db", help="Database name")
parser.add_argument("-u", "--update-from", dest="date", help="Set date to update from")
args = parser.parse_args()

if args.db == 'ct':
    if args.date:
        print('Clinical Trial Update does not require --update-from date')
    else:
        while True:
            update_data = check_update(args.db)
            if update_data[0]:
                clinical.update.main()
                get_node_counts()
                p = Popen(['sudo', 'python3', 'generate_dump.py', '-dir clinical', '-b', '-t', '-s dev'], encoding='utf8')
                p.wait()
            sleep(3600)


elif args.db == 'pm':
    if args.date:
        while True:
            update_data = check_update(args.db)
            if update_data[0]:
                pubmed.update.main(update_from=args.date.strftime("%d/%m/%y"))
                get_node_counts()
                p = Popen(['sudo', 'python3', 'generate_dump.py', '-dir pubmed', '-b', '-t', '-s dev'], encoding='utf8')
                p.wait()
            sleep(3600)
    else:
        while True:
            update_data = check_update(args.db)
            if update_data[0]:
                pubmed.update.main(update_from=update_data[1])
                get_node_counts()
                p = Popen(['sudo', 'python3', 'generate_dump.py', '-dir pubmed', '-b', '-t', '-s dev'], encoding='utf8')
                p.wait()
            sleep(3600)


elif args.db == 'gnt':
    if args.date:
        print('Grant Update does not require --update-from date')
    else:
        while True:
            if check_update(args.db):
                grant.update.main()
                get_node_counts()
                p = Popen(['sudo', 'python3', 'generate_dump.py', '-dir grant', '-b', '-t', '-s dev'], encoding='utf8')
                p.wait()

            sleep(3600)

else:
    print(r'Invalid arguments/flags [TRY: python3 driver_automatic.py -db {ct/pm/gnt} -u 06/30/23]')


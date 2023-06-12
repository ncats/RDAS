import os
import sys
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import argparse
import clinical.init, pubmed.init, grant.init

parser = argparse.ArgumentParser()
parser.add_argument("-db", "--database", dest = "db", help="Database name")
parser.add_argument("-m", "--mode", dest = "mode", help="Database mode")
parser.add_argument("-r", "--start-from", dest = "restart", help="Erases saved data up to specified point and restarts database from that point")
args = parser.parse_args()

if args.db == 'ct':
    if args.restart:
        if args.restart == '0':
            print('[CT] STARTING FROM DISEASE NAME WEBSCRAPING')
            if os.path.exists(f'{sysvars.ct_files_path}queries.csv'):
                os.remove(f'{sysvars.ct_files_path}queries.csv')
            if os.path.exists(f'{sysvars.ct_files_path}all_trial_data.json'):
                os.remove(f'{sysvars.ct_files_path}all_trial_data.json')
            if os.path.exists(f'{sysvars.ct_files_path}ctgov_nctids.json'):
                os.remove(f'{sysvars.ct_files_path}ctgov_nctids.json')
            if os.path.exists(f'{sysvars.ct_files_path}ctgov_webscraped_names.csv'):
                os.remove(f'{sysvars.ct_files_path}ctgov_webscraped_names.csv')

        elif args.restart == '1':
            print('[CT] STARTING FROM NCTID GATHERING')
            if os.path.exists(f'{sysvars.ct_files_path}queries.csv'):
                os.remove(f'{sysvars.ct_files_path}queries.csv')
            if os.path.exists(f'{sysvars.ct_files_path}all_trial_data.json'):
                os.remove(f'{sysvars.ct_files_path}all_trial_data.json')
            if os.path.exists(f'{sysvars.ct_files_path}ctgov_nctids.json'):
                os.remove(f'{sysvars.ct_files_path}ctgov_nctids.json')

        elif args.restart == '2':
            print('[CT] STARTING FROM FULL TRIAL INFORMATION GATHERING')
            if os.path.exists(f'{sysvars.ct_files_path}queries.csv'):
                os.remove(f'{sysvars.ct_files_path}queries.csv')
            if os.path.exists(f'{sysvars.ct_files_path}all_trial_data.json'):
                os.remove(f'{sysvars.ct_files_path}all_trial_data.json')

        elif args.restart == '3':
            print('[CT] STARTING FROM CYPHER QUERY GENERATION')
            if os.path.exists(f'{sysvars.ct_files_path}queries.csv'):
                os.remove(f'{sysvars.ct_files_path}queries.csv')

        elif args.restart == '4':
            print('STARTING FROM DATABASE POPULATION')

        else:
            print('start-from argument invalid [TRY: python3 driver_manual.py -db {ct/pm/gnt} -m {create/update} -r {0/1/2/3}]')
    

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
else:
    print(r'Invalid arguments/flags [TRY: python3 driver_manual.py -db {ct/pm/gnt} -m {create/update} -r {0/1/2/3}]')


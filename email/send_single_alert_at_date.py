import django
from django.core.mail import send_mail
from django.template.loader import render_to_string
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
from AlertCypher import AlertCypher

print('Insert name of database to get update from:')
dbname = input()
print('Insert desired alert date to select from (FORMAT: MM/DD/YY)')
date = input()

selecteddb = AlertCypher(dbname)
nodename = 'Article'

if dbname == 'clinical':
	nodename = 'ClinicalTrial'
elif dbname == 'pubmed':
	nodename = 'Article'
elif dbname == 'grant':
	nodename = 'Project'

response = selecteddb.run(f'MATCH (x:{nodename}) WHERE x.DateCreatedRDAS = \'{date}\' return count(x)').data()['count(x)']

send_mail (
	'Test Email with Data',
    	f'Daily Digest for {date}\nThere are {response} new updates in our database',
    	'ncatsrdas@mail.nih.gov',
    	['shanmardev@hotmail.com']
)



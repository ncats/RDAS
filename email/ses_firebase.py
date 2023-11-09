import glob
import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append(os.getcwd())
import sysvars
from AlertCypher import AlertCypher
from datetime import date,datetime
from dateutil.relativedelta import relativedelta
import firebase_admin
from firebase_admin import auth
from firebase_admin import credentials
from firebase_admin import firestore
import alert
def fill_template(type,data):
    tabs = {'clinical':'trials', 'grant':'project', 'pubmed':'nonepi-articles'}
    txt_db = {'clinical': 'clinical trial', 'grant': 'funded project', 'pubmed': 'publication'}
    full_msg = ''
    html = ''

    full_msg += 'You have {num} new entries for your subscribed rare diseases in the {db_title} database\nOut of that {num},\n\n'.format(num=data['total'], db_title=txt_db[type])

    html += """
        <html>
        <head>
        </head>
        <body style="background-color:white;font-family: Arial, sans-serif;">
        <div style="text-align:center">
        <h1 style="font-size:50;color:purple"><u>Rare Disease Alert System</u></h1>
        </div>
        <div style="text-align:center;margin:auto">
        <h1 style=font-size:25>{name}</h1>
        <h1>Within the last week, <b>{num}</b> new entries for your subscribed rare diseases have been added to the {db_title} database</h1>
        <table style="border:5 solid purple;margin:auto;text-align:center" width="750">
        <tr>
        <th>Name</th>
        <th>GARD ID</th>
        <th>Nodes Modified</th>
        </tr>
    """.format(num=data['total'],images_path=sysvars.images_path,db_title=txt_db[type],name=data['name'])

    for gard in data['subscriptions']:
        if data[gard]['num'] > 0:
            full_msg += '{name} [{gardId}] - {num} new additions have been added to the database\n'.format(name=data[gard]['name'], num=data[gard]['num'], gardId=gard)
            html += """
                <tr>
                <td><a href='https://rdas.ncats.nih.gov/disease?id={gardId}#{tab}'>{name}</a></td>
                <td>{gardId}</td>
                <td>{num}</td>
                </tr>
            """.format(name=data[gard]['name'], num=data[gard]['num'], gardId=gard, tab=tabs[type])

    html += """
        </table>
        <h4>Results gathered within the time period of {date_start}-{date_end}</h4>
        <br>
        </div>
        <div style="text-align:center">
        <img src="https://upload.wikimedia.org/wikipedia/commons/6/6e/National_Center_for_Advancing_Translational_Sciences_logo.png" alt="Rare Disease Alert System" width="300" style="display:block;margin:auto">
        <body>
        <html>
    """.format(date_start=data['update_date_start'],date_end=data['update_date_end'])

    print(html)
    return (full_msg,html)

def send_mail(type, data):
    print(f"[{data['total']}, {data['email']}]")
    if data['total'] > 0 and data['email'] == 'timothy.sheils@ncats.nih.gov' or data['email'] == 'zhuqianzq@gmail.com':

        data['email'] = 'devon.leadman@nih.gov' # TEST EMAIL

        if type == "clinical":
            txt,html = fill_template(type,data)
            alert.send_email('RDAS-Alert: Clinical Trial update regarding your subscriptions', txt, data['email'], html=html) #data['email'] in place of email
            print('[Email Sent...]')

        if type == "pubmed":
            txt,html = fill_template(type,data)
            alert.send_email('RDAS-Alert: Publication update regarding your subscriptions', txt, data['email'], html=html)
            print('[Email Sent...]')

        if type == "grant":
            txt,html = fill_template(type,data)
            alert.send_email('RDAS-Alert: Funded Project update regarding your subscriptions', txt, data['email'], html=html)
            print('[Email Sent...]')

def get_stats(type, gards, date=None):
    db = AlertCypher(type)
    return_data = dict()

    if date:
        now = date
        start_search_date = datetime.strptime(now,"%m/%d/%y") - relativedelta(days=7)
        end_search_date = datetime.strptime(now,"%m/%d/%y")
        now_end = now.replace("/","-")
        now_start = start_search_date.strftime("%m/%d/%y")
        print(now_start, now)
    else:
        end_search_date = date.today()
        start_search_date = now - relativedelta(days=7)
        now = end_search_date.strftime("%m/%d/%y")
        now_end = now.replace("/","-")
        now_start = start_search_date.strftime("%m/%d/%y")

    print(f'Searching for nodes created between {now_start} and {now}')

    convert = {'clinical':['ClinicalTrial','GARD','GardId'], 'pubmed':['Article','GARD','GardId'], 'grant':['Project','GARD','GardId']}
    connect_to_gard = {'clinical':'--(:Condition)--(:Annotation)--','pubmed':'--','grant':'--'}
    query = 'MATCH (x:{node}){connection}(y:{gardnode}) WHERE x.DateCreatedRDAS = \"{now}\" AND y.{property} IN {list} RETURN COUNT(x)'.format(node=convert[type][0], gardnode=convert[type][1], property=convert[type][2], list=list(gards.keys()), now=now, connection=connect_to_gard[type])

    print(query)

    response = db.run(query)
    return_data['total'] = response.data()[0]['COUNT(x)']

    for gard in gards.keys():
        response = db.run('MATCH (x:{node}){connection}(y:{gardnode}) WHERE x.DateCreatedRDAS = \"{now}\" AND y.{property} = \"{gard}\" RETURN COUNT(x)'.format(node=convert[type][0], gardnode=convert[type][1], property=convert[type][2], gard=gard, now=now, connection=connect_to_gard[type]))
        return_data[gard] = {'name':gards[gard],'num':response.data()[0]['COUNT(x)']}

    return_data['update_date_end'] = now
    return_data['update_date_start'] = now_start
    return return_data

def trigger_email(type,date=None):
    convert = {'clinical':'trials', 'pubmed':'articles', 'grant':'grants'}
    user_data = dict()
    cred = credentials.Certificate(sysvars.firebase_key_path)
    firebase_admin.initialize_app(cred)
    firestore_db = firestore.client()

    firestore_docs = firestore_db.collection(u'users').stream()

    for doc in firestore_docs:
        if doc.exists:
            user_data[doc.id] = doc.to_dict()
        else:
            print(u'Document Doesnt Exist')

    for firestore_user, data in user_data.items():
        subscript_gard = dict()
        for subscript in data['subscriptions']:
            try:
                if convert[type] in subscript['alerts']:
                    subscript_gard[subscript['gardID']] = subscript['diseaseName']
            except KeyError:
                print('')
                pass
        users = auth.list_users()
        if users:
            users = users.iterate_all()
            for user in users:
                uid = user.uid
                if uid == firestore_user and len(subscript_gard) > 0:
                    update_data = get_stats(type, subscript_gard, date)
                    update_data['email'] = user.email
                    update_data['name'] = user_data[uid]['displayName']
                    update_data['subscriptions'] = list(subscript_gard.keys())
                    send_mail(type, update_data)

#trigger_email(sysvars.gnt_db, '04/27/23') #TEST

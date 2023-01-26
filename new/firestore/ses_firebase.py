from datetime import date
import firebase_admin
from firebase_admin import auth
from firebase_admin import credentials
from firebase_admin import firestore

def send_mail(type, data):
    if data['total'] > 0:
        
        if type == "clinical":
            print('To: {toemail}\nYou have {num} new entries for your subscribed rare diseases in the clinical trial database\nOut of that {num},\n'.format(toemail=data['email'],num=data['total']))
            for gard in data['subscriptions']:
                if data[gard]['num'] > 0:
                    print('{name} [{ID}] - {num} new additions have been added to the database'.format(name=data[gard]['name'], num=data[gard]['num'], ID=gard))

            #ses.clinical_msg() # pass in data for email as dict
        elif type == "pubmed":
            print('To: {toemail}\nYou have {num} new entries for your subscribed rare diseases'.format(toemail=data['email'],num=data['update_num']))
            #ses.pubmed_msg()
        elif type == "grant":
            print('To: {toemail}\nYou have {num} new entries for your subscribed rare diseases'.format(toemail=data['email'],num=data['update_num']))
            #ses.grant_msg()
        

def get_stats(db, type, gards):
    return_data = dict()
    now = date.today()
    now = now.strftime("%m/%d/%y")
    convert = {'clinical':['ClinicalTrial','GARD','GARDId'], 'pubmed':['Article','Disease','gard_id'], 'grant':['Project','Disease','gard_id']}

    response = db.run('MATCH (x:{node})--(y:{gardnode}) WHERE x.DateCreated = \"{now}\" AND y.{property} IN {list} RETURN COUNT(x)'
        .format(node=convert[type][0], gardnode=convert[type][1], property=convert[type][2], list=list(gards.keys()), now=now))
    return_data['total'] = response.data()[0]['COUNT(x)']

    for gard in gards.keys():
        response = db.run('MATCH (x:{node})--(y:{gardnode}) WHERE x.DateCreated = \"{now}\" AND y.{property} = \"{gard}\" RETURN COUNT(x)'
            .format(node=convert[type][0], gardnode=convert[type][1], property=convert[type][2], gard=gard, now=now))
        return_data[gard] = {'name':gards[gard],'num':response.data()[0]['COUNT(x)']}

    return return_data

def trigger_email(db, type):
    convert = {'clinical':'trials', 'pubmed':'articles', 'grant':'grants'}
    user_data = dict()
    cred = credentials.Certificate('new\\firestore\\key.json')
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
                pass

        users = auth.list_users()
        if users:
            users = users.iterate_all()
            for user in users:
                uid = user.uid
                if uid == firestore_user:
                    update_data = get_stats(db, type, subscript_gard)
                    update_data['email'] = user.email
                    update_data['subscriptions'] = list(subscript_gard.keys())
                    send_mail(type, update_data)

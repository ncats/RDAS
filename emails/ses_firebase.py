import glob
import os
import sys
import json
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
# sys.path.append(os.getcwd())
sys.path.append('/home/aom2/RDAS')
sys.path.append('/home/aom2/RDAS/emails')
import sysvars
from AlertCypher import AlertCypher
from datetime import date,datetime
from jinja2 import Environment, FileSystemLoader
import pandas as pd
from dateutil.relativedelta import relativedelta
import firebase_admin
from firebase_admin import auth
from firebase_admin import credentials
from firebase_admin import firestore
import alert
import email_test


prefix = sysvars.db_prefix # you can set the db_prefix in sysvars.py


def render_template(filename, data={}):
    env = Environment(loader=FileSystemLoader(f'{sysvars.base_path}emails/'))
    template = env.get_template(filename)
    rendered_content = template.render(data=data)
    return rendered_content

def send_mail(type, data):
    # Define the tabs dictionary and txt_db
    tabs = {prefix +'clinical': 'trials', prefix +'grant': 'project', prefix +'pubmed': 'nonepi-articles'}
    txt_db = {prefix +'clinical': 'clinical trial', prefix +'grant': 'funded project', prefix +'pubmed': 'publication'}
            
    # Add tabs and type to the data dictionary
    data['tabs'] = tabs
    data["db_title"]=str(txt_db[type])
   
    if data['total'] > 0 and data['email'] == 'timothy.sheils@ncats.nih.gov' or data['email'] == 'zhuqianzq@gmail.com':# for testing

        data['email'] = 'minghui.ao@nih.gov' # TEST EMAIL 
        html_content = render_template('email_template1.html', data=data)
        alert.send_email(f'RDAS-Alert: {str(txt_db[type])} update regarding your subscriptions',  html_content, data['email'])# change to your alert.py sending email method.you may need to adjust your method abit to read in these parameters.
        print("finish sending enail")

def get_stats(type, gards, date_start=datetime.today().strftime('%m/%d/%y'), date_end=datetime.today().strftime('%m/%d/%y')):
    db = AlertCypher(type)
    return_data = dict()
    date_start_string = date_start
    date_end_string = date_end
    date_start_obj = datetime.strptime(date_start, '%m/%d/%y')
    date_end_obj = datetime.strptime(date_end, '%m/%d/%y')

    date_list = pd.date_range(date_start_obj, date_end_obj, freq='D').strftime('%m/%d/%y').to_list()

    convert = {prefix+'clinical':['ClinicalTrial','GARD','GardId'], prefix+'pubmed':['Article','GARD','GardId'], prefix+'grant':['Project','GARD','GardId']}
    connect_to_gard = {prefix+'clinical':'--(:Condition)--(:Annotation)--',prefix+'pubmed':'--',prefix+'grant':'--'}

    query = 'MATCH (x:{node}){connection}(y:{gardnode}) WHERE x.DateCreatedRDAS IN {date_list} AND y.{property} IN {list} RETURN COUNT(x)'.format(node=convert[type][0], gardnode=convert[type][1], property=convert[type][2], list=list(gards.keys()), date_list=date_list, connection=connect_to_gard[type])

    response = db.run(query)
    result = response.single()
    return_data['total'] = result['COUNT(x)']
    
    for gard in gards.keys():
        query_1='MATCH (x:{node}){connection}(y:{gardnode}) WHERE x.DateCreatedRDAS IN {date_list} AND y.{property} = \"{gard}\" RETURN COUNT(x)'.format(node=convert[type][0], gardnode=convert[type][1], property=convert[type][2], gard=gard, date_list=date_list, connection=connect_to_gard[type])
        response = db.run(query_1)
        result = response.single()
        return_data[gard] = {'name':gards[gard],'num':result['COUNT(x)']}

    return_data['update_date_end'] = date_end_string
    return_data['update_date_start'] = date_start_string

    return return_data


# the trigger_email function was rewrite to avoid the three nested for loops.
def trigger_email(firestore_db,type,date_start=None,date_end=None):
    convert = {prefix+'clinical':'trials', prefix+'pubmed':'articles', prefix+'grant':'grants'}
    user_data = dict()
    firestore_docs = firestore_db.collection(u'users').stream()
   
   # get user subscription data here to avoid 3 nested for loops
    for doc in firestore_docs:
        if doc.exists:
            user_data[doc.id] = doc.to_dict()
        else:
            print('Document Doesnt Exist')

    users = auth.list_users()
    user_info={}   
    if users:
        users = users.iterate_all()
        for user in users:
            uid = user.uid
            user_info[user.uid]=user
    
    for firestore_user, data in user_data.items():
        subscript_gard = dict()
        for subscript in data['subscriptions']:
            if convert[type] in subscript['alerts']:     
                if 'diseaseName' not in subscript:
                    subscript_gard[subscript['gardID']] = ""
                else:    
                    subscript_gard[subscript['gardID']] = subscript['diseaseName']

        # get user emails
        user=user_info.get(firestore_user,None)
        if user:
            uid=user.uid
            # print("uid == firestore_user::",uid == firestore_user,len(subscript_gard))
            if uid == firestore_user and len(subscript_gard) > 0:
               
                if not date_start and not date_end:
                    update_data = get_stats(type, subscript_gard)
                elif date_start and date_end:
                    update_data = get_stats(type, subscript_gard, date_start=date_start, date_end=date_end)
                elif date_start:
                    update_data = get_stats(type, subscript_gard, date_start=date_start)
                elif date_end:
                    update_data = get_stats(type, subscript_gard, date_end=date_end)

                update_data['email'] = user.email
                update_data['name'] =  user_data[uid].get('displayName',"")
                update_data['subscriptions'] = list(subscript_gard.keys())
                # print("update_data::",update_data)
                if update_data["total"]>0: # only send email to user if there is any updates
                    send_mail(type, update_data)
                
# trigger_email(sysvars.ct_db, date_start='12/07/22') #TEST. you can put this to the start_dev. so when there are any db upates, it will trigger emails

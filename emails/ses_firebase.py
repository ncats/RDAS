import glob
import os
import sys
import json
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
# sys.path.append(os.getcwd())
sys.path.append('/home/aom2/RDAS_master')
sys.path.append('/home/aom2/RDAS_master/emails')
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
# import email_test


prefix = sysvars.db_prefix # you can set the db_prefix in sysvars.py


def render_template(filename, data={}):
    env = Environment(loader=FileSystemLoader(f'{sysvars.base_path}emails/'))
    template = env.get_template(filename)
    rendered_content = template.render(data=data)
    return rendered_content

def send_mail( data):

    # Add tabs and type to the data dictionary
    # if data['total'] > 0 and data['email'] == '' or data['email'] == '':# for testing
   
    if data['total'] > 0:
        data['email'] = '' # TEST EMAIL 
        html_content = render_template('email_template3.html', data=data)
        alert.send_email(f'RDAS-Alert: Rare Disease Updates Regarding Your Subscriptions',  html_content, data['email'])# change to your alert.py sending email method.you may need to adjust your method abit to read in these parameters.
        print("finish sending enail")

def get_stats(type, gard, date_start,date_end):
    # print("master get_stats----start::",date_start, "end::",date_end)
    db = AlertCypher(type)
    return_data = dict()
    # date_start_string = date_start
    # date_end_string = date_end
    date_start_obj = datetime.strptime(date_start, '%m/%d/%y')
    date_end_obj = datetime.strptime(date_end, '%m/%d/%y')

    date_list = pd.date_range(date_start_obj, date_end_obj, freq='D').strftime('%m/%d/%y').to_list()
    # print("date_list::",date_list)

    convert = {prefix+sysvars.ct_db_name:['ClinicalTrial','GARD','GardId'], prefix+sysvars.pa_db_name:['Article','GARD','GardId'], prefix+sysvars.gf_db_name:['Project','GARD','GardId']}
    connect_to_gard = {prefix+sysvars.ct_db_name:'--(:Condition)--(:Annotation)--',prefix+sysvars.pa_db_name:'--',prefix+sysvars.gf_db_name:'--'}

    # query = 'MATCH (x:{node}){connection}(y:{gardnode}) WHERE x.DateCreatedRDAS IN {date_list} AND y.{property} IN {list} RETURN COUNT(x)'.format(node=convert[type][0], gardnode=convert[type][1], property=convert[type][2], list=list(gards.keys()), date_list=date_list, connection=connect_to_gard[type])
    query = 'MATCH (x:{node}){connection}(y:{gardnode}) WHERE x.DateCreatedRDAS IN {date_list} AND y.{property} = \"{gard}\" RETURN COUNT(x)'.format(node=convert[type][0], gardnode=convert[type][1], property=convert[type][2], gard=gard, date_list=date_list, connection=connect_to_gard[type])
    # print("query::",query)
    response = db.run(query)
    result = response.single()
    # print("total_query count::",result['COUNT(x)'])

    return result['COUNT(x)']

def trigger_email(firestore_db,has_updates,date_start=datetime.today().strftime('%m/%d/%y'), date_end=datetime.today().strftime('%m/%d/%y')):
    print("start_date",date_start, " end date:: ",date_end)
    #obtain users contact information
    txt_tabs_1 = {'trials':prefix +sysvars.ct_db_name, 'grants':prefix +sysvars.gf_db_name, 'articles':prefix +sysvars.pa_db_name}
    tabs = {prefix +sysvars.ct_db_name: 'trials', prefix +sysvars.gf_db_name: 'grants', prefix +sysvars.pa_db_name: 'articles'}
    users = auth.list_users()
    user_info={}   
    if users:
        users = users.iterate_all()
        for user in users:
            uid = user.uid
            user_info[user.uid]=user
   
    # obtain user subscription information from firestore_db
    firestore_docs = firestore_db.collection(u'users').stream()
    user_data={}
    # get user subscription data 
    for doc in firestore_docs:
        if doc.exists:
            user_data[doc.id] = doc.to_dict()
        else:
            print('Document Doesnt Exist')
    for uid, subscript in user_data.items():
        # print(uid,subscript,"\n")

        user=user_info.get(uid,None)
        if user:
            # print("user contact info: ",user.email)

            subscript_gard={}
            query_results={}
            total=0
            query_results = {}
            uniques=set()
            for subs in subscript["subscriptions"]:# for each gard id
                # print("subs::",subs)


                
                if "gardID" in subs and len(subs["alerts"])>0 and subs["alerts"][0]:
                    
                    subscript_gard[subs["gardID"]]=subs["diseaseName"]
            #         # query databases
                    query_results[subs["gardID"]]={}
                    
                    
                    for dtype in subs["alerts"]:
                        print(dtype,txt_tabs_1 [dtype],has_updates)
                        uniques.add(dtype)
                        if txt_tabs_1 [dtype] in has_updates and has_updates[txt_tabs_1 [dtype]]==True:
                           
                            # print("dtype::",dtype)
                            update_count = get_stats(txt_tabs_1 [dtype], subs["gardID"], date_start=date_start, date_end=date_end)
                            query_results[subs["gardID"]][dtype]=update_count
                            total+=update_count
                        else:
                            query_results[subs["gardID"]][dtype]=0
            uniques=list(uniques)
            # if len(uniques)<3:
            #     print("len < 3")
            query_results["datasets"]=uniques
            query_results['email'] = user.email
            query_results['name'] =  user_data[uid].get('displayName',"")
            query_results['subscriptions'] = subscript_gard
            query_results["total"]=total
            query_results["update_date_start"]=date_start
            query_results["update_date_end"]=date_end
            print("total updates::",total)
            # print("query_results::",query_results,"\n")
            if total>0:
                send_mail( query_results)
        print("\n")

   
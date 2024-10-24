import os
import sys
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append('/home/leadmandj/RDAS/')
sys.path.append(os.getcwd())
import sysvars
from AlertCypher import AlertCypher
import firebase_admin
from firebase_admin import auth
from firebase_admin import credentials
from firebase_admin import firestore
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
import boto3
import pandas as pd

class Alert:
    def __init__(self, mode='dev'):
        if mode in ['dev','test','prod']:
            self.mode = mode
        else:
            raise Exception

        cred = credentials.Certificate(sysvars.firebase_key_path)
        firebase_admin.initialize_app(cred)
        self.firestore_db = firestore.client()
        self.sender_email = 'ncatsrdas@mail.nih.gov'
        self.client = self.setup_email_client()

    def setup_email_client(self):
        client = boto3.client(
            service_name='ses',
            region_name='us-east-1'
        )
        return client

    def render_template(self,filename, data={}):
        env = Environment(loader=FileSystemLoader(f'{sysvars.base_path}emails/'))
        template = env.get_template(filename)
        rendered_content = template.render(data=data)
        return rendered_content
    
    def send_email(self,subject,html,recipient):
        print("Sending emails to:", recipient)
        # sender_email = client  # Replace with your email
        # Set up the email
        message={
                'Subject': {
                    'Data': f'{subject}',
                },
                'Body': {
                    
                    'Html': {
                        'Data': f'{html}'
                    },
                }
            }

        # Send the email
        response = self.client.send_email(
            Source=self.sender_email,
            Destination={'ToAddresses': recipient},
            Message=message
        )
        print(f"Email sent successfully to {recipient}.")

    def send_mail(self,data):
        # Add tabs and type to the data dictionary
        # if data['total'] > 0 and data['email'] == '' or data['email'] == '':# for testing
    
        if data['total'] > 0:
            if self.mode == 'dev':
                data['email'] = ['devon.leadman@axleinfo.com']
            elif self.mode == 'test':
                data['email'] = ['devon.leadman@axleinfo.com']

            html_content = self.render_template('email_template3.html', data=data)
            self.send_email(f'RDAS-Alert: Rare Disease Updates Regarding Your Subscriptions',  html_content, data['email'])# change to your alert.py sending email method.you may need to adjust your method abit to read in these parameters.
            print("finish sending enail")

    def get_stats(self, type, gard, date_start, date_end):
        # print("master get_stats----start::",date_start, "end::",date_end)
        db = AlertCypher(type)
        date_list = pd.date_range(date_start, date_end, freq='D').strftime('%m/%d/%y').to_list()

        convert = {sysvars.ct_db:['ClinicalTrial','GARD','GardId'], sysvars.pm_db:['Article','GARD','GardId'], sysvars.gnt_db:['Project','GARD','GardId']}
        connect_to_gard = {sysvars.ct_db:'--',sysvars.pm_db:'--',sysvars.gnt_db:'--'}

        query = 'MATCH (x:{node}){connection}(y:{gardnode}) WHERE x.DateCreatedRDAS IN {date_list} AND y.{property} = \"{gard}\" RETURN COUNT(x)'.format(node=convert[type][0], gardnode=convert[type][1], property=convert[type][2], gard=gard, date_list=date_list, connection=connect_to_gard[type])
        response = db.run(query)
        result = response.single()

        return result['COUNT(x)']

    def trigger_email(self,has_updates,date_start=datetime.today().strftime('%m/%d/%y'), date_end=datetime.today().strftime('%m/%d/%y')):
        print("start_date",date_start, " end date:: ",date_end)
        print(has_updates)
        #obtain users contact information
        txt_tabs_1 = {'trials':sysvars.ct_db, 'grants':sysvars.gnt_db, 'articles':sysvars.pm_db}
        users = auth.list_users()
        user_info={}   
        
        if users:
            users = users.iterate_all()
            for user in users:
                uid = user.uid
                user_info[user.uid]=user
    
        # obtain user subscription information from firestore_db
        firestore_docs = self.firestore_db.collection(u'users').stream()
        user_data={}
        # get user subscription data 
        for doc in firestore_docs:
            if doc.exists:
                user_data[doc.id] = doc.to_dict()
            else:
                print('Document Doesnt Exist')

        for uid, subscript in user_data.items():
            user=user_info.get(uid,None)

            if user:
                subscript_gard={}
                query_results={}
                total=0
                query_results = {}
                uniques=set()

                for subs in subscript["subscriptions"]:# for each gard id
                    if "gardID" in subs and len(subs["alerts"])>0 and subs["alerts"][0] and "diseaseName" in subs:
                        subscript_gard[subs["gardID"]]=subs["diseaseName"]
                        # query databases
                        query_results[subs["gardID"]]={}
                        
                        for dtype in subs["alerts"]:
                            uniques.add(dtype)
                            if txt_tabs_1 [dtype] in has_updates and txt_tabs_1[dtype] in has_updates:
                                update_count = self.get_stats(txt_tabs_1 [dtype], subs["gardID"], date_start, date_end)
                                query_results[subs["gardID"]][dtype]=update_count
                                total+=update_count

                            else:
                                query_results[subs["gardID"]][dtype]=0

                uniques=list(uniques)
                query_results["datasets"]=uniques
                query_results['email'] = [user.email]
                query_results['name'] =  user_data[uid].get('displayName',"")
                query_results['subscriptions'] = subscript_gard
                query_results["total"]=total
                query_results["update_date_start"]=date_start
                query_results["update_date_end"]=date_end
                print(user.email,"total updates::",total)

                if total>0:
                    self.send_mail( query_results)
    

import os
import sys
import json
from typing import Any, Dict, List

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, ".")),
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from datetime import date, datetime, timedelta
from utils.tools import _is_english, _is_under_char_threshold
from pipelines.pipeline_1.gard_1 import GARDPipeline_1
from pipelines.pipeline_2.clinical_trial_1 import ClinicalTrialPipeline_1
from pipelines.pipeline_2.clinical_trial_2 import ClinicalTrialPipeline_2
from pipelines.pipeline_2.clinical_trial_3 import ClinicalTrialPipeline_3
from pipelines.pipeline_2.clinical_trial_4 import ClinicalTrialPipeline_4

from pipelines.pipeline_3.publication_1 import PublicationPipeline_1

from firebase.firebase_query import FirebaseAgent
from emails.email_client import EmailClient

from utils.conn import DBConnection as db

LOOK_BACK_DAYS = 7

def run_update():

    ''' 1. '''
    gardPipeline_1 = GARDPipeline_1() 

    ''' update rdas_db.gard set updated = null;'''
    batch_nodes_generator = gardPipeline_1.get_gard_nodes()

    ''' 2. '''
    clinicalTrialPipeline_1 = ClinicalTrialPipeline_1()
    
    ''' 3. '''
    publicationPipeline_1 = PublicationPipeline_1()


    ''' 1.1 '''
    for batch in batch_nodes_generator:

        for gard_node in batch:

            ''' Process the GARD name and synonyms '''
            ''' 0.1 '''
            name = gard_node['gardName']
            syns = gard_node['synonyms'] 
        
            ''' 0.2 '''
            gardsyns_eng = [syn for syn in syns if _is_english(syn)]
            gardsyns_char_threshold = [syn for syn in syns if _is_under_char_threshold(syn)]

            filtered_syns = [x for x in syns if x in gardsyns_eng]
            filtered_syns = [x for x in filtered_syns if not x in gardsyns_char_threshold]

            filtered_names = [name] + filtered_syns # names list
    
            ''' 0.3 '''
            last_update_date = gard_node.get("updated")

            if last_update_date is None:
                last_update_date = date.today() - timedelta(days = LOOK_BACK_DAYS)

            ''' 0.4 '''
            
            # Remove for production  
            # 2025-07-01
            gard_node["updated"] = date(2025, 7, 1)

            # This is PRODUCTION
            #gard_node['updated'] = last_update_date
            gard_node['filtered_names'] = filtered_names
            
            ''' 2.1 '''
            #clinicalTrialPipeline_1.find_new_data(gard_node)     
           

            ''' 3.1 '''
            publicationPipeline_1.find_new_data(gard_node)     
            

    # Explicitly close the db connections
    clinicalTrialPipeline_1.close()   
    publicationPipeline_1.close()   


    # Update MySQL database   
    """
    # Tested, success !!! #

    ClinicalTrialPipeline_2().process_new_data() 
    ClinicalTrialPipeline_3().process_new_data() 
    ClinicalTrialPipeline_4().process_new_data() 
    """
    

                 
   
    # Update Memgraph database


def run_alert():

    mysql = None
    firebaseAgent = None

    try:
        mysql = db().mysql_conn()
        if mysql is None:
            raise ConnectionError("Failed to connect to MySQL")

        find_new_items_query = '''
            SELECT
                gardId,
                'trials'        AS item_name,
                COUNT(*)        AS new_items_count
            FROM update_clinical_trial
            WHERE gardId    = %s
            AND is_new    = 1 AND alert_sent = 0
            GROUP BY gardId

            UNION ALL

            SELECT
                m.gard_id,
                'articles'      AS item_name,
                COUNT(*)        AS new_items_count
            FROM update_publication_article a
            INNER JOIN publication_gard_searchterm_pubmed_mapping m
                ON  a.pubmed_id   = m.pubmed_id
                AND  m.gard_id     = %s 
            WHERE a.is_new    = 1 AND a.alert_sent = 0 
            GROUP BY m.gard_id
        '''

        emailClient = EmailClient()
        firebaseAgent = FirebaseAgent()

        users = firebaseAgent.get_firebase_authed_users_with_firestore_gard_ids_list()
        
        for user in users:
            
            email = user['email']
            display_name = user['display_name']
            gard_id_list = user['gard_id_list']

            user_subscriptions = user.get("subscriptions", {})
            update_date_end = date.today()
 
            # production
            update_date_start = update_date_end - timedelta(days=LOOK_BACK_DAYS)

            #  Remove for production
            update_date_start = '2025-01-01'

            payload = {
                "data": {
                    "total": 0,
                    "datasets": [],
                    "subscriptions": dict(user_subscriptions),
                    "update_date_start": '2025-01-01', #update_date_start.strftime("%Y-%m-%d"),
                    "update_date_end": update_date_end.strftime("%Y-%m-%d"),
                }
            }
            
            datasets = set()
            active_subscriptions = {}
                
            for gard_id in gard_id_list:

                cursor = mysql.cursor()
                try:
                    cursor.execute(find_new_items_query, (gard_id, gard_id))
                    rows = cursor.fetchall()
                finally:
                    cursor.close()

                if not rows:
                    continue

                payload["data"][gard_id] = {}
                active_subscriptions[gard_id] = user_subscriptions.get(gard_id, gard_id)

                for row in rows:
                    gardId, item_name, new_items_count = row     
                    payload["data"][gardId][item_name] = new_items_count
                    datasets.add(item_name)

            subscription_count = len(active_subscriptions)
            if subscription_count == 0:
                continue
 
            payload["data"]["datasets"] = sorted(datasets)
            payload["data"]["subscriptions"] = active_subscriptions
            payload["data"]["total"] = subscription_count
 
            print(user)
            print(json.dumps(payload, indent=2, ensure_ascii=False))
            print('\n')

            emailClient.send_html_email(
                subject="RDAS Alert Test",
                payload = payload,                            
                #mail_to= user.get('email'), # For PRODUCTION
                mail_to='tongan.zhao@nih.gov', # For testing, remove for PRODUCTION
                mail_cc=None,
            )
    
    finally:
        try:
            if firebaseAgent is not None:
                firebaseAgent.close()
        finally:
            if mysql is not None and mysql.is_connected():
                mysql.close()



if __name__ == "__main__":

    run_update()

    #run_alert()

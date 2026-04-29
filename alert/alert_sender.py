import os
import sys
import json

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, ".")),
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from datetime import date, datetime, timedelta 
from firebase.firebase_query import FirebaseAgent
from emails.email_client import EmailClient
from pipelines.pipeline_base import PipelineBase 

class AlertSender(PipelineBase):

    def __init__(self, look_back_days=7):

        super().__init__(init_mysql=True, init_memgraph=False)

        self.subject="RDAS Alert"
        self.LOOK_BACK_DAYS = look_back_days


    # Not implemented
    def process_new_data(self) -> None:
        raise NotImplementedError("AlertSender does not implement process_new_data().")
   

    # Not implemented   
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("AlertSender does not implement find_new_data().")
 

    '''
    Find new clinical trail & publication and send alert to users which suscribe to them.
    '''
    def find_new_and_send_alert(self):
 
        try: 
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
    
                # PRODUCTION
                update_date_start = update_date_end - timedelta(days = self.LOOK_BACK_DAYS)

                #  Remove for PRODUCTION
                update_date_start = '2025-01-01'

                payload = {
                    "data": {
                        "total": 0,
                        "datasets": [],
                        "subscriptions": dict(user_subscriptions),
                        "update_date_start": '2025-01-01', #update_date_start.strftime("%Y-%m-%d"), #change for PRODUCTION
                        "update_date_end": update_date_end.strftime("%Y-%m-%d"),
                    }
                }
                
                datasets = set()
                active_subscriptions = {}
                    
                for gard_id in gard_id_list:

                    cursor = self.mysql.cursor()
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
                    self.appender.log_stdout(f'* No new subscriptions found for user: {user} - {datetime.now()}')
                    continue
    
                payload["data"]["datasets"] = sorted(datasets)
                payload["data"]["subscriptions"] = active_subscriptions
                payload["data"]["total"] = subscription_count
    
                                
                emailClient.send_html_email(
                    subject = self.subject,
                    payload = payload,                            
                    #mail_to = user.get('email'), # For PRODUCTION
                    mail_to = 'tongan.zhao@nih.gov', # For testing, remove for PRODUCTION
                    mail_cc = None,
                )
        
                self.appender.log_stdout(f'\nSent alert to user: {user} - {datetime.now()}')
                self.appender.log_stdout(json.dumps(payload, indent=2, ensure_ascii=False))

        finally:
            # Explicitly close the db connections
            self.close()

            if firebaseAgent is not None:
                firebaseAgent.close()





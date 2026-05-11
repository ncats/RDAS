import os
import sys
import json

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, ".")),
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from dotenv import load_dotenv
from datetime import date, datetime, timedelta 
from firebase.firebase_query import FirebaseAgent
from emails.email_client import EmailClient
from pipelines.pipeline_base import PipelineBase 
from utils.tools import _recipient_list

load_dotenv()

class AlertSender(PipelineBase):
    """
    Build and send alert emails for new RDAS updates.

    The sender connects three pieces of data: Firebase user subscriptions,
    new clinical trial/publication rows in MySQL, and the HTML email client
    used to notify users and administrators.
    """

    def __init__(self, look_back_days=7):
        """Initialize database access and alert window settings."""

        super().__init__(init_mysql=True, init_memgraph=False)

        self.subject="RDAS Notification"
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
        """
        Match new update rows to Firebase subscriptions and send alert emails.

        A per-user alert is sent only when at least one subscribed GARD ID has
        new data. A summary email is sent to admins after all users are
        processed.
        """
 
        try: 
            # For a single GARD ID, return counts of unsent new clinical trials
            # and new publications. clinical_trial has alert_sent; publication_article
            # does not, so publication alerts are selected by is_new only.
            find_new_items_query = '''
                SELECT
                    ct.gardId,
                    'trials'        AS item_name,
                    COUNT(*)        AS new_items_count
                FROM clinical_trial AS ct
                WHERE ct.gardId = %s
                AND ct.is_new = 1
                AND COALESCE(ct.alert_sent, '0') = '0'
                GROUP BY ct.gardId

                UNION ALL

                SELECT
                    m.gard_id,
                    'articles'      AS item_name,
                    COUNT(*)        AS new_items_count
                FROM publication_article a
                INNER JOIN publication_gard_searchterm_pubmed_mapping m
                    ON  a.pubmed_id   = m.pubmed_id
                    AND  m.gard_id     = %s 
                WHERE a.is_new    = 1
                GROUP BY m.gard_id
            '''

            # EmailClient handles rendering/delivery; FirebaseAgent provides
            # active, verified users and their Firestore subscriptions.
            emailClient = EmailClient()
            firebaseAgent = FirebaseAgent()

            ''' 1. Get all users '''
            users = firebaseAgent.get_firebase_authed_users_with_firestore_gard_ids_list()

            all_updates_summary = []
            
            ''' 2. Send alert to each user '''
            for user in users:
                '''
                {
                    "display_name": "Timothy Sheils",
                    "email": "timothy.sheils@ncats.nih.gov",
                    "gard_id_list": [
                        "GARD:0007704",
                        "GARD:0007827",
                        "GARD:0023606",
                        "GARD:0023607",
                        "GARD:0023954",
                        "GARD:0024146",
                        "GARD:0016773"
                    ],
                    "subscriptions": {
                        "GARD:0007704": "gastric cancer",
                        "GARD:0007827": "tuberculosis",
                        "GARD:0023606": "pediatric lymphoma",
                        "GARD:0023607": "adult lymphoma",
                        "GARD:0023954": "childhood leukemia",
                        "GARD:0024146": "leukemia",
                        "GARD:0016773": "hepatocellular carcinoma"
                    }
                }        
                '''

                gard_id_list = user['gard_id_list']
                if not gard_id_list:
                    continue

                email = user['email']
                display_name = user['display_name']

                user_subscriptions = user.get("subscriptions", {})
                update_date_end = date.today()

                update_date_start = update_date_end - timedelta(days = self.LOOK_BACK_DAYS)

                ''' A payload template / initial payload structure'''
                payload = {
                    "data": {
                        "total": 0,
                        "datasets": [],
                        "subscriptions": dict(user_subscriptions),
                        "update_date_start": update_date_start.strftime("%Y-%m-%d"),
                        "update_date_end": update_date_end.strftime("%Y-%m-%d"),
                    }
                }
                
                datasets = set()
                active_subscriptions = {}
                ''' 3. For each user subscriped GARD id'''
                for gard_id in gard_id_list:

                    cursor = self.mysql.cursor()
                    try:
                        ''' Find new clinical-trial & publication items by gard_id '''
                        cursor.execute(find_new_items_query, (gard_id, gard_id))
                        rows = cursor.fetchall()
                    finally:
                        cursor.close()

                    # No rows means this subscribed disease has no new alertable
                    # trials or articles in the current staging tables.
                    if not rows:
                        continue

                    payload["data"][gard_id] = {}

                    ''' If gard_id exists in user_subscriptions, use its saved value. Otherwise, use gard_id itself as the fallback value. '''
                    active_subscriptions[gard_id] = user_subscriptions.get(gard_id, gard_id)

                    for row in rows:
                        gardId, item_name, new_items_count = row     
                        payload["data"][gardId][item_name] = new_items_count
                        datasets.add(item_name)

                subscription_count = len(active_subscriptions)
                if subscription_count == 0:
                    self.logger.info(f'* No new subscriptions found for user: {user} - {datetime.now()}')
                    continue 

                payload["data"]["datasets"] = sorted(datasets)
                payload["data"]["subscriptions"] = active_subscriptions
                payload["data"]["total"] = subscription_count
                 
                ''' 4. Send alert email to user '''
                # The payload contains only subscriptions that actually had new
                # content, so users do not receive empty disease sections.
                emailClient.send_html_alert_email(
                    subject = self.subject,
                    payload = payload,                            
                    #mail_to = user.get('email'), # For PRODUCTION
                    mail_to = 'tongan.zhao@nih.gov', # For testing, remove for PRODUCTION
                    mail_cc = None,
                )
        
                self.logger.info(f'\nSent alert to user: {user} - {datetime.now()}')
                self.logger.info(json.dumps(payload, indent=2, ensure_ascii=False))
                 
                all_updates_summary.append({"email": email, "display_name": display_name, "payload": payload})

            ''' 5. Send summary email to admins '''
            # The summary email is an operational audit of all user alerts sent
            # during this run. It is skipped when no user had new updates.
            if all_updates_summary:
                try:
                    summary_recipients = _recipient_list(os.getenv("ALERT_SUMMARY_EMAIL_RECIPIENTS"))

                    if summary_recipients:

                        emailClient.send_html_summary_email(
                            subject = f"{self.subject} Summary",
                            all_updates_summary = all_updates_summary,
                            title = "RDAS Alerts Summary for Admins",
                            mail_to = summary_recipients,
                            mail_cc = [],
                        )
                        self.logger.info(f"Sent summary alert email with {len(all_updates_summary)} user sections.")
                    else:
                        self.logger.error("ALERT_SUMMARY_EMAIL_RECIPIENTS is empty. Summary alert email was not sent.")

                except Exception as e:
                    self.logger.error(f"Unable to send summary alert email: {e}")

            else:
                self.logger.info("No user updates found. Summary alert email was not sent.")

        finally:
            # Explicitly close the db connections
            self.close()

            if firebaseAgent is not None:
                firebaseAgent.close()

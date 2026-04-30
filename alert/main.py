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
from pipelines.pipeline_1.task_gard_1 import GARDTask_1
from pipelines.pipeline_2.task_clinical_trial_1 import ClinicalTrialTask_1
from pipelines.pipeline_2.task_clinical_trial_2 import ClinicalTrialTask_2
from pipelines.pipeline_2.task_clinical_trial_3 import ClinicalTrialTask_3
from pipelines.pipeline_2.task_clinical_trial_4 import ClinicalTrialTask_4
from pipelines.pipeline_2.task_clinical_trial_5 import ClinicalTrialTask_5
from pipelines.pipeline_2.task_clinical_trial_6 import ClinicalTrialTask_6

from pipelines.pipeline_3.task_publication_1 import PublicationTask_1

from firebase.firebase_query import FirebaseAgent
from emails.email_client import EmailClient

from utils.conn import DBConnection as db
from alert_sender import AlertSender


def run_update():

    ''' 1. '''
    gardPipeline_1 = GARDTask_1()

    ''' update rdas_db.gard set updated = null;'''
    batch_nodes_generator = gardPipeline_1.get_gard_nodes()

    ''' 2. '''
    clinicalTrialTask_1 = ClinicalTrialTask_1()

    ''' 3. '''
    publicationPipeline_1 = PublicationTask_1()


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
            #clinicalTrialTask_1.find_new_data(gard_node)


            ''' 3.1 '''
            #publicationPipeline_1.find_new_data(gard_node)


    # Explicitly close the db connections
    clinicalTrialTask_1.close()
    publicationPipeline_1.close()


    # Update MySQL database
    """
    ClinicalTrialTask_2().process_new_data()
    ClinicalTrialTask_3().process_new_data()
    ClinicalTrialTask_4().process_new_data()
    ClinicalTrialTask_5().process_new_data()
    ClinicalTrialTask_6().process_new_data()
    """


    # Update Memgraph database




def send_alert(look_back_days = 7):

    alertSender = AlertSender(look_back_days)
    alertSender.find_new_and_send_alert()




if __name__ == "__main__":

    ''' 1. '''
    #run_update()

    ''' 2. '''
    LOOK_BACK_DAYS = 7
    send_alert(LOOK_BACK_DAYS)

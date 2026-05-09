from dotenv import load_dotenv
load_dotenv()
  
# Add the project root to the Python path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.tools import _id_range_generator
from baseclass.init_base import InitBase
 
#
# ClinicalTrial.Reference - Publication relationship
# See rdas-memgraph/2_clinical_trial/initializer/reference.py
#
class ClinicalTrialToPublicationRelationshipInitializer(InitBase):

    def __init__(self): 
        pass

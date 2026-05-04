import os
import sys
import json
import spacy
import scispacy
from scispacy.linking import EntityLinker

# --- Installation Check and Model Loading ---
# Ensure you've installed all necessary scispaCy models:
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_core_sci_lg-0.5.3.tar.gz
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bionlp13cg_md-0.5.3.tar.gz
# pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.3/en_ner_bc5cdr_md-0.5.3.tar.gz

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from utils.tools import _val, _normalize_txt, _clean
from pipelines.pipeline_base import PipelineBase

"""
Insert NEW Clinical Trail nodes
"""
# Reference: B_clinical_trial/initializer/clinicaltrial.py

class ClinicalTrialGraphTask_7(PipelineBase):

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("ClinicalTrialTask_2 does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        pass
import os
import sys
import json
from typing import Dict, List, Any, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "../..")),
    os.path.abspath(os.path.join(_dir, "../../..")),
])

from utils.tools import _clean, _safe_get
from pipelines.pipeline_base import PipelineBase

"""
Create the clinical trial nodes to GARD nodes mapping
"""
# Reference: B_clinical_trial/initializer/clinicaltrial_gard_mapping.py

class NewClinicalTrialGardRelationshipTask(PipelineBase):
    """
    Create relationships from new ClinicalTrial nodes to GARD nodes.

    update_clinical_trial records preserve the disease search term that matched
    each trial. This task uses that term as matchedTermRDAS on the Memgraph
    relationship.
    """

    def __init__(self):
        """Initialize MySQL and Memgraph connections for relationship loading."""

        super().__init__(init_mysql=True, init_memgraph=True)


    # Not implemented
    def find_new_data(self, gard_node) -> None:
        raise NotImplementedError("NewClinicalTrialGardRelationshipTask does not implement find_new_data().")


    # implement
    def process_new_data(self) -> None:
        """Fetch new trial/GARD mappings and merge them into Memgraph."""

        ''' 
        Creates the edge only if that exact pattern does not already exist.
        Do nothing if the same relationship with the same matchedTermRDAS already exists
        '''
        batch_create = '''
            UNWIND $chunks AS chunk
            MATCH (x: GARD {gardId: chunk.gardId})
            MATCH (y: ClinicalTrial {nctId: chunk.nctId})
            MERGE (x)<-[:mapped_to_gard {matchedTermRDAS: chunk.disease}]-(y)
        '''

        # update_clinical_trial can contain multiple GARD matches per NCT ID;
        # is_new keeps this incremental task scoped to the current alert run.
        fetch_new_clinical_query = '''
                SELECT id, gardid, disease, nctid
                FROM update_clinical_trial
                WHERE nctid IS NOT NULL
                AND is_new = 1
        '''

        count = 0
        batch_num = 0
        batch_size = 200
        try:
            fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)
            fetch_cursor.execute(fetch_new_clinical_query)

            while True:
                rows = fetch_cursor.fetchmany(batch_size)

                if not rows:
                    self.logger.info(f"No more rows to fetch.")
                    break

                batch_num += 1
                self.logger.info(f'--- batch# = {batch_num} ---')

                chunks = []

                for row in rows:
                    gard_id = row['gardid']
                    disease = row['disease']
                    nctid = row['nctid']  

                    # These keys match the Cypher query above: source trial,
                    # target GARD node, and the matched disease term.
                    chunks.append({"nctId": nctid, "gardId": gard_id, "disease": disease})

                if len(chunks) > 0:
                    try:
                        self.memgraph.execute(batch_create, {"chunks": chunks})

                        count += len(chunks)
                        self.logger.info(f'Inserted {len(chunks)} mappings into memgraph. Total = {count}') 
                    except Exception as e:
                        self.logger.error(f"Error executing batch create: {e}") 
                else:
                    self.logger.info('No new mappings to insert into memgraph.')
  
        except Exception as e:
            self.logger.error(f"Error executing batch create: {e}")

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()
 

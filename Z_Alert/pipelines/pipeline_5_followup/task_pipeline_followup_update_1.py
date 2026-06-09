import os
import sys 
import time
from typing import Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase 

"""
Final Memgraph statistics update.

This task should run after the alert pipelines finish writing graph data. 
It recomputes the GARD/Disease relationship count properties used by the UI and downstream alert views.
"""

# Reference: G_update/GARD_relationships_statistics_updater.py


class GardRelationshipCountRefreshTask(PipelineBase):

    '''
    Each tuple is: (short log label, Cypher statement).
    Keep the Cypher here so process_new_data stays small and easy to audit.

    # If relationships can be deleted, changed, or reloaded: reset is needed for correctness.
    '''
    COUNTING_COMMANDS: Tuple[Tuple[str, str], ...] = (
        (
            'Ensure GARD nodes also have the Disease label',
            '''
            MATCH (n:GARD)
            SET n:Disease
            ''',
        ),
        (
            'Reset GARD/Disease relationship counts before recomputing',
            '''
            MATCH (n:Disease)
            SET n += {
                countArticles: 0,
                countTrials: 0,
                countCoreProjects: 0,
                countProjects: 0,
                countGenes: 0,
                countPhenotypes: 0
            }
            ''',
        ),
        (
            'Reset Gene disease counts before recomputing',
            '''
            MATCH (n:Gene)
            SET n.countDiseases = 0
            ''',
        ),
        (
            'Reset Phenotype disease counts before recomputing',
            '''
            MATCH (n:Phenotype)
            SET n.countDiseases = 0
            ''',
        ),
        (
            'Set countArticles on Disease nodes',
            '''
            MATCH (n:Disease)-[r]-(a:Article)
            WITH count(r) AS counts, n
            SET n.countArticles = counts
            ''',
        ),
        (
            'Set countTrials on Disease nodes',
            '''
            MATCH (n:Disease)-[r]-(a:ClinicalTrial)
            WITH count(r) AS counts, n
            SET n.countTrials = counts
            ''',
        ),
        (
            'Set countCoreProjects on Disease nodes',
            '''
            MATCH (n:Disease)-[r]-(a:CoreProject)
            WITH count(r) AS counts, n
            SET n.countCoreProjects = counts
            ''',
        ),
        (
            'Set countProjects on Disease nodes',
            '''
            MATCH (n:Disease)-[r]-(a:Project)
            WITH count(r) AS counts, n
            SET n.countProjects = counts
            ''',
        ),
        (
            'Set countGenes on Disease nodes',
            '''
            MATCH (n:Disease)-[r]-(a:Gene)
            WITH count(r) AS counts, n
            SET n.countGenes = counts
            ''',
        ),
        (
            'Set countPhenotypes on Disease nodes',
            '''
            MATCH (n:Disease)-[r]-(a:Phenotype)
            WITH count(r) AS counts, n
            SET n.countPhenotypes = counts
            ''',
        ),
        (
            'Set countDiseases on Gene nodes',
            '''
            MATCH (n:Gene)-[r]-(d:Disease)
            WITH count(r) AS diseaseCount, n
            SET n.countDiseases = diseaseCount
            ''',
        ),
        (
            'Set countDiseases on Phenotype nodes',
            '''
            MATCH (n:Phenotype)-[r]-(d:Disease)
            WITH count(r) AS diseaseCount, n
            SET n.countDiseases = diseaseCount
            ''',
        ),
    )


    def __init__(self):
        super().__init__(init_mysql=False, init_memgraph=True) 


    # Not implemented
    def find_new_data(self, gard_node) -> None:        
        raise NotImplementedError("GardRelationshipCountRefreshTask does not implement find_new_data().")
    

    # implement
    def process_new_data(self) -> None:

        start_time = time.time()

        try:
            for command_num, (description, cypher) in enumerate(self.COUNTING_COMMANDS, 1):

                command_start = time.time()

                self.logger.info(f"Executing command {command_num}/{len(self.COUNTING_COMMANDS)}: {description}" )
                self.logger.info(cypher)

                ''' 
                Execute each statistics command separately so the log shows exactly which graph update failed if Memgraph raises an error.
                '''
                self.memgraph.execute(cypher)

                elapsed = time.time() - command_start
                self.logger.info(f"Command {command_num} completed in {elapsed:.2f} seconds.")

            total_elapsed = time.time() - start_time
            self.logger.info(f"Completed pipeline final statistics update in {total_elapsed:.2f} seconds.")

        except Exception as e:
            self.logger.error(f"Error updating final graph statistics: {e}")
            raise

        finally:
            ''' Explicitly close all db connections. '''
            self.close()


if __name__ == '__main__':

    task = GardRelationshipCountRefreshTask()
    task.process_new_data()

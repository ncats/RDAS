import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase

"""
Grant person extraction task placeholder.
"""


class NewGrantPersonTask(PipelineBase):

    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=False)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("NewGrantPersonTask does not use find_new_data().")


    def process_new_data(self) -> None:
        self.logger.info("NewGrantPersonTask is empty for now.")

        ''' Explicitly close all db connections. '''
        self.close()

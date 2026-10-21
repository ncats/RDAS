"""Reuse GARD/OMIM associations that remain in the current GARD source.

Invocation: run_omim_updates_for_gard_node(gard_id) calls this task after direct
clinical-trial/publication discovery and before OMIM entry retrieval.

Example association shape:
    gard_id = "GARD:0000001"
    current_label_xrefs = "OMIM:603358"
    source_mapping = {"gard_id": "GARD:0000001", "omim_id": 603358}
    copied_mapping = {"gard_id": "GARD:0000001", "omim_id": 603358, "is_new": 1}

The source pair is eligible only if the NEW GARD data still lists OMIM:603358.
An old source association removed from current cross-references is excluded.
The inherited mapping task then adds new current cross-references absent from
production, using its existing NOT EXISTS behavior. Other GARD IDs are never
imported merely because they share an OMIM ID. The values show conditional copy
behavior, not a claim that a particular source row currently exists."""

from pipelines.pipeline_3_publication.task_publication_3 import GardOmimPublicationMappingTask


class GARDOMIMPublicationMappingTask(GardOmimPublicationMappingTask):
    """Copy current source associations and let shared mapping add new GARD xrefs."""

    __module__ = GardOmimPublicationMappingTask.__module__

    def __init__(self, reuse, gard_id):

        if gard_id != reuse.current_gard_id:
            raise ValueError("OMIM mapping tasks require the current GARD node.")
        super().__init__(gard_id=gard_id)
        self.reuse = reuse

    def process_new_data(self):

        '''
        Reuse only source pairs still present in current GARD cross-references.
        The inherited task keeps its normal NOT EXISTS behavior for any pairs
        absent from production, without changing the regular Alert task.
        '''
        self.reuse.copy_gard_omim_mappings(self.mysql, self.logger, self.gard_id)
        super().process_new_data()

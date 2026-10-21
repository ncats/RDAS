"""Copy current-GARD OMIM entries before requesting missing entries from the API.

Invocation: run_omim_updates_for_gard_node(gard_id) calls this immediately after
GARD/OMIM mapping. Only OMIM IDs mapped to the current GARD node are eligible.

Illustrative cache example:
    current_mapping = {"gard_id": "GARD:0000001", "omim_id": 603358}
    cached_entry = {
        "omim": {"entryList": [{"entry": {"mimNumber": 603358}}]}
    }
    destination_entry = {"omim_id": 603358, "entry_json": cached_entry, "is_new": 1}

The helper verifies mimNumber, preserves existing destination JSON, and copies
reference pairs only when they agree with the chosen JSON. Missing or invalid
source entries remain eligible for the inherited API retrieval task. Relevant
existing OMIM entries are restaged for PMID import and OMIMRef graph processing,
since another GARD node may already have consumed their previous is_new flags.
The minimal example omits fields normally present in a complete API payload."""

from pipelines.pipeline_3_publication.task_publication_4 import PublicationOminDataRetrievalTask


class GARDOMIMDataRetrievalTask(PublicationOminDataRetrievalTask):
    """Reuse current GARD's OMIM JSON/reference pairs before missing-entry HTTP calls."""

    __module__ = PublicationOminDataRetrievalTask.__module__

    def __init__(self, reuse, gard_id):

        if gard_id != reuse.current_gard_id:
            raise ValueError("OMIM retrieval tasks require the current GARD node.")
        super().__init__(gard_id=gard_id)
        self.reuse = reuse

    def process_new_data(self):

        '''
        The shared retrieval query already excludes OMIM IDs in the target.
        Copying before it runs avoids those API calls; genuinely absent or
        invalid source entries continue through the normal downloader.
        '''
        self.reuse.copy_omim_data(self.mysql, self.logger, self.gard_id)
        super().process_new_data()

"""Search complete PubMed ID slices and reuse publications for the current GARD.

Invocation: publication discovery immediately follows clinical-trial discovery
for the same GARD record. The inherited search retains its date splitting, so a
count greater than the returned ID list cannot silently truncate the import.

Example input for a coma search:
    gard_id = "GARD:0000016"
    search_term = "Coma"
    pubmed_ids = ["9282845", "9280151"]

Assuming PMID 9282845 has usable source metadata and 9280151 is missing from both
databases, reuse the first and let the shared importer download the second.
Both mappings must be (GARD:0000016, Coma, PMID), regardless of which other GARD
IDs were associated with the articles in production. Already completed target
Articles stay completed; new disease edges are handled by the relationship task.

After all live slices for this term are consumed, copy compatible complete search
history. A historical coma row with total=41918 and only 9999 IDs is rejected.
Live raw search responses take precedence over historical rows with the same key.
These are illustrative cache states, not statements about current database rows."""

from pipelines.pipeline_3_publication.task_publication_1 import NewPublicationDiscoveryTask


class GARDPublicationDiscoveryTask(NewPublicationDiscoveryTask):
    """Retain complete PubMed ID searches and create current-GARD mappings."""

    __module__ = NewPublicationDiscoveryTask.__module__

    def __init__(self, reuse):

        super().__init__()
        self.reuse = reuse

    def find_new_publications(self, gard_id, search_term, pubmed_ids):

        '''
        Copy complete cached metadata and available enrichment before the shared
        importer checks for missing Articles. Its bounded HTTP pool downloads
        only remaining PMIDs and creates mappings using this gard_id and term.
        Matching source mapping rows are copied only for this exact GARD/term;
        the shared importer adds associations missing from that cache.
        '''
        if gard_id != self.reuse.current_gard_id:
            raise ValueError("Publication mappings must use the current GARD node.")
        self.reuse.copy_publications(self.mysql, self.logger, pubmed_ids, search_term=search_term)
        super().find_new_publications(gard_id, search_term, pubmed_ids)

    def retrieve_pubmed_ids(self, gard_id, search_term, mindate=None, maxdate=None):

        '''
        Preserve the shared live search and complete date splitting. Copy
        compatible source search history after every live slice has committed,
        so historical keys cannot block later live response inserts. The helper
        checks current mappings in batches rather than accumulating all PMIDs.
        '''
        if gard_id != self.reuse.current_gard_id:
            raise ValueError("Publication searches must use the current GARD node.")
        for pubmed_ids in super().retrieve_pubmed_ids(gard_id, search_term, mindate, maxdate):
            yield pubmed_ids
        self.reuse.copy_publication_search_history(self.mysql, self.logger, gard_id, search_term, mindate, maxdate)

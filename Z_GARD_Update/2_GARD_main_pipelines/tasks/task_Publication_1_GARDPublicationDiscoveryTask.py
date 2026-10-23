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

Search history in publication_gard_pubmed comes only from the inherited live
PubMed search. Do not copy source search responses: an older 1970-2025 snapshot
for gracile syndrome is a different search from today's 1971-01-01-2026-10-23
request and would add an unnecessary second date range. Reusing Article metadata
and enrichment below does not require copying those historical search responses.
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

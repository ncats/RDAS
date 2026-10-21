"""Import/reuse PubMed Articles cited by the current GARD node's OMIM entries.

Invocation: run_publication_mysql_updates() runs this before classification,
PubTator, substance extraction, and abbreviation filtering. This makes OMIM-only
Articles available to enrichment and graph tasks in the same GARD iteration.

Illustrative JSON shape; the PMID is not asserted to cite this OMIM entry:
    current_mapping = {"gard_id": "GARD:0000001", "omim_id": 603358, "is_new": 1}
    entry_json = {"omim": {"entryList": [{"entry": {
        "mimNumber": 603358,
        "referenceList": [{"reference": {"pubmedID": 9282845}}],
    }}]}}

The inherited parser saves (603358, 9282845) OMIM/PMID provenance. Before its
missing-Article downloader runs, this subclass copies any reusable Article and
enrichment records for PMID 9282845. The OMIM selector includes only current-GARD
mappings marked new; pending mappings for another disease do not expand its work.
Article metadata stays shared by PMID rather than being duplicated per disease."""

from pipelines.pipeline_3_publication.task_publication_8 import NewOmimPublicationArticleImportTask


class GARDOMIMPublicationArticleImportTask(NewOmimPublicationArticleImportTask):
    """Apply the same PMID cache lookup to the current GARD node's OMIM references."""

    __module__ = NewOmimPublicationArticleImportTask.__module__

    def __init__(self, reuse):

        super().__init__()
        self.reuse = reuse

    def get_unique_new_omim_id_list(self):

        '''
        A restarted build may contain pending associations from earlier nodes.
        Keep this import tied to CURRENT GARD mappings; the helper has staged
        their OMIM JSON even when a shared OMIM entry was completed previously.
        '''
        cursor = self.mysql.cursor(buffered=True)
        try:
            cursor.execute(
                '''SELECT DISTINCT omim_id FROM publication_gard_omim_mapping
                   WHERE gard_id = %s AND is_new = 1 AND omim_id IS NOT NULL''',
                (self.reuse.current_gard_id,),
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _retrieve_missing_publication_articles(self, pubmed_ids):

        '''
        The inherited task creates provenance only for the staged OMIM entries.
        Reuse each referenced Article before its missing-PMID downloader runs;
        no unrelated source OMIM/GARD mappings are imported.
        '''
        self.reuse.copy_publications(self.mysql, self.logger, pubmed_ids)
        super()._retrieve_missing_publication_articles(pubmed_ids)

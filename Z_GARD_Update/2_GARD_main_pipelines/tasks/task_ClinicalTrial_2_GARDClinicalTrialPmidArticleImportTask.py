"""Reuse Articles referenced by trials before downloading missing trial citations.

Invocation: run_clinical_trial_mysql_updates() calls this after the shared trial
publication-mapping task has extracted the current studies' reference PMIDs.

Illustrative reference pairs; these do not assert real trial/publication links:
    pairs = [
        {"nctid": "NCT01234567", "pubmed_id": 9282845, "is_new": 1},
        {"nctid": "NCT01234567", "pubmed_id": 9280151, "is_new": 1},
    ]

A cited PMID need not appear in the disease's direct PubMed search. The helper
therefore gives these reference PMIDs the same Article/PubTator/substance reuse
lookup. The inherited importer downloads Articles still absent from the target.
No artificial disease search-term mapping is invented for a trial-only citation;
its disease association is represented by the current GARD -> trial -> PMID path.
The helper and inherited task retain parent-owned database writes."""

from pipelines.pipeline_2_clinical_trial.task_clinical_trial_5 import ClinicalTrialPmidArticleImportTask


class GARDClinicalTrialPmidArticleImportTask(ClinicalTrialPmidArticleImportTask):
    """Reuse publication records referenced by copied or newly downloaded trials."""

    __module__ = ClinicalTrialPmidArticleImportTask.__module__

    def __init__(self, reuse):

        super().__init__()
        self.reuse = reuse

    def process_new_data(self):

        '''
        Trial reference PMIDs can be absent from the disease's PubMed search.
        Copy their cached article/annotation data too, then let the existing
        importer download references absent from both databases.
        '''
        self.reuse.copy_trial_publications(self.mysql, self.logger)
        super().process_new_data()

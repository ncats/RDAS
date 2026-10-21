"""Separate per-node organization sources from final person affiliations.

First invocation: per-node run_pipeline_maintenance() substitutes this task for
the shared source tracker. It is also invoked later with person_only=True,
after final person grouping and Agent graph updates. Its number records its
FIRST invocation, not every subsequent use of the same class.

Illustrative input states:
    trial = {"nctid": "NCT01234567", "is_new": 1}
    person = {"id": 42, "affiliation": "National Institutes of Health", "is_new": 1}

With person_only=False, consume staged trial/project organization sources before
source flags are cleared; leave accumulated person affiliations pending. With
person_only=True, consume the person backlog once after all GARD nodes finish.
This prevents repeatedly scanning a growing person backlog for every disease.
The inherited methods retain their bounded reads and organization identity rules;
this task controls which source family runs, without clearing person flags."""

from pipelines.pipeline_7_graph_maintenance.task_pipeline_maintenance_3 import NewOrganizationSourceTrackingTask


class GARDOrganizationSourceTrackingTask(NewOrganizationSourceTrackingTask):
    """Separate per-node trial/project sources from final person affiliations."""

    __module__ = NewOrganizationSourceTrackingTask.__module__

    def __init__(self, person_only: bool = False):

        super().__init__()
        self.person_only = person_only


    def process_new_data(self) -> None:

        '''
        Pending person flags accumulate until finalization. Scanning them in
        every per-node maintenance step would replay a growing backlog. Track
        trial/project sources before their flags are cleared, and consume the
        person affiliation backlog once in the final phase instead.
        '''
        try:
            if self.person_only:
                self.track_person_sources()
            else:
                self.track_clinical_trial_sources()
                self.track_core_project_sources()
        finally:
            self.close()

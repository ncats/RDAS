import os
import sys
import time

from dotenv import load_dotenv

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, ".")),
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])
load_dotenv(os.path.abspath(os.path.join(_dir, "..", ".env")))

from pipeline_runner_base import PipelineRunnerBase
from pipelines.pipeline_4_grant.grant_base import GrantPipelineBase
from utils.tools import _time_hms



# This year's data may not be published yet.
LAST_YEAR = GrantPipelineBase.LATEST_COMPLETED_REPORTER_YEAR

class GrantPipelineRunner(PipelineRunnerBase):
    """Run grant-specific alert pipeline tasks without touching Z_Alert/main.py."""

    """ Step 1 """
    def download_reporters_data(self) -> None:
         
        """ Download projects, abstracts, publications, linktables files by year. """
        from pipelines.pipeline_4_grant.task_grant_1 import GrantExporterDownloadTask
        self._run_pipeline_task(GrantExporterDownloadTask, years=[LAST_YEAR], )
 
        """ Download NIH RePORTER patent and clinical-study files """
        from pipelines.pipeline_4_grant.task_grant_2 import GrantPatentClinicalStudyDownloadTask
        self._run_pipeline_task(GrantPatentClinicalStudyDownloadTask)


    """ Step 2 """
    def upload_reporters_data_to_mysql(self) -> None:

        """ Upload project files into MySQL. """
        from pipelines.pipeline_4_grant.task_grant_3 import GrantProjectUploadTask
        self._run_pipeline_task(GrantProjectUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_4 import GrantPublicationUploadTask
        self._run_pipeline_task(GrantPublicationUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_5 import GrantAbstractUploadTask       
        self._run_pipeline_task(GrantAbstractUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_6 import GrantLinktableUploadTask
        self._run_pipeline_task(GrantLinktableUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_7 import GrantClinicalStudyUploadTask
        self._run_pipeline_task(GrantClinicalStudyUploadTask, years=[LAST_YEAR])

        from pipelines.pipeline_4_grant.task_grant_8 import GrantPatentUploadTask
        self._run_pipeline_task(GrantPatentUploadTask)


    """ Step 3 """
    def build_relationship(self) -> None:

        # Only if `rdas_db.gard` table changes, REBUILD or REFRESH `grant_gard_processed_names`
        # so the processed search terms stay aligned with the source GARD labels.
        from pipelines.pipeline_4_grant.task_grant_9 import GrantGardNameProcessingTask
        #self._run_pipeline_task(GrantGardNameProcessingTask)

        from pipelines.pipeline_4_grant.task_grant_10 import GrantGardProjectRelationshipTask
        self._run_pipeline_task(GrantGardProjectRelationshipTask)

        from pipelines.pipeline_4_grant.task_grant_11 import GrantProjectAnnotationTask
        self._run_pipeline_task(GrantProjectAnnotationTask)

        
    """ Step 5 """
    def graph_updates(self) -> None:

        from pipelines.pipeline_4_grant.task_grant_graph_1 import NewProjectGraphTask
        self._run_pipeline_task(NewProjectGraphTask)

        from pipelines.pipeline_4_grant.task_grant_graph_2 import NewGardProjectRelationshipGraphTask
        self._run_pipeline_task(NewGardProjectRelationshipGraphTask)

        from pipelines.pipeline_4_grant.task_grant_graph_3 import NewCoreProjectGraphTask
        self._run_pipeline_task(NewCoreProjectGraphTask)

        from pipelines.pipeline_4_grant.task_grant_graph_4 import NewPatentGraphTask
        self._run_pipeline_task(NewPatentGraphTask)

        from pipelines.pipeline_4_grant.task_grant_graph_5 import NewGrantAnnotationGraphTask
        self._run_pipeline_task(NewGrantAnnotationGraphTask)

        from pipelines.pipeline_4_grant.task_grant_graph_6 import NewCoreProjectGardRelationshipGraphTask
        self._run_pipeline_task(NewCoreProjectGardRelationshipGraphTask)


        """
        Notes:
            This NewCoreProjectArticleRelationshipGraphTask expects Article and CoreProject nodes to already exist in Memgraph.
            Run the publication NewPublicationArticleGraphTask and task_grant_graph_3.py before this
            task so both MATCH clauses can create the relationship.
        """
        ''' Re-use '''
        from pipelines.pipeline_3_publication.task_publication_graph_1 import NewPublicationArticleGraphTask
        self._run_pipeline_task(NewPublicationArticleGraphTask)

        from pipelines.pipeline_4_grant.task_grant_graph_7 import NewCoreProjectArticleRelationshipGraphTask
        self._run_pipeline_task(NewCoreProjectArticleRelationshipGraphTask)

        from pipelines.pipeline_4_grant.task_grant_graph_8 import NewCoreProjectClinicalTrialRelationshipGraphTask
        self._run_pipeline_task(NewCoreProjectClinicalTrialRelationshipGraphTask)

        from pipelines.pipeline_4_grant.task_grant_graph_9 import NewFundingIcGraphTask
        self._run_pipeline_task(NewFundingIcGraphTask) 

        

    """ Step 4 """
    def followup_updates(self) -> None:

        from pipelines.pipeline_4_grant.task_grant_12 import GrantPublicationArticleImportTask
        self._run_pipeline_task(GrantPublicationArticleImportTask)

        ''' Re-use '''
        from pipelines.pipeline_3_publication.task_publication_2_local import PublicationEpiNhsClassificationTask
        self._run_pipeline_task(PublicationEpiNhsClassificationTask)



    """ Step 6 """
    def run_pipeline_wrapup(self) -> None:
        from pipelines.pipeline_4_grant.task_grant_pipeline_wrapup import GrantPipelineWrapUpTask 
        self._run_pipeline_task(GrantPipelineWrapUpTask)


class GARDGrantUpdateRunner(GrantPipelineRunner):
    """Refresh GARD grant search terms before the shared grant stages."""

    '''
    A failed MySQL or graph task must stop this update before wrap-up clears
    is_new flags. The regular Alert grant runner retains its existing policy.
    '''
    raise_task_errors = True

    def build_relationship(self) -> None:

        """Rebuild GARD search names, then run the shared relationship tasks."""

        from baseclass.conn import DBConnection
        from pipelines.pipeline_4_grant.task_grant_9 import GrantGardNameProcessingTask

        mysql = DBConnection().mysql_conn()
        if mysql is None:
            raise ConnectionError("Unable to connect to MySQL for grant GARD name refresh.")

        cursor = None
        try:
            cursor = mysql.cursor()
            cursor.execute("SELECT COUNT(*) FROM gard")
            gard_row_count = cursor.fetchone()[0]
            if gard_row_count == 0:
                raise RuntimeError("The gard table is empty; complete the GARD MySQL update first.")

            '''
            GrantGardNameProcessingTask updates derived search columns but
            does not add source GARD rows. Rebuild those rows first using the
            historical grant grouping. Keep DELETE and INSERT in one
            transaction so a failed insert can restore the previous rows.
            '''
            cursor.execute("SET SESSION group_concat_max_len = 1048576")
            cursor.execute("DELETE FROM grant_gard_processed_names")
            cursor.execute('''
                INSERT INTO grant_gard_processed_names
                    (GardID, name, synonyms, data_source)
                SELECT
                    GardID,
                    MAX(CASE WHEN Label_Predicate_Type = 'Name' THEN Label END),
                    GROUP_CONCAT(
                        CASE WHEN Label_Predicate_Type = 'Synonym' THEN Label END
                        SEPARATOR '$$$'
                    ),
                    Label_Source
                FROM gard
                WHERE Label_Predicate_Mapping != 'DEPRECATED'
                    AND LENGTH(Label) > 3
                GROUP BY GardID, MONDO_ID, Label_Source
            ''')
            inserted_count = cursor.rowcount
            if inserted_count == 0:
                raise RuntimeError("No grant GARD name rows were built from the gard table.")

            mysql.commit()
            self.logger.info(f"Refreshed {inserted_count} grant_gard_processed_names rows from gard.")

        except Exception:
            mysql.rollback()
            self.logger.exception("Unable to refresh grant GARD names from gard.")
            raise

        finally:
            if cursor is not None:
                cursor.close()
            mysql.close()

        '''
        Derive normalized terms from the rebuilt rows before the existing
        project-matching task reads them. The base method then runs the same
        relationship and annotation tasks as the regular Alert grant runner.
        '''
        self._run_pipeline_task(GrantGardNameProcessingTask)
        super().build_relationship()


def run_grant_pipeline(runner: GrantPipelineRunner) -> None:

    """Run the shared grant stages for the Alert and GARD entry points."""

    '''
    Dispatch each stage through the supplied runner. The GARD runner overrides
    build_relationship() to refresh GARD names before matching grant projects;
    both entry points then execute the same remaining tasks in the same order.
    '''
    runner._run_step_with_timing("Step 1: download_reporters_data()", runner.download_reporters_data)
    runner._run_step_with_timing("Step 2: upload_reporters_data_to_mysql()", runner.upload_reporters_data_to_mysql)
    runner._run_step_with_timing("Step 3: build_relationship()", runner.build_relationship)

    '''
    GrantPublicationArticleImportTask inserts missing grant-linked PubMed rows
    into publication_article with is_new = 1. Import and classify those rows
    before the graph stage, which creates Article nodes and then connects them
    to CoreProject nodes.
    '''
    runner._run_step_with_timing("Step 4: followup_updates()", runner.followup_updates)
    runner._run_step_with_timing("Step 5: graph_updates()", runner.graph_updates)
    runner._run_step_with_timing("Step 6: run_pipeline_wrapup()", runner.run_pipeline_wrapup)


if __name__ == "__main__":

    total_run_start_time = time.time()

    runner = GrantPipelineRunner()

    try:
        runner.logger.info(f"Grant pipeline generated latest completed year={LAST_YEAR}.")
        run_grant_pipeline(runner)


    finally:
        total_hours, total_minutes, total_seconds = _time_hms(time.time() - total_run_start_time)
        runner.logger.info(
            f"\n\n{'=' * 20} Total grant pipeline run time: "
            f"{total_hours} hours, {total_minutes} minutes, {total_seconds} seconds "
            f"{'=' * 20}\n\n"
        )

        """ Flush and close the runner logger. """
        runner.close()

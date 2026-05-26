import os
import sys 
import json
import time
from collections import Counter
from typing import Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase 
from utils.tools import _as_bool, _time_hms
 
# Reference: disease-counts.json

class DiseaseCountsRefreshTask(PipelineBase):


    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    '''
    diseaseArticleByEpi, diseaseArticleByNHS & diseaseArticleByYear
    '''
    disease_article_query = '''
        SELECT
            pgspm.gard_id,
            pa.pubmed_id,
            MAX(pa.publication_year) AS publication_year,
            MAX(LOWER(COALESCE(pa.is_EPI, '')) IN ('1', 'true', 't', 'yes', 'y')) AS is_EPI,
            MAX(LOWER(COALESCE(pa.is_NHS, '')) IN ('1', 'true', 't', 'yes', 'y')) AS is_NHS
        FROM publication_gard_searchterm_pubmed_mapping AS pgspm
        INNER JOIN publication_article AS pa
            ON pa.pubmed_id = pgspm.pubmed_id
        WHERE pgspm.gard_id = %s
        AND pgspm.pubmed_id IS NOT NULL
        AND pa.publication_year IS NOT NULL
        AND (pgspm.is_valid IS NULL OR pgspm.is_valid = 1) 
        GROUP BY pgspm.gard_id, pa.pubmed_id
    '''

    '''
    diseaseProjectsByYear 
    '''
    disease_project_query = '''
        SELECT
            related.project_year,
            COUNT(*) AS project_count
        FROM (
            SELECT DISTINCT
                p.APPLICATION_ID,
                p.FY AS project_year
            FROM grant_gard_project_relation AS gpr
            INNER JOIN grant_project AS p
                ON p.APPLICATION_ID = gpr.application_id
            WHERE gpr.gard_id = %s
            AND gpr.application_id IS NOT NULL
            AND p.FY IS NOT NULL
        ) AS related
        GROUP BY
            related.project_year
        ORDER BY
            related.project_year DESC
    '''

    ''' 
    diseaseTrialsByPhase, diseaseTrialsByStatus & diseaseTrialsByType
    '''
    fetch_nctid_by_gard_id_query = '''
        SELECT distinct nctid 
        FROM clinical_trial 
        WHERE gardId = %s
        AND  nctid is not null;
    '''

    disease_clinical_trial_query = '''        
        
        WITH $nctids AS nctids

        MATCH (ct:ClinicalTrial)
        WHERE ct.nctId IN nctids

        WITH collect(ct) AS trials

        UNWIND trials AS ct
        WITH
            trials,
            CASE
                WHEN ct.phase IS NULL OR ct.phase = "" THEN "NA"
                ELSE ct.phase
            END AS phase_term,
            count(*) AS phase_count
        ORDER BY phase_term
        WITH
            trials,
            collect({
                term: phase_term,
                count: phase_count
            }) AS phase

        UNWIND trials AS ct
        WITH
            trials,
            phase,
            CASE
                WHEN ct.overallStatus IS NULL OR ct.overallStatus = "" THEN "UNKNOWN"
                ELSE ct.overallStatus
            END AS overallStatus_term,
            count(*) AS overallStatus_count
        ORDER BY overallStatus_count DESC, overallStatus_term
        WITH
            trials,
            phase,
            collect({
                term: overallStatus_term,
                count: overallStatus_count
            }) AS overallStatus

        UNWIND trials AS ct
        WITH
            phase,
            overallStatus,
            CASE
                WHEN ct.studyType IS NULL OR ct.studyType = "" THEN "UNKNOWN"
                ELSE ct.studyType
            END AS studyType_term,
            count(*) AS studyType_count
        ORDER BY studyType_count DESC, studyType_term
        WITH
            phase,
            overallStatus,
            collect({
                term: studyType_term,
                count: studyType_count
            }) AS studyType

        RETURN {
            phase: phase,
            overallStatus: overallStatus,
            studyType: studyType
        } AS result
    '''


    def find_new_data(self, gard_node) -> None:
        self.logger.info("DiseaseCountsRefreshTask does not use find_new_data().")


    def process_new_data(self) -> None:
        
        batch_num = 0
        batch_size = 50       
        total_updated = 0
        very_start_time = time.time()

        fetch_GARD_nodes_cypher = '''
            MATCH (g:GARD)
            WHERE g.gardId IS NOT NULL
            RETURN g.gardId AS gard_id
            ORDER BY g.gardId
            SKIP $skip LIMIT $limit
        '''

        batch_update_GARD_nodes_cypher = '''
            unwind $batch as item
            match (g:GARD {gardId: item.gard_id})
            set g.filterCounts = item.data
        '''

        fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)

        try:
            while True:
                batch_start_time = time.time()

                params = {
                    "skip": batch_num * batch_size,
                    "limit": batch_size,
                }
                results = list(self.memgraph.execute_and_fetch(fetch_GARD_nodes_cypher, params))
 
                if not results:
                    break
 
                gard_id_list = [row.get("gard_id") for row in results if row.get("gard_id")]
                
                batch_num += 1 
                self.logger.info(f'\n--- Processing batch {batch_num} ---\n')

                batch = []
                for gard_id in gard_id_list:
                    self.logger.info(f'Processing GARD ID: {gard_id}')

                    disease_article_iterms_count = self._disease_article_iterms_count(gard_id, fetch_cursor)
                    disease_project_by_year_count = self._disease_project_by_year_count(gard_id, fetch_cursor)
                    disease_clinical_trial_terms_count = self._disease_clinical_trial_terms_count(gard_id, fetch_cursor)

                    total_updated += 1

                    obj = {'gard_id': gard_id,
                                  "data": {
                                        **disease_article_iterms_count,
                                        **disease_project_by_year_count,
                                        **disease_clinical_trial_terms_count,
                                    }
                                }
                    # ** dict unpacking
                    batch.append(obj)                    
                    self.logger.info(f'\n{json.dumps(obj, ensure_ascii=False)}')                 

                if batch:
                    try:
                        self.memgraph.execute(batch_update_GARD_nodes_cypher, {"batch": batch})
                    except Exception as e:
                        self.logger.error(f'{e}')

                hours, minutes, seconds = _time_hms(time.time() - batch_start_time)
                self.logger.info(f'\n *** Total updated={total_updated}. Time={hours} hours, {minutes} minutes, {seconds} seconds *** \n')
                    
        except Exception as e:
            self.logger.error(f'{e}')

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            ''' Explicitly close all db connections. '''
            self.close()

            hours, minutes, seconds = _time_hms(time.time() - very_start_time)
            self.logger.info(f'\n\n****** Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds ******\n\n')


    def _disease_article_iterms_count(self, gard_id: str, fetch_cursor):
        """
        Return article count buckets for one GARD ID.

        The shape follows disease-counts.json:
            diseaseArticleByEpi: [{term: "true", count: n}, ...]
            diseaseArticleByNHS: [{term: "true", count: n}, ...]
            diseaseArticleByYear: [{term: "2026", count: n}, ...]
        """

        epi_counts = Counter({"true": 0, "false": 0})
        nhs_counts = Counter({"true": 0, "false": 0})
        year_counts = Counter()

        fetch_cursor.execute(self.disease_article_query, (gard_id,))

        for row in fetch_cursor.fetchall():
            epi_term = "true" if _as_bool(row.get("is_EPI")) else "false"
            nhs_term = "true" if _as_bool(row.get("is_NHS")) else "false"
            publication_year = row.get("publication_year")

            epi_counts[epi_term] += 1
            nhs_counts[nhs_term] += 1

            if publication_year is not None:
                year_counts[str(publication_year)] += 1

        return {
            "diseaseArticleByEpi": [
                {"count": epi_counts["true"], "term": "true"},
                {"count": epi_counts["false"], "term": "false"},
            ],
            "diseaseArticleByNHS": [
                {"count": nhs_counts["true"], "term": "true"},
                {"count": nhs_counts["false"], "term": "false"},
            ],
            "diseaseArticleByYear": [
                {"count": count, "term": year}
                for year, count in sorted(
                    year_counts.items(),
                    key=lambda item: int(item[0]),
                    reverse=True,
                )
            ],
        }


    def _disease_project_by_year_count(self, gard_id: str, fetch_cursor):
        """
        Return project count buckets for one GARD ID.

        The shape follows disease-counts.json:
            diseaseProjectsByYear: [{term: "2023", count: n}, ...]
        """

        fetch_cursor.execute(self.disease_project_query, (gard_id,))
        rows = fetch_cursor.fetchall()

        return {
            "diseaseProjectsByYear": [
                {
                    "term": str(row.get("project_year")),
                    "count": int(row.get("project_count") or 0),
                }
                for row in rows
                if row.get("project_year") is not None
            ]
        }
        

    def _disease_clinical_trial_terms_count(self, gard_id: str, fetch_cursor):
        """
        Return clinical-trial count buckets for one GARD ID.

        The shape follows disease-counts.json:
            diseaseTrialsByPhase: [{term: "PHASE3", count: n}, ...]
            diseaseTrialsByStatus: [{term: "COMPLETED", count: n}, ...]
            diseaseTrialsByType: [{term: "INTERVENTIONAL", count: n}, ...]
        """

        _empty = {
            "diseaseTrialsByPhase": [],
            "diseaseTrialsByStatus": [],
            "diseaseTrialsByType": [],
        }


        fetch_cursor.execute(self.fetch_nctid_by_gard_id_query, (gard_id,))

        rows = fetch_cursor.fetchall()

        nctid_list = sorted({row.get("nctid") for row in rows if row.get("nctid")})

        if not nctid_list:
            return _empty

        results = list(
            self.memgraph.execute_and_fetch(
                self.disease_clinical_trial_query,
                {"nctids": nctid_list},
            )
        )

        if not results:
            return _empty

        result = results[0].get("result") or {}

        return {
            "diseaseTrialsByPhase": result.get("phase") or [],
            "diseaseTrialsByStatus": result.get("overallStatus") or [],
            "diseaseTrialsByType": result.get("studyType") or [],
        }



if __name__ == '__main__':

    task = DiseaseCountsRefreshTask()
    task.process_new_data()
    task.close()

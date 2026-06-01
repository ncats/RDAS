import os
import sys 
import json
import time
from typing import Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _time_hms
 
# Reference: disease-counts.json

class DiseaseCountsRefreshTask(PipelineBase):


    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    '''
    diseaseArticleByEpi, diseaseArticleByNHS & diseaseArticleByYear
    '''
    disease_article_query = '''
        WITH article_rows AS (
            SELECT
                pgspm.gard_id,
                pa.pubmed_id,
                MAX(pa.publication_year) AS publication_year,
                MAX(LOWER(COALESCE(pa.is_EPI, '')) IN ('1', 'true', 't', 'yes', 'y')) AS is_EPI,
                MAX(LOWER(COALESCE(pa.is_NHS, '')) IN ('1', 'true', 't', 'yes', 'y')) AS is_NHS
            FROM publication_gard_searchterm_pubmed_mapping AS pgspm
            INNER JOIN publication_article AS pa
                ON pa.pubmed_id = pgspm.pubmed_id
            WHERE pgspm.gard_id IN ({placeholders})
            AND pgspm.pubmed_id IS NOT NULL
            AND pa.publication_year IS NOT NULL
            AND (pgspm.is_valid IS NULL OR pgspm.is_valid = 1)
            GROUP BY pgspm.gard_id, pa.pubmed_id
        )

        SELECT
            gard_id,
            'diseaseArticleByEpi' AS metric_name,
            CASE WHEN is_EPI = 1 THEN 'true' ELSE 'false' END AS term,
            COUNT(*) AS item_count
        FROM article_rows
        GROUP BY gard_id, term

        UNION ALL

        SELECT
            gard_id,
            'diseaseArticleByNHS' AS metric_name,
            CASE WHEN is_NHS = 1 THEN 'true' ELSE 'false' END AS term,
            COUNT(*) AS item_count
        FROM article_rows
        GROUP BY gard_id, term

        UNION ALL

        SELECT
            gard_id,
            'diseaseArticleByYear' AS metric_name,
            CAST(publication_year AS CHAR) AS term,
            COUNT(*) AS item_count
        FROM article_rows
        GROUP BY gard_id, publication_year
    '''

    '''
    diseaseProjectsByYear 
    '''
    disease_project_query = '''
        SELECT
            related.gard_id,
            related.project_year,
            COUNT(*) AS project_count
        FROM (
            SELECT DISTINCT
                gpr.gard_id,
                p.APPLICATION_ID,
                p.FY AS project_year
            FROM grant_gard_project_relation AS gpr
            INNER JOIN grant_project AS p
                ON p.APPLICATION_ID = gpr.application_id
            WHERE gpr.gard_id IN ({placeholders})
            AND gpr.application_id IS NOT NULL
            AND p.FY IS NOT NULL
        ) AS related
        GROUP BY
            related.gard_id,
            related.project_year
        ORDER BY
            related.gard_id,
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
        batch_size = 100     
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

                # Step 1:
                # Fetch all article filter counts for this Memgraph GARD batch in a single MySQL round trip. 
                # The result is keyed by gard_id, so the per-GARD loop below only does dictionary lookups.
                disease_article_counts_by_gard = self._disease_article_iterms_count_batch(gard_id_list, fetch_cursor)
                hours, minutes, seconds = _time_hms(time.time() - batch_start_time)
                self.logger.info(f'\n\ndisease_article_counts_by_gard: time={hours} hours, {minutes} minutes, {seconds} seconds')
                
                # Step 2:
                # Fetch all project-by-year counts for the same GARD IDs in one query. 
                # The idx_gpr_gard_application index supports the gpr.gard_id IN (...) filter plus application_id join path.
                disease_project_counts_by_gard = self._disease_project_by_year_count_batch(gard_id_list, fetch_cursor)
                hours, minutes, seconds = _time_hms(time.time() - batch_start_time)
                self.logger.info(f'disease_project_counts_by_gard: time={hours} hours, {minutes} minutes, {seconds} seconds')
                
                # Step 3:
                batch = []
                for gard_id in gard_id_list:

                    self.logger.info(f'Processing GARD ID: {gard_id}')
                   
                    # Clinical-trial counts still need Memgraph trial node properties, so keep that helper per GARD ID for now.
                    disease_clinical_trial_terms_count = self._disease_clinical_trial_terms_count(gard_id, fetch_cursor)
                    hours, minutes, seconds = _time_hms(time.time() - batch_start_time)
                    self.logger.info(f'disease_clinical_trial_terms_count: time={hours} hours, {minutes} minutes, {seconds} seconds')

                    disease_article_iterms_count = disease_article_counts_by_gard[gard_id]
                    disease_project_by_year_count = disease_project_counts_by_gard[gard_id]

                    total_updated += 1

                    # Step 4:
                    # Merge the three independent count payloads into the final filterCounts object written to the matching GARD node.
                    obj = {'gard_id': gard_id,
                                  "data": {
                                        **disease_article_iterms_count,
                                        **disease_project_by_year_count,
                                        **disease_clinical_trial_terms_count,
                                    }
                                }
                    batch.append(obj)                    
                    self.logger.info(f'\n{json.dumps(obj, ensure_ascii=False)}')                 

                if batch:
                    try:
                        # Step 5:
                        # Write the whole GARD batch to Memgraph in one call instead of updating each GARD node separately.
                        self.memgraph.execute(batch_update_GARD_nodes_cypher, {"batch": batch}) 
                    except Exception as e:
                        self.logger.error(f'{e}')

                hours, minutes, seconds = _time_hms(time.time() - batch_start_time)
                self.logger.info(f'\n * Total updated={total_updated}. Batch processing time={hours} hours, {minutes} minutes, {seconds} seconds * \n')
                    
        except Exception as e:
            self.logger.error(f'{e}')

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            hours, minutes, seconds = _time_hms(time.time() - very_start_time)
            self.logger.info(f'\n\n****** Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds ******\n\n')

            ''' Explicitly close all db connections. '''
            self.close()


    def _disease_article_iterms_count(self, gard_id: str, fetch_cursor):
        """Compatibility wrapper for callers that still request one GARD ID."""

        return self._disease_article_iterms_count_batch([gard_id], fetch_cursor).get(gard_id, self._empty_disease_article_iterms_count(),)


    def _disease_article_iterms_count_batch(self, gard_id_list, fetch_cursor):
        """
        Return article count buckets for a batch of GARD IDs.

        The shape follows disease-counts.json:
            diseaseArticleByEpi: [{term: "true", count: n}, ...]
            diseaseArticleByNHS: [{term: "true", count: n}, ...]
            diseaseArticleByYear: [{term: "2026", count: n}, ...]
        """

        if not gard_id_list:
            return {}

        # Start every GARD ID with a complete empty payload. This guarantees that GARDs with no articles still receive true/false buckets with 0.
        counts_by_gard = {
            gard_id: {
                "epi": {"true": 0, "false": 0},
                "nhs": {"true": 0, "false": 0},
                "year": [],
            }
            for gard_id in gard_id_list
        }

        # Build one IN (...) clause for this batch. The values are still passed as parameters, so only the placeholder count is interpolated.
        placeholders = self._sql_placeholders(gard_id_list)

        query = self.disease_article_query.format(placeholders=placeholders)
        fetch_cursor.execute(query, tuple(gard_id_list))

        for row in fetch_cursor.fetchall():

            gard_id = row.get("gard_id")
            metric_name = row.get("metric_name")
            term = str(row.get("term"))
            item_count = int(row.get("item_count") or 0)

            if gard_id not in counts_by_gard:
                continue

            # The SQL returns one compact row per metric/term/GARD. Convert
            # those rows into the nested disease-counts.json payload shape.
            if metric_name == "diseaseArticleByEpi":
                counts_by_gard[gard_id]["epi"][term] = item_count
            elif metric_name == "diseaseArticleByNHS":
                counts_by_gard[gard_id]["nhs"][term] = item_count
            elif metric_name == "diseaseArticleByYear":
                counts_by_gard[gard_id]["year"].append({
                    "count": item_count,
                    "term": term,
                })

        return {
            gard_id: self._format_disease_article_counts(counts)
            for gard_id, counts in counts_by_gard.items()
        }


    def _format_disease_article_counts(self, counts):
        """Convert intermediate article counters into the final JSON shape."""

        year_counts = sorted(
            counts["year"],
            key=lambda item: int(item["term"]),
            reverse=True,
        )

        return {
            "diseaseArticleByEpi": [
                {"count": counts["epi"]["true"], "term": "true"},
                {"count": counts["epi"]["false"], "term": "false"},
            ],
            "diseaseArticleByNHS": [
                {"count": counts["nhs"]["true"], "term": "true"},
                {"count": counts["nhs"]["false"], "term": "false"},
            ],
            "diseaseArticleByYear": year_counts,
        }


    def _empty_disease_article_iterms_count(self):
        """Return the article-count payload for a GARD ID with no articles."""

        return {
            "diseaseArticleByEpi": [
                {"count": 0, "term": "true"},
                {"count": 0, "term": "false"},
            ],
            "diseaseArticleByNHS": [
                {"count": 0, "term": "true"},
                {"count": 0, "term": "false"},
            ],
            "diseaseArticleByYear": [],
        }


    def _disease_project_by_year_count(self, gard_id: str, fetch_cursor):
        """Compatibility wrapper for callers that still request one GARD ID."""

        return self._disease_project_by_year_count_batch([gard_id], fetch_cursor).get(gard_id, self._empty_disease_project_by_year_count(),)


    def _disease_project_by_year_count_batch(self, gard_id_list, fetch_cursor):
        """
        Return project count buckets for a batch of GARD IDs.

        The shape follows disease-counts.json:
            diseaseProjectsByYear: [{term: "2023", count: n}, ...]
        """

        if not gard_id_list:
            return {}

        # Initialize all GARD IDs so no-project diseases still get the expected
        # diseaseProjectsByYear key with an empty list.
        counts_by_gard = {
            gard_id: self._empty_disease_project_by_year_count()
            for gard_id in gard_id_list
        }

        # Use one indexed query for the whole Memgraph batch instead of one
        # query per GARD ID. This is the main MySQL performance win here.
        placeholders = self._sql_placeholders(gard_id_list)
        query = self.disease_project_query.format(placeholders=placeholders)

        fetch_cursor.execute(query, tuple(gard_id_list))
        rows = fetch_cursor.fetchall()

        for row in rows:
            gard_id = row.get("gard_id")
            project_year = row.get("project_year")

            if gard_id not in counts_by_gard or project_year is None:
                continue

            counts_by_gard[gard_id]["diseaseProjectsByYear"].append({
                "term": str(project_year),
                "count": int(row.get("project_count") or 0),
            })

        return counts_by_gard


    def _empty_disease_project_by_year_count(self):
        return {"diseaseProjectsByYear": []}


    def _sql_placeholders(self, values):
        """Return a comma-separated placeholder list for a parameterized IN clause."""
        return ",".join(["%s"] * len(values))
        

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

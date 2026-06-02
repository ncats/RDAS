import json
import os
import sys
import time
from typing import Any, Dict, List, Tuple

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, "..")),
    os.path.abspath(os.path.join(_dir, "../..")),
])

from pipelines.pipeline_base import PipelineBase
from utils.tools import _date_string, _time_hms


class DashboardFieldCountsRefreshTask(PipelineBase):

    NODE_ID = "all-filed-counts"

    ARTICLE_BY_YEAR_QUERY = '''
        SELECT
            CAST(pa.publication_year AS CHAR) AS term,
            COUNT(*) AS all_count,
            SUM(CASE WHEN pa.is_EPI = '1' THEN 1 ELSE 0 END) AS epi_count,
            SUM(CASE WHEN pa.is_NHS = '1' THEN 1 ELSE 0 END) AS nhs_count
        FROM publication_article AS pa FORCE INDEX (idx_article_year_epi_nhs)
        WHERE pa.publication_year IS NOT NULL
        AND pa.publication_year >= 1970
        AND pa.publication_year <= YEAR(CURDATE())
        GROUP BY pa.publication_year
    '''

    PROJECT_ALL_BY_YEAR_QUERY = '''
        SELECT
            CAST(p.FY AS CHAR) AS term,
            COUNT(DISTINCT gpru.application_id) AS item_count
        FROM grant_gard_project_relation_unique_application_id AS gpru
        INNER JOIN grant_project AS p
            ON p.APPLICATION_ID = gpru.application_id
        WHERE gpru.application_id IS NOT NULL
        AND p.FY IS NOT NULL
        GROUP BY p.FY
    '''

    TRIAL_ALL_BY_PHASE_QUERY = '''
        MATCH (ct:ClinicalTrial)
        WITH
            CASE
                WHEN ct.phase IS NULL OR ct.phase = "" THEN "NA"
                ELSE ct.phase
            END AS term,
            count(ct) AS item_count
        RETURN term, item_count
    '''

    TRIAL_ALL_BY_STATUS_QUERY = '''
        MATCH (ct:ClinicalTrial)
        WITH
            CASE
                WHEN ct.overallStatus IS NULL OR ct.overallStatus = "" THEN "UNKNOWN"
                ELSE ct.overallStatus
            END AS term,
            count(ct) AS item_count
        RETURN term, item_count
    '''

    TRIAL_ALL_BY_TYPE_QUERY = '''
        MATCH (ct:ClinicalTrial)
        WITH
            CASE
                WHEN ct.studyType IS NULL OR ct.studyType = "" THEN "UNKNOWN"
                ELSE ct.studyType
            END AS term,
            count(ct) AS item_count
        RETURN term, item_count
    '''

    MYSQL_COUNT_QUERIES: Tuple[Tuple[str, str, str], ...] = (
        ("allProjectsByYear", PROJECT_ALL_BY_YEAR_QUERY, "year_desc"),
    )

    MEMGRAPH_COUNT_QUERIES: Tuple[Tuple[str, str, str], ...] = (
        ("allTrialsByPhase", TRIAL_ALL_BY_PHASE_QUERY, "term"),
        ("allTrialsByStatus", TRIAL_ALL_BY_STATUS_QUERY, "count_desc"),
        ("allTrialsByType", TRIAL_ALL_BY_TYPE_QUERY, "count_desc"),
    )

    UPSERT_ALL_FIELD_COUNTS_NODE = '''
        MERGE (n:FieldCounts {id: $id})
        ON CREATE SET n.dateCreatedByRDAS = $last_updated
        SET n.fieldCounts = $field_counts,
            n.lastUpdatedDateByRDAS = $last_updated
        RETURN n.id AS id
    '''


    def __init__(self):
        super().__init__(init_mysql=True, init_memgraph=True)


    def find_new_data(self, gard_node) -> None:
        self.logger.info("DashboardFieldCountsRefreshTask does not use find_new_data().")


    def process_new_data(self) -> None:
        """Build global field count buckets and save them to one Memgraph node."""

        start_time = time.time()

        try:
            field_counts = self._build_field_counts()
            self.logger.info(f"\n{json.dumps({'data': field_counts}, ensure_ascii=False)}")

            results = list(
                self.memgraph.execute_and_fetch(
                    self.UPSERT_ALL_FIELD_COUNTS_NODE,
                    {
                        "id": self.NODE_ID,
                        "field_counts": field_counts,
                        "last_updated": _date_string(),
                    },
                )
            )

            node_id = results[0].get("id") if results else self.NODE_ID
            self.logger.info(f"Updated FieldCounts node: id={node_id}")

        except Exception as e:
            self.logger.error(f"Error refreshing all field counts: {e}")
            raise

        finally:
            hours, minutes, seconds = _time_hms(time.time() - start_time)
            self.logger.info(f"\n\n****** Total time elapsed: {hours} hours, {minutes} minutes, {seconds} seconds ******\n\n")

            ''' Explicitly close all db connections. '''
            self.close()


    def _build_field_counts(self) -> Dict[str, List[Dict[str, Any]]]:
        """Return the complete all-dataset fieldCounts payload."""

        field_counts = {}
        fetch_cursor = self.mysql.cursor(dictionary=True, buffered=True)

        try:
            article_metric_start = time.time()
            field_counts.update(self._fetch_article_by_year_counts(fetch_cursor))
            hours, minutes, seconds = _time_hms(time.time() - article_metric_start)
            self.logger.info(
                "article by year metrics: "
                f"allArticlesByYear={len(field_counts['allArticlesByYear'])}, "
                f"allEpiArticlesByYear={len(field_counts['allEpiArticlesByYear'])}, "
                f"allNHSArticlesByYear={len(field_counts['allNHSArticlesByYear'])}, "
                f"time={hours} hours, {minutes} minutes, {seconds} seconds"
            )

            for metric_name, query, sort_mode in self.MYSQL_COUNT_QUERIES:
                metric_start = time.time()
                rows = self._fetch_mysql_count_rows(fetch_cursor, query, sort_mode)
                field_counts[metric_name] = rows

                hours, minutes, seconds = _time_hms(time.time() - metric_start)
                self.logger.info(
                    f"{metric_name}: buckets={len(rows)}, "
                    f"time={hours} hours, {minutes} minutes, {seconds} seconds"
                )

            for metric_name, cypher, sort_mode in self.MEMGRAPH_COUNT_QUERIES:
                metric_start = time.time()
                rows = self._fetch_memgraph_count_rows(cypher, sort_mode)
                field_counts[metric_name] = rows

                hours, minutes, seconds = _time_hms(time.time() - metric_start)
                self.logger.info(
                    f"{metric_name}: buckets={len(rows)}, "
                    f"time={hours} hours, {minutes} minutes, {seconds} seconds"
                )

        finally:
            if fetch_cursor:
                fetch_cursor.close()

        return field_counts


    def _fetch_article_by_year_counts(self, cursor: Any) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch all article year buckets in one MySQL grouped scan.

        EPI and NHS lists include every publication year returned by
        allArticlesByYear, with count 0 when that year has no matching articles.
        """

        cursor.execute(self.ARTICLE_BY_YEAR_QUERY)

        all_articles_by_year = []
        all_epi_articles_by_year = []
        all_nhs_articles_by_year = []

        for row in cursor.fetchall():
            term = row.get("term")

            if term is None or str(term).strip() == "":
                continue

            term = str(term)
            all_articles_by_year.append({"term": term, "count": int(row.get("all_count") or 0)})
            all_epi_articles_by_year.append({"term": term, "count": int(row.get("epi_count") or 0)})
            all_nhs_articles_by_year.append({"term": term, "count": int(row.get("nhs_count") or 0)})

        return {
            "allEpiArticlesByYear": self._sort_count_rows(all_epi_articles_by_year, "year_desc"),
            "allNHSArticlesByYear": self._sort_count_rows(all_nhs_articles_by_year, "year_desc"),
            "allArticlesByYear": self._sort_count_rows(all_articles_by_year, "year_desc"),
        }


    def _fetch_mysql_count_rows(self, cursor: Any, query: str, sort_mode: str) -> List[Dict[str, Any]]:
        """Execute one MySQL aggregate query and normalize count rows."""

        cursor.execute(query)
        return self._normalize_count_rows(cursor.fetchall(), sort_mode)


    def _fetch_memgraph_count_rows(self, cypher: str, sort_mode: str) -> List[Dict[str, Any]]:
        """Execute one Memgraph aggregate query and normalize count rows."""

        return self._normalize_count_rows(self.memgraph.execute_and_fetch(cypher), sort_mode)


    def _normalize_count_rows(self, source_rows: Any, sort_mode: str) -> List[Dict[str, Any]]:
        """
        Normalize MySQL or Memgraph aggregate rows into the shared
        [{term: String, count: Int}] field-count shape.
        """

        rows = []

        for row in source_rows:
            term = row.get("term")

            if term is None or str(term).strip() == "":
                continue

            rows.append({
                "term": str(term),
                "count": int(row.get("item_count") or 0),
            })

        return self._sort_count_rows(rows, sort_mode)


    def _sort_count_rows(self, rows: List[Dict[str, Any]], sort_mode: str) -> List[Dict[str, Any]]:
        """Sort normalized count rows using the ordering expected by the UI."""

        if sort_mode == "year_desc":
            # The year fields are expected to be four-digit years. Keep a
            # string fallback so unexpected values do not break the refresh.
            return sorted(rows, key=lambda item: self._year_sort_key(item["term"]), reverse=True)

        if sort_mode == "term":
            return sorted(rows, key=lambda item: item["term"])

        return sorted(rows, key=lambda item: (-item["count"], item["term"]))


    def _year_sort_key(self, term: str) -> Tuple[int, str]:
        """Sort normal years numerically while keeping odd values deterministic."""

        try:
            return int(term), term

        except ValueError:
            return -1, term


if __name__ == "__main__":

    task = DashboardFieldCountsRefreshTask()
    task.process_new_data()

"""
Grant alert helper for AlertSender.

This file owns the grant-specific alert rules so AlertSender can focus on
Firebase users, payload assembly, email delivery, and summary logging.
"""

from typing import Any, Dict, Optional, Set, Tuple


GrantAlertPair = Tuple[str, int, int]
GrantAlertRow = Tuple[str, str, int]


class GrantAlertHelper:
    """
    Find previous-year grant alerts and record globally sent grant alerts.

    The grant pipeline runs separately from the main alert pipeline and its
    wrap-up resets grant `is_new` flags before AlertSender runs. Because of
    that, grant alerts are based on fiscal year plus the `grant_alert_sent`
    ledger table, not on `grant_project.is_new` or
    `grant_gard_project_relation.is_new`.
    """

    GRANT_ITEM_NAME = "grants"

    def __init__(self, mysql: Any, logger: Any):

        """Keep the shared MySQL connection/logger from AlertSender."""

        self.mysql = mysql
        self.logger = logger
        self._alertable_pairs_by_gard_id: Dict[str, Set[GrantAlertPair]] = {}


    def find_alertable_grants(self, gard_id: str) -> Tuple[Optional[GrantAlertRow], Set[GrantAlertPair]]:

        """
        Return the email count row and ledger pairs for one GARD ID.

        AlertSender needs a count row shaped like the clinical-trial and
        publication query rows: `(gard_id, item_name, count)`. It also needs the
        exact `(gard_id, application_id, funding_year)` tuples to insert into
        `grant_alert_sent` after successful email delivery. Building both from
        the same pair query keeps the email count and ledger insert aligned.
        """

        grant_pairs = self.find_alertable_grant_pairs(gard_id)

        if not grant_pairs:
            return None, set()

        return (gard_id, self.GRANT_ITEM_NAME, len(grant_pairs)), grant_pairs


    def find_alertable_grant_pairs(self, gard_id: str) -> Set[GrantAlertPair]:

        """
        Find previous-year GARD/project/year tuples not already alerted.

        The result is cached by GARD ID for one AlertSender run. Many users can
        subscribe to the same GARD ID, and the grant ledger is intentionally not
        updated until after the user email loop, so repeated lookups during the
        same run would return the same result anyway.
        """

        if gard_id in self._alertable_pairs_by_gard_id:
            return set(self._alertable_pairs_by_gard_id[gard_id])

        find_alertable_grant_pairs_query = '''
            /*
            Grant alerts are selected from the grant/GARD relationship table,
            then narrowed to projects from the previous fiscal year. Do not
            require grant_project.is_new or grant_gard_project_relation.is_new:
            main_grant.py runs once per year before main.py, and its wrap-up
            step resets those grant flags to 0 before AlertSender runs.

            NIH RePORTER grant exports are refreshed annually, so
            p.FY = YEAR(CURDATE()) - 1 prevents older rebuilt grant rows from
            being announced as new alerts.

            grant_alert_sent is a global send-once ledger. If a
            (gard_id, application_id, funding_year) tuple already exists in
            that table, the grant was included in a previous successful RDAS
            grant alert run and must not be sent again, including for users who
            subscribe after that first alert.

            SELECT DISTINCT avoids duplicate ledger pairs when one project
            matches the same GARD ID through multiple grant relationship terms.
            */
            SELECT DISTINCT
                gpr.gard_id,
                gpr.application_id,
                p.FY AS funding_year
            FROM grant_gard_project_relation AS gpr
            INNER JOIN grant_project AS p
                ON p.APPLICATION_ID = gpr.application_id
            LEFT JOIN grant_alert_sent AS gas
                ON gas.gard_id = gpr.gard_id
                AND gas.application_id = gpr.application_id
                AND gas.funding_year = p.FY
            WHERE gpr.gard_id = %s
                AND p.FY = YEAR(CURDATE()) - 1
                AND gpr.application_id IS NOT NULL
                AND gas.application_id IS NULL
        '''

        cursor = None

        try:
            cursor = self.mysql.cursor()
            cursor.execute(find_alertable_grant_pairs_query, (gard_id,))
            rows = cursor.fetchall()

            grant_pairs = {
                (str(grant_gard_id), int(application_id), int(funding_year))
                for grant_gard_id, application_id, funding_year in rows
                if grant_gard_id and application_id is not None and funding_year is not None
            }

            self._alertable_pairs_by_gard_id[gard_id] = grant_pairs
            return set(grant_pairs)

        except Exception:
            self.logger.exception(f"Unable to find alertable grant pairs for gard_id={gard_id}.")
            return set()

        finally:
            if cursor:
                cursor.close()


    def save_alert_sent_pairs(self, grant_alert_pairs: Set[GrantAlertPair]) -> int:

        """
        Save globally alerted GARD/project/fiscal-year tuples.

        INSERT IGNORE works with the `grant_alert_sent` unique key so a repeated
        tuple from multiple users in the same alert run is harmless. AlertSender
        passes only pairs that appeared in at least one successful user email,
        so the global ledger records grant alerts that were actually sent.
        """

        if not grant_alert_pairs:
            self.logger.info("No grant alert pairs were sent; grant_alert_sent was not updated.")
            return 0

        insert_sql = '''
            INSERT IGNORE INTO grant_alert_sent (gard_id, application_id, funding_year)
            VALUES (%s, %s, %s)
        '''

        cursor = None
        cleaned_pairs = sorted({
            (str(gard_id), int(application_id), int(funding_year))
            for gard_id, application_id, funding_year in grant_alert_pairs
            if gard_id and application_id is not None and funding_year is not None
        })

        if not cleaned_pairs:
            self.logger.info("Grant alert pairs were collected, but none had complete gard_id/application_id/funding_year values.")
            return 0

        try:
            cursor = self.mysql.cursor()
            cursor.executemany(insert_sql, cleaned_pairs)
            self.mysql.commit()

            self.logger.info(f"Inserted {cursor.rowcount} new row(s) into grant_alert_sent from {len(cleaned_pairs)} grant alert pair(s).")
            return cursor.rowcount

        except Exception:
            if self.mysql:
                self.mysql.rollback()

            self.logger.exception("Unable to save grant alert pairs to grant_alert_sent.")
            return 0

        finally:
            if cursor:
                cursor.close()

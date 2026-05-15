import os
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
    os.path.abspath(os.path.join(_dir, '../..')),
])

try:
    from colorama import init, Fore, Style
except ModuleNotFoundError:
    class _NoColor:
        RED = GREEN = YELLOW = BLUE = RESET_ALL = ""

    def init():
        pass

    Fore = _NoColor()
    Style = _NoColor()

init()

from baseclass.conn import DBConnection as db
from utils.file_appender import FileAppender
from utils.tools import _clean, _date_string, _make_hash_key, _remove_parentheses


"""
Create/update publication Agent nodes in Memgraph.

This updater reads person_of_all_sources rows whose source is Publication,
MERGEs Agent nodes by rdas_group_id, and MERGEs Article -> Agent has_author
relationships. Article.pubmedId is an integer in Memgraph, so publication
associate_id_int is used as the PubMed ID before matching Article nodes.
person_of_all_sources.id is only the MySQL sequence number for logging.
"""


class PublicationPersonAgentUpdater:
    """Sync publication people from MySQL into Agent nodes in Memgraph."""

    BATCH_SIZE = 1000
    MAX_RETRIES = 3
    RETRY_SECONDS = 5

    PERSON_TABLE = "person_of_all_sources"
    SOURCE = "Publication"

    FETCH_PUBLICATION_PERSON_QUERY = f'''
        SELECT
            id AS table_row_id,
            associate_id,
            associate_id_int,
            first_name,
            last_name,
            affiliation,
            orcid,
            email,
            rdas_group_id,
            PI_id
        FROM {PERSON_TABLE} FORCE INDEX (idx_poas_source)
        WHERE source = '{SOURCE}'
    '''

    MERGE_PUBLICATION_AGENT_CYPHER = '''
       
        UNWIND $chunks AS chunk

        // Agent identity is the hash of rdas_group_id, so rows that belong to the same resolved person merge into the same Agent node.
        MERGE (a:Agent {_idx_key: chunk._idx_key})

        // These properties are written only when the Agent node is first created. Existing Agent values are not overwritten here.
        ON CREATE SET
            a.fullName = chunk.fullName,
            a.firstName = chunk.firstName,
            a.lastName = chunk.lastName,
            a.orc_id = chunk.orc_id,
            a.pi_id = chunk.pi_id,
            a.contactEmail = CASE
                WHEN chunk.email = '' THEN []
                ELSE [chunk.email]
            END,
            a.dateCreatedByRDAS = chunk.formattedToday

        // Fill only missing Agent fields on existing nodes, and always refresh lastUpdatedByRDAS. 
        // This keeps old values stable while allowing a new publication row to complete blank properties.
        SET
            a.fullName = CASE
                WHEN coalesce(a.fullName, '') = '' THEN chunk.fullName
                ELSE a.fullName
            END,
            a.firstName = CASE
                WHEN coalesce(a.firstName, '') = '' THEN chunk.firstName
                ELSE a.firstName
            END,
            a.lastName = CASE
                WHEN coalesce(a.lastName, '') = '' THEN chunk.lastName
                ELSE a.lastName
            END,
            a.orc_id = CASE
                WHEN coalesce(a.orc_id, '') = '' AND chunk.orc_id <> '' THEN chunk.orc_id
                ELSE coalesce(a.orc_id, '')
            END,
            a.pi_id = CASE
                WHEN coalesce(a.pi_id, '') = '' AND chunk.pi_id <> '' THEN chunk.pi_id
                ELSE coalesce(a.pi_id, '')
            END,
            a.contactEmail = CASE
                WHEN chunk.email = '' THEN coalesce(a.contactEmail, [])
                WHEN a.contactEmail IS NULL THEN [chunk.email]
                WHEN chunk.email IN a.contactEmail THEN a.contactEmail
                ELSE a.contactEmail + [chunk.email]
            END,
            a.lastUpdatedByRDAS = chunk.formattedToday

        // Link the publication Article to the Agent as an author. Article nodes use integer pubmedId, so Python sends chunk.pubmedId as int.
        WITH a, chunk
        CALL {
            WITH a, chunk
            MATCH (article:Article {pubmedId: chunk.pubmedId})
            MERGE (article)-[:has_author]->(a)
        }

        // If the person row has an affiliation and the Organization already exists in Memgraph, connect the Agent to that Organization.
        WITH a, chunk
        CALL {
            WITH a, chunk
            WHERE chunk.organizationIdxKey <> ''
            MATCH (o:Organization {_idx_key: chunk.organizationIdxKey})
            MERGE (a)-[:has_affiliation]->(o)
        }
    '''

    def __init__(self):
        self.mysql = db().mysql_conn()
        self.memgraph = db().memgraph_conn()
        self.formatted_today = datetime.today().strftime("%Y-%m-%d")

        class_name = type(self).__name__
        self.log_file = f"logs/G-{class_name}-{_date_string()}.log"
        self.appender = FileAppender(self.log_file)


    def update(self) -> None:
        """Fetch Publication people from MySQL and MERGE them into Memgraph."""

        fetch_cursor = None
        total_rows_read = 0
        total_chunks_sent = 0
        batch_num = 0
        start_time = time.time()

        self.appender.log_stdout(f"Starting publication person Agent update. Batch size = {self.BATCH_SIZE}.")

        try:
            fetch_cursor = self.mysql.cursor(dictionary=True)
            fetch_cursor.execute(self.FETCH_PUBLICATION_PERSON_QUERY)

            while True:
                rows = fetch_cursor.fetchmany(self.BATCH_SIZE)

                if not rows:
                    break

                batch_num += 1
                total_rows_read += len(rows)

                chunks = self.build_agent_chunks(rows)

                if not chunks:
                    self.appender.log_stdout(
                        f"{Fore.YELLOW}Batch #{batch_num}: no valid publication Agent chunks."
                        f"{Style.RESET_ALL}"
                    )
                    continue

                self.execute_graph_batch_with_retry(chunks, batch_num)

                total_chunks_sent += len(chunks)
                self.appender.log_stdout(
                    f"Batch #{batch_num}: read {len(rows)} rows, sent {len(chunks)} "
                    f"Agent chunks. Total chunks sent = {total_chunks_sent}."
                )

            hours, minutes, seconds = self.elapsed_time(start_time, time.time())
            self.appender.log_stdout(
                f"\n{Fore.GREEN}{'=' * 30} Done. Rows read: {total_rows_read}. "
                f"Agent chunks sent: {total_chunks_sent}. "
                f"Total time: {hours}:{minutes}:{seconds} {'=' * 30}{Style.RESET_ALL}\n"
            )

        finally:
            if fetch_cursor:
                fetch_cursor.close()

            self.close()


    def build_agent_chunks(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert MySQL Publication person rows into Memgraph Agent payloads."""

        chunks = []

        for row in rows:
            chunk = self.build_agent_chunk(row)

            if chunk:
                chunks.append(chunk)

        return chunks


    def build_agent_chunk(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build one Agent payload from one Publication person row."""

        rdas_group_id = row.get("rdas_group_id")
        pubmed_id = self.get_pubmed_id(row)
        first_name = row.get("first_name")
        original_last_name = row.get("last_name")

        if not rdas_group_id or pubmed_id is None or not first_name or not original_last_name:
            return None

        normalized_last_name = self.normalize_last_name(original_last_name)

        if not normalized_last_name:
            self.appender.log_stdout(
                f"{Fore.YELLOW}Skipping invalid last_name={original_last_name} "
                f"for person table_row_id={row.get('table_row_id')}.{Style.RESET_ALL}"
            )
            return None

        first_name = str(first_name).strip().title()
        last_name = normalized_last_name.strip().title()
        full_name = f"{first_name} {last_name}"

        affiliation = _clean(row.get("affiliation"))
        organization_idx_key = ""

        if affiliation:
            organization_idx_key = _make_hash_key(_remove_parentheses(affiliation))

        return {
            "_idx_key": _make_hash_key(rdas_group_id),
            "fullName": full_name,
            "firstName": first_name,
            "lastName": last_name,
            "orc_id": _clean(row.get("orcid")),
            "pi_id": _clean(row.get("PI_id")),
            "email": _clean(row.get("email")),
            "organizationIdxKey": organization_idx_key,
            "pubmedId": pubmed_id,
            "formattedToday": self.formatted_today,
        }


    def get_pubmed_id(self, row: Dict[str, Any]) -> Optional[int]:
        """Return associate_id_int, the integer PubMed ID used by Article.pubmedId."""

        return self.to_int(
            row.get("associate_id_int"),
            row.get("table_row_id"),
            row.get("associate_id")
        )


    def to_int(self, value: Any, table_row_id: Any = None, associate_id: Any = None) -> Optional[int]:
        """Convert associate_id_int to the integer PubMed ID used in Memgraph."""

        try:
            return int(value)
        except (TypeError, ValueError):
            self.appender.log_stdout(
                f"{Fore.YELLOW}Invalid PubMed ID skipped for person "
                f"table_row_id={table_row_id}, associate_id={associate_id}, "
                f"associate_id_int={value}.{Style.RESET_ALL}"
            )
            return None


    def normalize_last_name(self, last_name: Any) -> Optional[str]:
        """Normalize last names using the same rules as the person graph task."""

        if not last_name or not isinstance(last_name, str):
            return None

        last_name = last_name.strip()

        if not last_name:
            return None

        if re.match(r"^#", last_name):
            return None

        if re.match(r"^[\(\)]|[\)']$", last_name):
            return None

        if re.match(r"^(-|\.|\.Null)$", last_name):
            return None

        if re.match(r"^\d", last_name):
            return None

        if re.match(r"^[?<>@\[\]{}]", last_name):
            return None

        match = re.match(r"^'([ntsNTS])\s+(.+)$", last_name)
        if match:
            prefix = match.group(1).lower()
            remainder = match.group(2).strip()
            return f"'{prefix} {remainder}" if remainder else None

        match = re.match(r"^'([ntsNTS])([A-Za-z-].*)$", last_name)
        if match:
            prefix = match.group(1).lower()
            remainder = match.group(2).strip()
            return f"'{prefix} {remainder}" if remainder else None

        last_name = re.sub(r"^'", "", last_name)
        last_name = re.sub(r"^-", "", last_name)
        last_name = last_name.strip()

        return last_name or None


    def execute_graph_batch_with_retry(self, chunks: List[Dict[str, Any]], batch_num: int) -> None:
        """Execute one Memgraph batch with a small retry loop."""

        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                self.memgraph.execute(self.MERGE_PUBLICATION_AGENT_CYPHER, {"chunks": chunks})
                return
            except Exception as e:
                last_error = e

                if attempt >= self.MAX_RETRIES:
                    self.appender.log_stdout(
                        f"{Fore.RED}Batch #{batch_num} failed after {attempt} attempts: "
                        f"{e}{Style.RESET_ALL}"
                    )
                    raise

                self.appender.log_stdout(
                    f"{Fore.YELLOW}Batch #{batch_num} attempt {attempt}/{self.MAX_RETRIES} "
                    f"failed: {e}. Retrying in {self.RETRY_SECONDS} seconds..."
                    f"{Style.RESET_ALL}"
                )
                time.sleep(self.RETRY_SECONDS)

        if last_error:
            raise last_error


    def close(self) -> None:
        """Close open connections and the log appender."""

        if self.mysql:
            self.mysql.close()

        close_memgraph = getattr(self.memgraph, "close", None)

        if callable(close_memgraph):
            close_memgraph()

        self.appender.close()


    @staticmethod
    def elapsed_time(start_time: float, end_time: float):
        """Return elapsed time as hours, minutes, seconds."""

        time_diff = end_time - start_time
        hours = int(time_diff // 3600)
        minutes = int((time_diff % 3600) // 60)
        seconds = int(time_diff % 60)

        return hours, minutes, seconds


if __name__ == "__main__":

    updater = PublicationPersonAgentUpdater()
    updater.update()

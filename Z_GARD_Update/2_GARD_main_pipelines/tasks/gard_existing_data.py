"""Copy reusable RDAS records for the one-time GARD build, without changing Alert."""

import json
import os
import re
from contextlib import contextmanager
from datetime import date
from time import perf_counter

from baseclass.conn import DBConnection
from pipelines.pipeline_2_clinical_trial.clinical_trial_study_change_handler import ClinicalTrialStudyChangeHandler
from utils.tools import _safe_get


class GARDExistingDataReuse:
    """Copy source records into the configured target using each caller's connection.

    MYSQL_COPY_SOURCE_DATABASE and MYSQL_COPY_TARGET_DATABASE select the schema
    roles when this helper is constructed. The GARD .env.newdb configuration
    copies rdas_db -> gard_rdas_db; .env.alert reverses those roles. The caller's
    selected MYSQL_DATABASE must match the configured target before any writes.

    This is shared copy infrastructure, not a separately scheduled pipeline
    task, so its module has no invocation number. The runner creates one
    instance and passes it to the numbered discovery/reference import tasks.
    Metadata columns remain cached across nodes; the current GARD ID and
    expensive-enrichment exclusion sets reset at each node boundary. Database
    connections, cursors, and task loggers are owned by the callers.

    Example lifecycle, with conditional cache states rather than live DB claims:
        reuse = GARDExistingDataReuse()
        reuse.begin_gard_node("GARD:0000016")
        reuse.copy_publications(mysql, logger, ["9282845"], search_term="Coma")

    If the source has valid Article data and the destination does not, the
    helper inserts the Article with is_new=1 and assigns a new destination ID.
    Source mappings for other diseases are excluded; current disease mappings
    always use GARD:0000016 and the supplied term. An Article already completed
    in the target stays completed, while its new GARD edge is handled later.

    For a reused trial annotation, the helper records its NCT ID in memory.
    enrichment_scope() temporarily changes only that task's pending input rows
    from is_new=1 to 2, then restores them to 1 before graph/person processing.
    If the process is killed, startup recovery restores those temporary flags.
    Neither copying nor temporary exclusions change the source database flags.
    """

    BATCH_SIZE = 100
    TABLES = (
        "clinical_trial", "clinical_trial_unique", "clinical_trial_annotation",
        "clinical_trial_intervention_drug", "clinical_trial_nctid_pmids_mapping",
        "publication_article", "publication_pubtator", "publication_pubtator_parsed",
        "publication_substance", "publication_substance_unique",
        "publication_gard_pubmed", "publication_gard_searchterm_pubmed_mapping",
        "publication_gard_omim_mapping", "publication_omim", "publication_omim_pubmed_mapping",
    )

    def __init__(self):

        '''
        Resolve the direction once per helper so a long run keeps a consistent
        source and target. Require explicit settings instead of silently using
        the GARD direction when the active configuration is .env.alert.

        Database identifiers cannot use SQL value placeholders. Validate these
        names before interpolating them as backtick-quoted identifiers; record
        IDs, disease names and other data remain ordinary query parameters.
        '''
        self.SOURCE_DATABASE = os.getenv("MYSQL_COPY_SOURCE_DATABASE", "").strip()
        self.TARGET_DATABASE = os.getenv("MYSQL_COPY_TARGET_DATABASE", "").strip()
        for setting, database in (("MYSQL_COPY_SOURCE_DATABASE", self.SOURCE_DATABASE), ("MYSQL_COPY_TARGET_DATABASE", self.TARGET_DATABASE)):
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", database):
                raise ValueError(f"{setting} must specify a database name using letters, digits and underscores, up to 64 characters.")
        if self.SOURCE_DATABASE.lower() == self.TARGET_DATABASE.lower():
            raise ValueError("MySQL copy source and target databases must be different.")

        self.columns = {}
        self.drug_nctids = set()
        self.annotation_nctids = set()
        self.current_gard_id = None

    def begin_gard_node(self, gard_id):

        """Keep task exclusion lists bounded to the current disease, not the whole build."""
        if not re.fullmatch(r"GARD:\d{7}", gard_id):
            raise ValueError(f"Invalid current GARD ID: {gard_id!r}")
        self.current_gard_id = gard_id
        self.drug_nctids.clear()
        self.annotation_nctids.clear()

    def validate_connection(self, mysql):

        '''
        Qualify every destination table and reject an accidentally selected
        database that differs from MYSQL_COPY_TARGET_DATABASE before any copy
        or temporary flag change. This also protects the reversed Alert
        configuration. Source access uses SELECT only, on the same MySQL server
        as the target; MYSQL_DATABASE remains the caller's selected destination.
        '''
        cursor = mysql.cursor()
        try:
            cursor.execute("SELECT DATABASE()")
            if cursor.fetchone()[0] != self.TARGET_DATABASE:
                raise RuntimeError(f"GARD data reuse requires {self.TARGET_DATABASE} as the selected database.")
            if self.columns:
                return
            cursor.execute(
                '''SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, EXTRA
                   FROM information_schema.COLUMNS
                   WHERE TABLE_SCHEMA IN (%s, %s)
                   ORDER BY ORDINAL_POSITION''',
                (self.SOURCE_DATABASE, self.TARGET_DATABASE),
            )
            available = {}
            for schema, table, column, extra in cursor.fetchall():
                if table in self.TABLES and column not in {"id", "create", "created", "processed"} and "GENERATED" not in extra.upper() and "auto_increment" not in extra.lower():
                    available.setdefault((schema, table), []).append(column)
            for table in self.TABLES:
                source = available.get((self.SOURCE_DATABASE, table), [])
                target = available.get((self.TARGET_DATABASE, table), [])
                if not target or set(source) != set(target):
                    raise RuntimeError(f"Source/target writable columns differ for {table}; review the schemas before copying.")
                self.columns[table] = target
        except Exception:
            self.columns.clear()
            raise
        finally:
            cursor.close()

    def _copy_rows(self, cursor, table, predicate, params, keys=None, overrides=None, source_from=None, restage_existing=False):

        '''
        Copy directly inside MySQL, omitting source primary keys, timestamps,
        the generic processed marker, and generated columns. The caller supplies fixed
        SQL fragments; record IDs and current GARD values remain parameters.
        NOT EXISTS makes retries safe even for child tables without unique
        keys. DISTINCT collapses identical source history rows in this batch.
        '''
        if table not in self.TABLES:
            raise ValueError(f"Unsupported reuse table: {table}")
        columns = self.columns[table]
        overrides = overrides or {}
        '''
        Disease-bearing tables must never inherit a source GARD ID. Enforce
        the current-node override here as well as at the discovery entry point
        so future copy callers cannot accidentally import other associations.
        '''
        for gard_column in ("gardId", "gard_id"):
            if gard_column in columns and (not self.current_gard_id or overrides.get(gard_column) != self.current_gard_id):
                raise ValueError(f"{table} copies must explicitly use the current GARD ID.")
        expressions = []
        values = []
        for column in columns:
            if column == "is_new":
                expression = "1"
            elif column in overrides:
                expression = "%s"
                values.append(overrides[column])
            else:
                expression = f"s.`{column}`"
            expressions.append(f"{expression} AS `{column}`")
        keys = keys or [column for column in columns if column != "is_new"]
        comparisons = " AND ".join(f"d.`{key}` <=> incoming.`{key}`" for key in keys)
        names = ", ".join(f"`{column}`" for column in columns)
        selected = ", ".join(f"incoming.`{column}`" for column in columns)
        source_from = source_from or f"`{self.SOURCE_DATABASE}`.`{table}` AS s"
        incoming = f"(SELECT DISTINCT {', '.join(expressions)} FROM {source_from} WHERE {predicate}) AS incoming"
        cursor.execute(
            f'''INSERT INTO `{self.TARGET_DATABASE}`.`{table}` ({names})
                SELECT {selected}
                FROM {incoming}
                WHERE NOT EXISTS (SELECT 1 FROM `{self.TARGET_DATABASE}`.`{table}` AS d WHERE {comparisons})''',
            (*values, *params),
        )
        inserted = max(cursor.rowcount, 0)
        if restage_existing and "is_new" in columns:
            '''
            An identical child row may survive an earlier interrupted build
            with is_new=0. Restage only rows matching this source copy, so graph
            cleanup sees the reused current annotations and reference pairs.
            '''
            cursor.execute(f"UPDATE `{self.TARGET_DATABASE}`.`{table}` d JOIN {incoming} ON {comparisons} SET d.is_new = 1", (*values, *params))
        return inserted

    def copy_publications(self, mysql, logger, pubmed_ids, search_term=None):

        '''
        Reuse the PMID's metadata and supporting PubTator/substance records.
        A direct disease search also copies its exact source GARD/term mappings;
        absent mappings still come from the shared current-search importer.
        Trial/OMIM references do not invent disease search terms. Existing completed
        target Articles stay completed; a separate GARD relationship task
        handles their new disease mapping without duplicating author rows.
        '''
        self.validate_connection(mysql)
        identifiers = list(dict.fromkeys(str(value) for value in pubmed_ids if str(value).isdigit()))
        copied = 0
        cursor = mysql.cursor(buffered=True)
        try:
            for start in range(0, len(identifiers), self.BATCH_SIZE):
                batch = identifiers[start:start + self.BATCH_SIZE]
                batch_started = perf_counter()
                copied_before = copied
                batch_number = start // self.BATCH_SIZE + 1
                batch_count = (len(identifiers) + self.BATCH_SIZE - 1) // self.BATCH_SIZE
                logger.info(f"Publication copy batch {batch_number}/{batch_count}: checking {len(batch)} PMIDs.")
                placeholders = ",".join(["%s"] * len(batch))
                latest = f"s.id = (SELECT MAX(latest.id) FROM `{self.SOURCE_DATABASE}`.publication_article latest WHERE latest.pubmed_id = s.pubmed_id)"
                valid = f"s.pubmed_id IN ({placeholders}) AND {latest} AND JSON_VALID(s.source_json)"
                copied += self._copy_rows(cursor, "publication_article", valid, batch, keys=["pubmed_id"])

                '''
                A retry can find an Article downloaded before the old process
                stopped. Fill missing classification results only when its
                title and abstract match the source snapshot. Never overwrite
                newer target content with a different cached article.
                '''
                cursor.execute(
                    f'''UPDATE `{self.TARGET_DATABASE}`.publication_article d
                        JOIN `{self.SOURCE_DATABASE}`.publication_article s ON s.pubmed_id = d.pubmed_id
                        SET d.is_EPI = COALESCE(d.is_EPI, s.is_EPI), d.is_NHS = COALESCE(d.is_NHS, s.is_NHS),
                            d.epi_probability = COALESCE(d.epi_probability, s.epi_probability),
                            d.epi_extract = COALESCE(d.epi_extract, s.epi_extract)
                        WHERE d.is_new = 1 AND d.pubmed_id IN ({placeholders})
                        AND d.title <=> s.title AND d.abstract_text <=> s.abstract_text AND {latest}''',
                    batch,
                )
                cursor.execute(
                    f'''SELECT DISTINCT d.pubmed_id FROM `{self.TARGET_DATABASE}`.publication_article d
                        JOIN `{self.SOURCE_DATABASE}`.publication_article s ON s.pubmed_id = d.pubmed_id
                        WHERE d.is_new = 1 AND d.pubmed_id IN ({placeholders})
                        AND d.title <=> s.title AND d.abstract_text <=> s.abstract_text AND {latest}''',
                    batch,
                )
                reusable = [row[0] for row in cursor.fetchall()]
                if reusable:
                    marks = ",".join(["%s"] * len(reusable))
                    self._copy_rows(cursor, "publication_pubtator", f"s.pubmed_id IN ({marks}) AND JSON_VALID(s.source_json) AND s.id = (SELECT MAX(p.id) FROM `{self.SOURCE_DATABASE}`.publication_pubtator p WHERE p.pubmed_id = s.pubmed_id AND JSON_VALID(p.source_json))", reusable, keys=["pubmed_id"])
                    for table in ("publication_pubtator_parsed", "publication_substance"):
                        self._copy_rows(cursor, table, f"s.pubmed_id IN ({marks})", reusable)
                    '''
                    Read only this PMID batch's substance hashes, then use the
                    source hash index for bounded INSERT ... SELECT copies.
                    The former correlated EXISTS could scan all unique source
                    substances repeatedly; a running copy was observed taking
                    over 27 seconds. These small identifier lists are query
                    parameters, not another persistent or in-memory data cache.

                    Example: PMIDs [9282845, 9280151] reference hashes H1/H2.
                    Copy only source unique rows with hash_id IN (H1, H2),
                    keeping NOT EXISTS retry checks in the destination.
                    '''
                    cursor.execute(
                        f'''SELECT DISTINCT hash_id FROM `{self.TARGET_DATABASE}`.publication_substance
                            WHERE pubmed_id IN ({marks}) AND hash_id IS NOT NULL''',
                        reusable,
                    )
                    hashes = [row[0] for row in cursor.fetchall()]
                    for offset in range(0, len(hashes), self.BATCH_SIZE):
                        hash_batch = hashes[offset:offset + self.BATCH_SIZE]
                        hash_marks = ",".join(["%s"] * len(hash_batch))
                        self._copy_rows(cursor, "publication_substance_unique", f"s.hash_id IN ({hash_marks})", hash_batch)
                if search_term is not None:
                    '''
                    A PMID can match several diseases and search terms. Copy
                    only this exact current association, retaining one source
                    history row per tuple. Reset abbreviation decisions because
                    current GARD labels/content may differ from production.
                    Requiring a destination Article avoids orphan mappings.
                    '''
                    self._copy_rows(
                        cursor, "publication_gard_searchterm_pubmed_mapping",
                        f'''s.gard_id = %s AND BINARY s.search_term = BINARY %s
                            AND s.pubmed_id IN ({placeholders})
                            AND s.id = (SELECT MAX(m.id) FROM `{self.SOURCE_DATABASE}`.publication_gard_searchterm_pubmed_mapping m
                                WHERE m.gard_id = s.gard_id AND BINARY m.search_term = BINARY s.search_term AND m.pubmed_id = s.pubmed_id)
                            AND EXISTS (SELECT 1 FROM `{self.TARGET_DATABASE}`.publication_article a WHERE a.pubmed_id = s.pubmed_id)''',
                        (self.current_gard_id, search_term, *batch),
                        keys=["gard_id", "search_term", "pubmed_id"],
                        overrides={"gard_id": self.current_gard_id, "search_term": search_term, "is_abbreviation": None, "is_valid": None},
                    )
                mysql.commit()
                logger.info(f"Publication copy batch {batch_number}/{batch_count}: inserted {copied - copied_before} Articles; checked {start + len(batch)}/{len(identifiers)} PMIDs in {perf_counter() - batch_started:.2f}s.")
            logger.info(f"Reused {copied} publication_article rows from {self.SOURCE_DATABASE} into {self.TARGET_DATABASE}; checked {len(identifiers)} PMIDs and copied their available enrichment.")
        except Exception:
            mysql.rollback()
            raise
        finally:
            cursor.close()

    def copy_publication_search_history(self, mysql, logger, gard_id, search_term, mindate, maxdate):

        '''
        Copy complete historical responses only for this exact disease/term
        and overlapping date window, after ALL live slices for the term finish.
        A raw response includes a whole PMID list and count: matching just one
        PMID cannot justify rebinding a different disease's response. Requiring
        every cached PMID to have a current target mapping avoids importing
        unrelated IDs. Count validation rejects older truncated rows such as
        coma's 9,999-of-41,918 response. The current live row takes
        precedence whenever its disease/term/date key already exists.
        '''
        self.validate_connection(mysql)
        if gard_id != self.current_gard_id:
            raise ValueError("PubMed search history must use the current GARD node.")
        current_search_term = search_term
        search_term = search_term.lower()
        lower = date.fromisoformat(str(mindate or date(1970, 1, 1))[:10])
        upper = date.fromisoformat(str(maxdate or date.today())[:10])
        reader = mysql.cursor(dictionary=True, buffered=True)
        writer = mysql.cursor(buffered=True)
        copied = 0
        try:
            reader.execute(
                f'''SELECT id, year_range, total, retstart, pubmed_ids, ref_json
                   FROM `{self.SOURCE_DATABASE}`.publication_gard_pubmed
                   WHERE gard_id = %s AND BINARY search_term = BINARY %s ORDER BY id DESC''',
                (gard_id, search_term),
            )
            for row in reader:
                try:
                    bounds = re.fullmatch(r"(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})", row["year_range"] or "")
                    years = re.fullmatch(r"(\d{4})-(\d{4})", row["year_range"] or "")
                    if bounds:
                        start, end = date.fromisoformat(bounds[1]), date.fromisoformat(bounds[2])
                    elif years:
                        start, end = date(int(years[1]), 1, 1), date(int(years[2]), 12, 31)
                    else:
                        continue
                    if start > end or end < lower or start > upper:
                        continue
                    result = json.loads(row["ref_json"])["esearchresult"]
                    response_ids = result["idlist"]
                    if not isinstance(response_ids, list) or any(not str(value).isdigit() for value in response_ids):
                        continue
                    ids = {str(value) for value in response_ids}
                    stored_ids = set(filter(None, (row["pubmed_ids"] or "").split(",")))
                    if int(result["count"]) != len(response_ids) or row["total"] != len(response_ids) or int(result["retstart"]) != 0 or row["retstart"] != 0 or stored_ids != ids:
                        continue
                except (ValueError, TypeError, KeyError):
                    continue
                '''
                Verify IDs in small indexed lookups instead of collecting all
                live search slices in memory. Run this after discovery finishes:
                copying historical keys earlier could block a later live slice's
                raw-row insert. Source date labels and response JSON stay intact.
                '''
                mapped = True
                identifiers = list(ids)
                for offset in range(0, len(identifiers), self.BATCH_SIZE):
                    batch = identifiers[offset:offset + self.BATCH_SIZE]
                    marks = ",".join(["%s"] * len(batch))
                    writer.execute(
                        f'''SELECT DISTINCT pubmed_id FROM `{self.TARGET_DATABASE}`.publication_gard_searchterm_pubmed_mapping
                            WHERE gard_id = %s AND BINARY search_term = BINARY %s AND pubmed_id IN ({marks})''',
                        (gard_id, current_search_term, *batch),
                    )
                    if {str(value[0]) for value in writer.fetchall()} != set(batch):
                        mapped = False
                        break
                if not mapped:
                    continue
                copied += self._copy_rows(
                    writer, "publication_gard_pubmed", "s.id = %s", (row["id"],),
                    keys=["gard_id", "search_term", "year_range"],
                    overrides={"gard_id": gard_id, "search_term": search_term},
                )
            mysql.commit()
            if copied:
                logger.info(f"{gard_id}: copied {copied} complete historical publication_gard_pubmed responses for {search_term}.")
        except Exception:
            mysql.rollback()
            raise
        finally:
            reader.close()
            writer.close()

    def copy_gard_omim_mappings(self, mysql, logger, gard_id):

        '''
        Source associations are eligible only when the destination GARD data
        still lists that OMIM cross-reference. This avoids resurrecting retired
        associations or attaching other diseases that happen to share a PMID.
        The shared mapping task subsequently adds any new cross-references
        missing from the copy source; all writes use the current GARD ID.
        '''
        self.validate_connection(mysql)
        if gard_id != self.current_gard_id:
            raise ValueError("OMIM mapping copies must use the current GARD node.")
        cursor = mysql.cursor(buffered=True)
        try:
            cursor.execute(f"SELECT Label_Xref FROM `{self.TARGET_DATABASE}`.gard WHERE GardID = %s", (gard_id,))
            omim_ids = set()
            for (xrefs,) in cursor.fetchall():
                for item in (xrefs or "").split(","):
                    prefix, separator, identifier = item.strip().partition(":")
                    if prefix == "OMIM" and separator and identifier.isdigit():
                        omim_ids.add(identifier)
            identifiers = sorted(omim_ids, key=int)
            copied = 0
            for start in range(0, len(identifiers), self.BATCH_SIZE):
                batch = identifiers[start:start + self.BATCH_SIZE]
                marks = ",".join(["%s"] * len(batch))
                copied += self._copy_rows(
                    cursor, "publication_gard_omim_mapping", f"s.gard_id = %s AND s.omim_id IN ({marks})",
                    (gard_id, *batch), keys=["gard_id", "omim_id"], overrides={"gard_id": gard_id}, restage_existing=True,
                )
            mysql.commit()
            logger.info(f"{gard_id}: reused {copied} current GARD/OMIM associations from {self.SOURCE_DATABASE} into {self.TARGET_DATABASE}.")
        except Exception:
            mysql.rollback()
            raise
        finally:
            cursor.close()

    def copy_omim_data(self, mysql, logger, gard_id):

        '''
        Cache OMIM entries for this disease's staged associations before the
        shared API task asks for missing entries. Validate the entry's identity,
        preserve existing destination JSON, and copy only references actually
        present in that chosen JSON. Re-stage relevant existing OMIM entries:
        the PMID importer/OMIMRef graph task must see them for this new GARD
        association even if another disease previously consumed their flags.
        '''
        self.validate_connection(mysql)
        if gard_id != self.current_gard_id:
            raise ValueError("OMIM data copies must use the current GARD node.")
        reader = mysql.cursor(dictionary=True, buffered=True)
        writer = mysql.cursor(buffered=True)
        copied = 0
        copied_pairs = 0
        try:
            reader.execute(f"SELECT DISTINCT omim_id FROM `{self.TARGET_DATABASE}`.publication_gard_omim_mapping WHERE gard_id = %s AND omim_id IS NOT NULL", (gard_id,))
            identifiers = [row["omim_id"] for row in reader.fetchall()]
            for start in range(0, len(identifiers), self.BATCH_SIZE):
                batch = identifiers[start:start + self.BATCH_SIZE]
                marks = ",".join(["%s"] * len(batch))
                reader.execute(f"SELECT s.id, s.omim_id, s.entry_json FROM `{self.SOURCE_DATABASE}`.publication_omim s WHERE s.omim_id IN ({marks}) AND s.id = (SELECT MAX(o.id) FROM `{self.SOURCE_DATABASE}`.publication_omim o WHERE o.omim_id = s.omim_id)", batch)
                for row in reader.fetchall():
                    try:
                        entries = json.loads(row["entry_json"])["omim"]["entryList"]
                        entries = [entries] if isinstance(entries, dict) else entries
                        if not isinstance(entries, list):
                            raise ValueError("Invalid OMIM entry list")
                        entry = next(item["entry"] for item in entries if isinstance(item, dict) and isinstance(item.get("entry"), dict) and str(item["entry"].get("mimNumber")) == str(row["omim_id"]))
                        references = entry.get("referenceList") or []
                        references = [references] if isinstance(references, dict) else references
                        if not isinstance(references, list):
                            raise ValueError("Invalid OMIM reference list")
                        pmids = sorted({str(item["reference"]["pubmedID"]) for item in references if isinstance(item, dict) and isinstance(item.get("reference"), dict) and str(item["reference"].get("pubmedID", "")).isdigit()}, key=int)
                    except (ValueError, TypeError, KeyError, StopIteration):
                        logger.warning(f"Invalid cached OMIM entry {row['omim_id']}; leave it for the normal OMIM retrieval task.")
                        continue
                    copied += self._copy_rows(writer, "publication_omim", "s.id = %s", (row["id"],), keys=["omim_id"])
                    for ref_start in range(0, len(pmids), self.BATCH_SIZE):
                        refs = pmids[ref_start:ref_start + self.BATCH_SIZE]
                        ref_marks = ",".join(["%s"] * len(refs))
                        copied_pairs += self._copy_rows(
                            writer, "publication_omim_pubmed_mapping",
                            f'''s.omim_id = %s AND s.pubmed_id IN ({ref_marks})
                                AND EXISTS (SELECT 1 FROM `{self.TARGET_DATABASE}`.publication_omim d
                                    WHERE d.omim_id = s.omim_id AND BINARY d.entry_json = BINARY %s)''',
                            (row["omim_id"], *refs, row["entry_json"]), keys=["omim_id", "pubmed_id"],
                        )
                writer.execute(f"UPDATE `{self.TARGET_DATABASE}`.publication_omim SET is_new = 1 WHERE omim_id IN ({marks})", batch)
                writer.execute(f"UPDATE `{self.TARGET_DATABASE}`.publication_gard_omim_mapping SET is_new = 1 WHERE gard_id = %s AND omim_id IN ({marks})", (gard_id, *batch))
                mysql.commit()
            logger.info(f"{gard_id}: reused {copied} publication_omim entries and {copied_pairs} OMIM/PMID pairs from {self.SOURCE_DATABASE} into {self.TARGET_DATABASE}.")
        except Exception:
            mysql.rollback()
            raise
        finally:
            reader.close()
            writer.close()

    def copy_clinical_trials(self, mysql, logger, gard_id, disease, nctids, source_url):

        '''
        Prefer an already imported target study, otherwise reuse a valid source
        study for a searched NCT ID. Only current gard_id/disease mappings are
        inserted. Canonical trials, annotations, and PMID references are shared
        by NCT ID; RxNorm mapping rows are explicitly rebound to this disease.
        Return unhandled IDs to the downloader rather than making empty trials.
        '''
        self.validate_connection(mysql)
        if gard_id != self.current_gard_id:
            raise ValueError("Clinical-trial copying must use the current GARD node.")
        handled = set()
        cursor = mysql.cursor(dictionary=True, buffered=True)
        writer = mysql.cursor(buffered=True)
        comparer = ClinicalTrialStudyChangeHandler(mysql, logger)
        try:
            for start in range(0, len(nctids), self.BATCH_SIZE):
                batch = list(dict.fromkeys(nctids[start:start + self.BATCH_SIZE]))
                batch_started = perf_counter()
                batch_number = start // self.BATCH_SIZE + 1
                batch_count = (len(nctids) + self.BATCH_SIZE - 1) // self.BATCH_SIZE
                logger.info(f"{gard_id}: trial copy batch {batch_number}/{batch_count}: checking {len(batch)} NCT IDs.")
                marks = ",".join(["%s"] * len(batch))
                cursor.execute(f"SELECT nctid, studies, is_new FROM `{self.TARGET_DATABASE}`.clinical_trial_unique WHERE nctid IN ({marks})", batch)
                targets = {row["nctid"]: row for row in cursor.fetchall()}
                '''
                Avoid transporting the same large studies JSON from staging
                when a canonical row already supplies it. For production,
                clinical_trial must still contain the NCT ID, but its payload
                is needed only when clinical_trial_unique has no row.

                Example: 100 IDs all have canonical source rows. Read those
                payloads once, plus 100 small staging IDs, instead of reading
                both complete staging and canonical payloads for every ID.
                '''
                staged = {}
                missing_targets = [nctid for nctid in batch if nctid not in targets]
                if missing_targets:
                    missing_marks = ",".join(["%s"] * len(missing_targets))
                    cursor.execute(
                        f'''SELECT s.nctid, s.studies FROM `{self.TARGET_DATABASE}`.clinical_trial s
                            JOIN (SELECT nctid, MAX(id) AS id FROM `{self.TARGET_DATABASE}`.clinical_trial
                                  WHERE nctid IN ({missing_marks}) GROUP BY nctid) latest ON latest.id = s.id''', missing_targets,
                    )
                    staged = {row["nctid"]: row for row in cursor.fetchall()}
                cursor.execute(f"SELECT nctid, studies, is_new, annotation_processed FROM `{self.SOURCE_DATABASE}`.clinical_trial_unique WHERE nctid IN ({marks})", batch)
                uniques = {row["nctid"]: row for row in cursor.fetchall()}
                cursor.execute(f"SELECT DISTINCT nctid FROM `{self.SOURCE_DATABASE}`.clinical_trial WHERE nctid IN ({marks})", batch)
                sources = {row["nctid"]: row for row in cursor.fetchall()}
                legacy_ids = [nctid for nctid in sources if nctid not in uniques]
                if legacy_ids:
                    legacy_marks = ",".join(["%s"] * len(legacy_ids))
                    cursor.execute(
                        f'''SELECT s.nctid, s.studies FROM `{self.SOURCE_DATABASE}`.clinical_trial s
                            JOIN (SELECT nctid, MAX(id) AS id FROM `{self.SOURCE_DATABASE}`.clinical_trial
                                  WHERE nctid IN ({legacy_marks}) GROUP BY nctid) latest ON latest.id = s.id''', legacy_ids,
                    )
                    sources.update((row["nctid"], row) for row in cursor.fetchall())
                reads_finished = perf_counter()
                compatible = []
                prepared = {}
                canonical_source_ids = []
                canonical_rows = []
                for nctid in batch:
                    source = (uniques.get(nctid) or sources.get(nctid)) if nctid in sources else None
                    target = targets.get(nctid) or staged.get(nctid)
                    chosen = target or source
                    try:
                        study = json.loads(chosen["studies"]) if chosen else None
                        if not isinstance(study, dict) or _safe_get(study, "protocolSection", "identificationModule", "nctId") != nctid:
                            raise ValueError("missing or mismatched NCT ID")
                    except (ValueError, TypeError, AttributeError):
                        if chosen:
                            logger.warning(f"Invalid cached study for {nctid}; use the ClinicalTrials.gov downloader.")
                        continue
                    prepared[nctid] = study
                    '''
                    If the chosen study IS the source row, equality is already
                    certain after validation; byte-identical target/source JSON
                    also needs no deep copy/normalization. Retain the shared
                    semantic comparison for genuinely different JSON strings.
                    '''
                    if source and (chosen is source or source.get("studies") == chosen.get("studies") or comparer.compare_studies(source.get("studies"), study)[0]):
                        compatible.append(nctid)
                    title = _safe_get(study, "protocolSection", "identificationModule", "briefTitle") or "N/A"
                    summary = _safe_get(study, "protocolSection", "descriptionModule", "briefSummary") or "N/A"
                    status = _safe_get(study, "protocolSection", "statusModule", "overallStatus") or "N/A"
                    if nctid not in targets:
                        if nctid in compatible and nctid in uniques:
                            canonical_source_ids.append(nctid)
                        else:
                            canonical_rows.append((nctid, json.dumps(study), title, summary, status))
                prepared_finished = perf_counter()
                '''
                Copy existing canonical studies inside MySQL in ONE statement
                per batch. Only target-staged/legacy studies without a reusable
                canonical source row need a VALUES insert from Python.
                Existing target canonical rows and their flags are preserved.
                '''
                if canonical_source_ids:
                    source_marks = ",".join(["%s"] * len(canonical_source_ids))
                    self._copy_rows(writer, "clinical_trial_unique", f"s.nctid IN ({source_marks})", canonical_source_ids, keys=["nctid"])
                if canonical_rows:
                    row_marks = ",".join(["(%s, %s, %s, %s, %s, 1)"] * len(canonical_rows))
                    writer.execute(
                        f'''INSERT INTO `{self.TARGET_DATABASE}`.clinical_trial_unique (nctid, studies, brief_title, brief_summary, overall_status, is_new)
                            VALUES {row_marks} ON DUPLICATE KEY UPDATE nctid = nctid''',
                        [value for row in canonical_rows for value in row],
                    )
                if prepared:
                    '''
                    Build all current-GARD mappings with one INSERT ... SELECT
                    and one flag UPDATE instead of two statements per study.
                    Join the destination canonical JSON inside MySQL, so large
                    studies are not serialized and sent back over the network.
                    Copy the canonical studies/title/summary fields as stored;
                    only GARD ID, disease, URL, and staging flag are overridden.
                    List discovery columns explicitly so staging-only columns
                    such as alert_sent keep their table defaults; those columns
                    do not exist on clinical_trial_unique.

                    Example: 100 new source-canonical IDs for GARD:0000522 need
                    one canonical copy, one mapping insert, and one flag update,
                    rather than roughly 300 individual write requests.
                    NOT EXISTS still preserves retry-safe association checks;
                    every mapping binds the current GARD ID and disease term.
                    '''
                    prepared_ids = list(prepared)
                    prepared_marks = ",".join(["%s"] * len(prepared_ids))
                    writer.execute(
                        f'''INSERT INTO `{self.TARGET_DATABASE}`.clinical_trial (gardId, disease, nctid, studies, brief_title, brief_summary, url, is_new)
                            SELECT %s, %s, s.nctid, s.studies, s.brief_title, s.brief_summary, %s, 1
                            FROM `{self.TARGET_DATABASE}`.clinical_trial_unique s
                            WHERE s.nctid IN ({prepared_marks}) AND NOT EXISTS (
                                SELECT 1 FROM `{self.TARGET_DATABASE}`.clinical_trial d
                                WHERE d.gardId = %s AND d.disease = %s AND d.nctid = s.nctid)''',
                        (gard_id, disease, source_url, *prepared_ids, gard_id, disease),
                    )
                    writer.execute(
                        f'''UPDATE `{self.TARGET_DATABASE}`.clinical_trial SET is_new = 1
                            WHERE gardId = %s AND disease = %s AND nctid IN ({prepared_marks})
                            AND (is_new IS NULL OR is_new <> 1)''',
                        (gard_id, disease, *prepared_ids),
                    )
                writes_finished = perf_counter()

                '''
                Source enrichment is reusable only for the same study JSON
                after its source staging flag has been cleared. Pending source
                trials may still carry derived rows from a previous version.
                The source bulk annotation generator uses completion marker 2;
                absent rows without that marker fall back to the NLP task.
                '''
                enriched_sources = [nctid for nctid in compatible if uniques.get(nctid, {}).get("is_new") == 0]
                if enriched_sources:
                    compatible_marks = ",".join(["%s"] * len(enriched_sources))
                    for table, keys in (("clinical_trial_annotation", None), ("clinical_trial_nctid_pmids_mapping", ["nctid", "pmid"])):
                        self._copy_rows(writer, table, f"s.nctid IN ({compatible_marks}) AND EXISTS (SELECT 1 FROM `{self.TARGET_DATABASE}`.clinical_trial_unique u WHERE u.nctid = s.nctid AND u.is_new = 1)", enriched_sources, keys=keys, restage_existing=True)
                    cursor.execute(f"SELECT DISTINCT nctid FROM `{self.SOURCE_DATABASE}`.clinical_trial_annotation WHERE nctid IN ({compatible_marks})", enriched_sources)
                    annotated = {row["nctid"] for row in cursor.fetchall()}
                    annotated.update(nctid for nctid in enriched_sources if (uniques.get(nctid) or {}).get("annotation_processed") == 2)
                    self.annotation_nctids.update(annotated)

                '''
                The drug table is indexed by gardId/disease/nctid, not nctid
                alone. Join the indexed trial lookup to source mapping tuples
                before copying. Try destination cache first for an NCT already
                completed here; use production data only for compatible JSON.
                Every copied drug row receives the CURRENT GARD ID and term.
                '''
                completed_targets = [nctid for nctid in prepared if targets.get(nctid, {}).get("is_new") == 0]
                reusable_drugs = set()
                for schema, identifiers in ((self.TARGET_DATABASE, completed_targets), (self.SOURCE_DATABASE, enriched_sources)):
                    if not identifiers:
                        continue
                    placeholders = ",".join(["%s"] * len(identifiers))
                    source_from = f'''`{schema}`.clinical_trial_intervention_drug s
                        JOIN (SELECT DISTINCT gardId, disease, nctid FROM `{schema}`.clinical_trial
                              WHERE nctid IN ({placeholders})) matched
                        ON s.gardId = matched.gardId AND s.disease <=> matched.disease AND s.nctid = matched.nctid'''
                    self._copy_rows(writer, "clinical_trial_intervention_drug", "1 = 1", identifiers, overrides={"gardId": gard_id, "disease": disease}, source_from=source_from)
                    cursor.execute(f"SELECT DISTINCT s.nctid FROM {source_from}", identifiers)
                    reusable_drugs.update(row["nctid"] for row in cursor.fetchall())
                if reusable_drugs:
                    '''
                    Existing target drug history alone does not prove that it
                    describes a newly changed study. Only skip RxNorm when
                    usable rows came from a completed target or compatible
                    source snapshot; otherwise normal remapping must run.
                    '''
                    identifiers = list(reusable_drugs)
                    placeholders = ",".join(["%s"] * len(identifiers))
                    writer.execute(f"UPDATE `{self.TARGET_DATABASE}`.clinical_trial_intervention_drug SET is_new = 1 WHERE gardId = %s AND disease = %s AND nctid IN ({placeholders})", (gard_id, disease, *identifiers))
                    self.drug_nctids.update(reusable_drugs)
                for nctid, study in prepared.items():
                    interventions = _safe_get(study, "protocolSection", "armsInterventionsModule", "interventions", default=[]) or []
                    if isinstance(interventions, list) and not any(isinstance(item, dict) and item.get("type") == "DRUG" for item in interventions):
                        self.drug_nctids.add(nctid)
                mysql.commit()
                handled.update(prepared)
                finished = perf_counter()
                logger.info(f"{gard_id}: trial copy batch {batch_number}/{batch_count}: available={len(prepared)}/{len(batch)}, read={reads_finished - batch_started:.2f}s, prepare={prepared_finished - reads_finished:.2f}s, write={writes_finished - prepared_finished:.2f}s, enrichment={finished - writes_finished:.2f}s, total={finished - batch_started:.2f}s.")
            logger.info(f"RDAS study reuse: available {len(handled)}/{len(nctids)} NCT IDs; all clinical_trial and drug mappings use {gard_id}: {disease}.")
            return handled
        except Exception:
            mysql.rollback()
            raise
        finally:
            writer.close()
            cursor.close()

    def copy_trial_publications(self, mysql, logger):

        '''
        The shared PMID mapper clears flags on existing pairs. Restage pairs
        for pending canonical trials before copying/downloading their Articles,
        so copied references reach the graph even when the pair already exists.
        '''
        self.validate_connection(mysql)
        cursor = mysql.cursor(buffered=True)
        try:
            cursor.execute(f'''UPDATE `{self.TARGET_DATABASE}`.clinical_trial_nctid_pmids_mapping m
                              JOIN `{self.TARGET_DATABASE}`.clinical_trial_unique u ON u.nctid = m.nctid
                              SET m.is_new = 1 WHERE u.is_new = 1''')
            mysql.commit()
            last_pmid = 0
            while True:
                cursor.execute(f'''SELECT DISTINCT pmid FROM `{self.TARGET_DATABASE}`.clinical_trial_nctid_pmids_mapping
                                  WHERE is_new = 1 AND pmid > %s ORDER BY pmid LIMIT %s''', (last_pmid, self.BATCH_SIZE))
                rows = cursor.fetchall()
                if not rows:
                    break
                last_pmid = rows[-1][0]
                self.copy_publications(mysql, logger, [row[0] for row in rows])
        finally:
            cursor.close()

    @contextmanager
    def enrichment_scope(self, task_name, logger):

        '''
        Shared enrichment tasks select is_new=1. Temporarily mark reused inputs
        is_new=2 for ONE task, then restore their exact row IDs before graph or
        person processing. Use a separate parent-owned connection because the
        shared task closes its own connection. Restore in finally on errors.
        Marker 2 distinguishes suspended inputs from completed rows (0). A hard
        process stop leaves gard.updated NULL; recover_interrupted_scopes()
        restores suspended inputs before the restarted GARD loop begins.
        '''
        specification = {
            "ClinicalTrialDrugInterventionMappingTask": ("clinical_trial", self.drug_nctids),
            "NewClinicalTrialAnnotationTask": ("clinical_trial_unique", self.annotation_nctids),
            "PublicationEpiNhsLocalClassificationTask": ("publication_article", set()),
        }
        if task_name not in specification:
            yield True
            return
        table, completed = specification[task_name]
        mysql = DBConnection().mysql_conn()
        if mysql is None:
            raise RuntimeError("Cannot open the GARD enrichment-scope connection.")
        masked_ids = []
        cursor = mysql.cursor(buffered=True)
        try:
            self.validate_connection(mysql)
            identifiers = list(completed)
            for start in range(0, len(identifiers), self.BATCH_SIZE):
                batch = identifiers[start:start + self.BATCH_SIZE]
                marks = ",".join(["%s"] * len(batch))
                cursor.execute(f"SELECT id FROM `{self.TARGET_DATABASE}`.`{table}` WHERE is_new = 1 AND nctid IN ({marks})", batch)
                row_ids = [row[0] for row in cursor.fetchall()]
                if row_ids:
                    row_marks = ",".join(["%s"] * len(row_ids))
                    cursor.execute(f"UPDATE `{self.TARGET_DATABASE}`.`{table}` SET is_new = 2 WHERE id IN ({row_marks})", row_ids)
                    masked_ids.extend(row_ids)
            extra_condition = "AND is_EPI IS NULL" if table == "publication_article" else ""
            cursor.execute(f"SELECT 1 FROM `{self.TARGET_DATABASE}`.`{table}` WHERE is_new = 1 {extra_condition} LIMIT 1")
            has_work = cursor.fetchone() is not None
            mysql.commit()
            logger.info(f"{task_name}: reused {len(masked_ids)} input rows; missing enrichment remains={has_work}.")
            yield has_work
        finally:
            mysql.rollback()
            try:
                for start in range(0, len(masked_ids), self.BATCH_SIZE):
                    batch = masked_ids[start:start + self.BATCH_SIZE]
                    marks = ",".join(["%s"] * len(batch))
                    cursor.execute(f"UPDATE `{self.TARGET_DATABASE}`.`{table}` SET is_new = 1 WHERE id IN ({marks})", batch)
                mysql.commit()
            finally:
                cursor.close()
                mysql.close()

    def recover_interrupted_scopes(self, mysql, logger):

        '''
        Only this GARD runner reserves is_new=2 for an enrichment exclusion.
        Restore such rows before discovery, even if the previous process was
        killed before its finally block ran. This preserves graph/person work
        without treating ordinary completed rows (is_new=0) as new again.
        '''
        self.validate_connection(mysql)
        cursor = mysql.cursor()
        try:
            for table in ("clinical_trial", "clinical_trial_unique"):
                cursor.execute(f"UPDATE `{self.TARGET_DATABASE}`.`{table}` SET is_new = 1 WHERE is_new = 2")
                if cursor.rowcount:
                    logger.info(f"Restored {cursor.rowcount} interrupted reuse inputs in {table} to is_new=1.")
            mysql.commit()
        except Exception:
            mysql.rollback()
            raise
        finally:
            cursor.close()

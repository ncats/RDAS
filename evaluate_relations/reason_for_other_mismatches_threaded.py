"""
    Evaluates the mismatch reason between the GARD name, PubMed Abstract 
    or Title, and Synonyms, and generates a reason for the mismatch. 
    The number of max-workers can be edited according to the system's capacity. 
"""

import json
import os
import sys
import time
import traceback
import concurrent.futures
from typing import Optional, Tuple

import ollama

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.conn import DBConnection as db

MODEL_NAME = "mistral:7b"
TABLE_NAME = "grant_gard_project_process_relations_sip"
BATCH_SIZE = 10
REASON_COLUMN = "mistral_reason"          # existing column with fixed labels
DETAIL_COLUMN = "mistral_other_detail"    # NEW column for the generated label
GARDID = None
REQUEST_TIMEOUT = 900
SOFT_TIMEOUT = 180
MAX_WORKERS = 2 # Can change to 1 for single, in event that 2 at once is timing out.
MAX_RETRIES = 2

_client = ollama.Client(timeout=REQUEST_TIMEOUT)
_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None

PROMPT_TEMPLATE = """Disease name: {disease_name}
Synonyms (split by $$$): {synonyms}
Text/title: {text}

This text/title was flagged as a mismatch for the disease above. 

In 10 words MAXIMUM, give a short, specific label describing WHY this text is
not about the disease. Do not write a full sentence or explanation -- just a
short phrase, like a tag. NO MORE THAN 10 WORDS.

Examples of the style wanted (do not reuse these unless they truly apply):
- "mentions related gene only"
- "describes lab technique, not disease"
- "institution name coincidence"
- "author affiliation, not subject"
- "clinical trial infrastructure, not diagnosis"

Return ONLY valid JSON in this exact shape, with no other keys:
{{"detail": "maximum 10 word label"}}

Do not include punctuation beyond the label itself, no quotes within the
label, no trailing period, no explanation.
"""

# Helper method to check if column exists in the table, and create it if it doesn't.
def ensure_column_exists(conn, column_name: str, column_def: str = "VARCHAR(255) NULL"):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
            AND table_name = %s
            AND column_name = %s
    """, (TABLE_NAME, column_name))
    exists = cursor.fetchone()[0] > 0

    if not exists:
        print(f"Column `{column_name}` not found on `{TABLE_NAME}`, creating it...", flush=True)
        cursor.execute(f"""
            ALTER TABLE `{TABLE_NAME}`
            ADD COLUMN `{column_name}` {column_def}
        """)
        conn.commit()
    cursor.close()

# Helper method to normalize the synonyms.
def normalize_synonyms(raw_synonyms) -> str:
    if not raw_synonyms:
        return ""
    return str(raw_synonyms).strip()

# Helper method to retrieve the row's PubMed Text - Either Abstract text or Project title.
def get_source_text(row) -> str:
    source_type = (row.get("source_type") or "").strip().lower()
    if source_type == "title":
        return (row.get("project_title") or "").strip()
    if source_type == "abstract":
        return (row.get("abstract_text") or "").strip()
    raise ValueError(f"Unknown source_type: {row.get('source_type')!r}")

# Helper method that checks the LLM generated response to ensure it is properly formatted.
def normalize_detail(payload: dict) -> str:
    if not isinstance(payload, dict):
        raise ValueError(f"Model response was not a JSON object: {payload}")

    if set(payload.keys()) != {"detail"}:
        raise ValueError(f"Model response had unexpected keys: {list(payload.keys())}")

    detail = payload.get("detail")
    if not isinstance(detail, str) or not detail.strip():
        raise ValueError(f"Missing detail in model response: {payload}")

    detail = detail.strip().strip('."\'').lower()

    word_count = len(detail.split())
    if word_count == 0 or word_count > 10: 
        raise ValueError(f"Detail label out of expected length: {detail!r}")

    return detail

# Helper method that calls ollama to provide prompt to mistral. Receives the generated reason back.
def _call_ollama(disease_name: str, synonyms: str, text: str) -> str:
    prompt = PROMPT_TEMPLATE.format(
        disease_name=disease_name,
        synonyms=synonyms,
        text=text,
    )

    start = time.time()
    result = _client.chat(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        options={
            "temperature": 0,
            "num_predict": 60, 
            "num_ctx": 4096,
        },
    )["message"]["content"].strip()
    elapsed = time.time() - start
    print(f"  ollama call took {elapsed:.2f}s -> {result}", flush=True)
    return result


# Wraps _call_ollama + parsing + validation with retry logic. If the model
# returns malformed JSON or a detail that fails validation,
# this retries up to max_retries more times, rather than giving up on the first bad response.
def call_model_with_retry(row_id, disease_name, synonyms, text, max_retries=MAX_RETRIES) -> str:
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 2):
        try:
            raw = _call_ollama(disease_name, synonyms, text)
            payload = json.loads(raw)
            detail = normalize_detail(payload)
            if attempt > 1:
                print(f"[{row_id}] Succeeded on retry attempt {attempt}", flush=True)
            return detail

        except json.JSONDecodeError:
            last_exc = ValueError(f"Model did not return valid JSON: {raw}")
        except Exception as exc:
            last_exc = exc

        if attempt <= max_retries:
            backoff = 2 * attempt
            print(f"[{row_id}] Attempt {attempt} failed ({last_exc}); retrying in {backoff}s...", flush=True)
            time.sleep(backoff)

    raise last_exc


# Submits every row in the batch to the thread pool, then polls for
# completions in a loop so it can log which rows are still pending if the model is slow.
def process_batch(rows, write_cursor, conn):
    future_to_row = {}

    for row in rows:
        row_id = row["id"]
        disease_name = (row.get("gard_name") or "").strip()
        synonym_text = normalize_synonyms(row.get("synonym"))

        try:
            source_text = get_source_text(row)
        except ValueError as exc:
            print(f"[{row_id}] Skipping: {exc}", flush=True)
            continue

        if not source_text:
            print(f"[{row_id}] Skipping: empty text for source_type={row.get('source_type')!r}", flush=True)
            continue

        future = _executor.submit(call_model_with_retry, row_id, disease_name, synonym_text, source_text)
        future_to_row[future] = row

    update_sql = f"""
        UPDATE `{TABLE_NAME}`
        SET `{DETAIL_COLUMN}` = %s
        WHERE id = %s
    """

    inserted_count = 0
    pending = set(future_to_row.keys())

    while pending:
        done, pending = concurrent.futures.wait(
            pending, timeout=SOFT_TIMEOUT, return_when=concurrent.futures.FIRST_COMPLETED
        )

        if not done:
            stuck_ids = [future_to_row[f]["id"] for f in pending]
            print(f"Still waiting on rows: {stuck_ids}", flush=True)
            continue

        for future in done:
            row = future_to_row[future]
            row_id = row["id"]
            try:
                detail = future.result()
                write_cursor.execute(update_sql, (detail, row_id))
                conn.commit()
                inserted_count += 1
                print(f"{detail!r} inserted for {row_id}", flush=True)
            except Exception as exc:
                print(f"Error/timeout processing GARD: {row.get('gard_name')} ({row_id}): {exc}", flush=True)

    return inserted_count

# Builds the SELECT query that finds candidate rows to label. 
def build_query() -> Tuple[str, Tuple]:
    # Only rows already labeled "Other" by the first pass, that haven't
    # gotten a detail label yet.
    where_clauses = [f"`{REASON_COLUMN}` = 'Other'", f"`{DETAIL_COLUMN}` IS NULL"]
    params = []

    if GARDID:
        where_clauses.insert(0, "gard_id = %s")
        params.append(GARDID)

    select_sql = f"""
        SELECT
            id,
            gard_id,
            gard_name,
            source_type,
            abstract_text,
            project_title,
            synonym
        FROM `{TABLE_NAME}`
        WHERE {' AND '.join(where_clauses)}
        ORDER BY id
    """

    return select_sql, tuple(params)

# Execute the prompt by calling the LLM, and having it label every
# unmatched row with a reason. Rows are processed in batches, and 
# are written to the DB as soon as each one finishes.
def process_rows():
    conn = db().mysql_conn()

    if conn is None:
        raise ConnectionError("Unable to connect to MySQL.")

    # Helper to ensure the column exists before we try to write to it. If it doesn't exist, create it.
    ensure_column_exists(conn, DETAIL_COLUMN)

    read_cursor = conn.cursor(buffered=True, dictionary=True)
    write_cursor = conn.cursor()

    select_sql, params = build_query()
    read_cursor.execute(select_sql, params)

    batch_num = 0
    total_updated = 0

    try:
        while True:
            rows = read_cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break

            batch_num += 1
            print(f"Batch {batch_num}: processing {len(rows)} rows concurrently...", flush=True)

            batch_start = time.time()
            inserted_count = process_batch(rows, write_cursor, conn)
            batch_elapsed = time.time() - batch_start

            total_updated += inserted_count

            print(f"Batch {batch_num} took {batch_elapsed:.2f}s "
                  f"({batch_elapsed / len(rows):.2f}s/row avg)", flush=True)
            print(f"Updated {inserted_count} rows. Total: {total_updated}\n", flush=True)

    except Exception as exc:
        print(f"Could not finish executing the process: {exc}\n", flush=True)
        raise

    finally:
        print(f"Total rows updated this run: {total_updated}\n", flush=True)
        if read_cursor is not None:
            read_cursor.close()
        if write_cursor is not None:
            write_cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()

# Wrapper that keeps restarting process_rows() if it ever raises,
# until there are no more unlabeled rows left to process (i.e.
# process_rows completes a full pass without error). Each restart
# creates a fresh ThreadPoolExecutor so no state/threads leak across
# restarts.
def main():
    global _executor

    RESTART_DELAY = 10
    MAX_CONSECUTIVE_FAILURES = 10
    consecutive_failures = 0

    while True:
        _executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
        try:
            process_rows()
            print("All 'Other' rows processed. Exiting.", flush=True)
            break

        except Exception:
            consecutive_failures += 1
            print(f"Run crashed (failure #{consecutive_failures}):", flush=True)
            traceback.print_exc()

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"Hit {MAX_CONSECUTIVE_FAILURES} consecutive failures. Giving up.", flush=True)
                return 1

            print(f"Restarting in {RESTART_DELAY}s...\n", flush=True)
            time.sleep(RESTART_DELAY)

        finally:
            _executor.shutdown(wait=False)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

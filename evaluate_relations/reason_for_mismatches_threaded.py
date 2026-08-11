"""
    Evaluates the mismatch reason between the GARD name, PubMed Abstract 
    or Title, and Synonyms, and provides a reason for the mismatch. 
    A list of reasons have already been made to select from. 
    If the reasons don't match the pre-made ones, the row will be left as null. 
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

# Make project imports available when running this file directly.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.conn import DBConnection as db

MODEL_NAME = "mistral:7b"
TABLE_NAME = "grant_gard_project_process_relations_sip"
BATCH_SIZE = 10
RELATED_COLUMN = "mistral_reason"
RESPONSE_COLUMN = "mistral_related_bool"
MATCHED_COLUMN = "matched_term"
GARDID = None  # set to a gard_id string to limit processing, or leave None for all rows
REQUEST_TIMEOUT = 900
MAX_WORKERS = 2 # Can change to 1, in the event that 2 at once is timing out. 

_client = ollama.Client(timeout=REQUEST_TIMEOUT)
_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None

VALID_REASONS = {
    "Wrong disease mentioned",
    "Broader/related, not this specific disease",
    "Synonym/abbreviation without context",
    "Passing mention / not the subject",
    "Text too vague",
    "Other",
}

PROMPT_TEMPLATE = """Disease name: {disease_name}
Synonyms (split by $$$): {synonyms}
Text/title: {text}

You are deciding why this abstract text/project title is NOT a match for the disease, even though it was flagged as a possible match.

IMPORTANT: A short text or title is NOT automatically vague. Titles are naturally brief but clearly describe a specific, identifiable topic. Only treat something as vague if its content gives no identifiable topic at all never because of its length alone.

Example: The title "CHILD CARE AND DEVELOPMENT FUND (CCDF) POLICIES DATABASE PROJECT" is short, but it clearly describes a specific topic (childcare policy/funding database) unrelated to any disease. This is NOT "Text too vague" -- it is "Synonym/abbreviation without context", because the word "child" matched a synonym without any disease-relevant meaning.

Follow these steps in order:

STEP 1: Check if any synonyms from the Synonyms list appears in the text/title in an unrelated context (e.g., "child", "glass", "fish", "map") from the disease. In this case, answer "Synonym/abbreviation without context" and stop.

STEP 2: If Step 1 does not apply, check if the text/title discusses the general disease category, a different subtype, a childhood or adult version, or a related/associated condition. If so, answer "Broader/related, not this specific disease".

STEP 3: If Step 1 or 2 do not apply, check if the text/title is about a clearly different, specific disease. If so, answer "Wrong disease mentioned".

STEP 4: If none of the above apply, check if the disease is mentioned only briefly as background/citation/example, not as the subject. If so, answer "Passing mention / not the subject".

STEP 5: Only if the text/title has NO identifiable topic at all (e.g., "Project Summary", "Core B", "Year 2 Report") -- not merely because it is short -- answer "Text too vague".

STEP 6: Otherwise, answer "Other".

Return ONLY valid JSON in this exact shape, with no other keys:
{{"reason": "one of the allowed labels"}}

Allowed reason labels:
- Synonym/abbreviation without context
- Wrong disease mentioned
- Broader/related, not this specific disease
- Passing mention / not the subject
- Text too vague
- Other

Do not include any explanation, evidence, quoted text, or extra keys. Return only the "reason" key.
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

# Helper method that checks the LLM provided response to ensure it is properly formatted.
def normalize_reason(payload: dict) -> str:
    if not isinstance(payload, dict):
        raise ValueError(f"Model response was not a JSON object: {payload}")

    if set(payload.keys()) != {"reason"}:
        raise ValueError(f"Model response had unexpected keys: {list(payload.keys())}")

    reason = payload.get("reason")

    if not isinstance(reason, str) or not reason.strip():
        raise ValueError(f"Missing reason in model response: {payload}")

    reason = reason.strip()
    if reason not in VALID_REASONS:
        raise ValueError(f"Invalid reason label: {reason}")

    return reason


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
        options={"temperature": 0},
    )["message"]["content"].strip()
    elapsed = time.time() - start
    print(f"  ollama call took {elapsed:.2f}s -> {result}", flush=True)
    return result


# Calls the model, parses its raw text response as JSON, and validates the
# resulting reason label. Any parsing/validation failure raises, which
# process_batch() catches per-row so one bad response doesn't kill the batch.
def call_model(row_id, disease_name: str, synonyms: str, text: str) -> str:
    raw = _call_ollama(disease_name, synonyms, text)

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Model did not return valid JSON: {raw}") from exc

    reason = normalize_reason(payload)
    print(f"[{row_id}] Parsed -> reason={reason!r}", flush=True)
    return reason



# Submit every row in the batch, then write each result to the database
# immediately as it completes. If the process dies partway through a 
# batch, everything that finished before the crash is already safely committed, not lost.
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

        future = _executor.submit(call_model, row_id, disease_name, synonym_text, source_text)
        future_to_row[future] = row

    update_sql = f"""
        UPDATE `{TABLE_NAME}`
        SET `{RELATED_COLUMN}` = %s
        WHERE id = %s
    """

    inserted_count = 0

    for future in concurrent.futures.as_completed(future_to_row):
        row = future_to_row[future]
        row_id = row["id"]

        try:
            reason = future.result()
            write_cursor.execute(update_sql, (reason, row_id))
            conn.commit()
            inserted_count += 1
            print(f"{reason!r} inserted for {row_id}", flush=True)

        except Exception as exc:
            print(f"Error/timeout processing GARD: {row.get('gard_name')} ({row_id}): {exc}", flush=True)

    return inserted_count


# Builds the SELECT query that finds candidate rows to label: rows already
# flagged as "not related" (RESPONSE_COLUMN = '0') that don't yet have a
# reason (RELATED_COLUMN IS NULL). 
def build_query() -> Tuple[str, Tuple]:
    where_clauses = [f"`{RESPONSE_COLUMN}` = '0'", f"`{RELATED_COLUMN}` IS NULL"]
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
            synonym,
            {RESPONSE_COLUMN},
            {RELATED_COLUMN},
            {MATCHED_COLUMN}
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
    ensure_column_exists(conn, RELATED_COLUMN)

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

    RESTART_DELAY = 10   # seconds to wait before retrying after a crash
    MAX_CONSECUTIVE_FAILURES = 10
    consecutive_failures = 0

    while True:
        _executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
        try:
            process_rows()
            print("All rows processed. Exiting.", flush=True)
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
            _executor.shutdown(wait=False)  # don't block restart on zombie threads

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

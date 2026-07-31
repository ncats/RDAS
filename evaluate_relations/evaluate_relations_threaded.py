"""
    Evaluates the relations between the GARD name and PubMed Abstract or Title. 
    Will provide 1 or 0 for match or mismatch
    This version does 2 rows at a time for a faster process. Any that timed out can be run again using the single version. 
"""

import os
import sys
import time
import traceback
import ollama
import concurrent.futures
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from baseclass.conn import DBConnection as db

# LLM Model selected
MODEL_NAME = "mistral:7b"
TABLE_NAME = "grant_gard_project_process_relations_sip"
BATCH_SIZE = 10         
RELATED = "mistral_related_bool"
REQUEST_TIMEOUT = 600
MAX_WORKERS = 2

_client = ollama.Client(timeout=REQUEST_TIMEOUT)


# Helper method to validate the generated response.
def validate_response(result: str):
    text = result.strip()

    if text == "1":
        return 1

    if text == "0":
        return 0

    return None


# Helper method that calls to ollama, providing the prompt to Mistral LLM. 
# Attempts to return the LLM's response as 1 or 0.
def _call_ollama(name: str, text: str) -> str:
    start = time.time()
    result = _client.chat(
        model=MODEL_NAME,
        messages=[
            {
                "role": "user",
                "content": f"""Disease name: {name}
                Text: {text}

                Does the text above discuss the exact disease "{name}"?

                Answer with exactly one character: 1 or 0
                1 = yes, the text is about this disease
                0 = no, the text is not about this disease

                Your entire response must be a single character: 1 or 0. No words, no punctuation, no explanation.""".strip()
            }
        ],
        options={"num_predict": 2, "temperature": 0}
    )["message"]["content"].strip()
    elapsed = time.time() - start
    print(f"  ollama call took {elapsed:.2f}s -> {result}", flush=True)
    return result


"""
    Submit every row in the batch, then write each result to the DB
    as it completes. If the process dies partway, everything that 
    finished is already safely committed, not lost.
"""
def process_batch(rows, write_cursor, conn):
    future_to_row = {}

    for row in rows:
        name = row['gard_name']
        text = row["abstract_text"] if row["source_type"] == "abstract" else row["project_title"]
        future = _executor.submit(_call_ollama, name, text)
        future_to_row[future] = row

    update_sql = f"""
        UPDATE `{TABLE_NAME}`
        SET `{RELATED}` = %s
        WHERE gard_id = %s
            AND id = %s
    """

    inserted_count = 0

    for future in concurrent.futures.as_completed(future_to_row):
        row = future_to_row[future]
        gard_id = row['gard_id']
        row_id = row['id']

        try:
            result = future.result()
            validated = validate_response(result)

            if validated is None:
                print(f"Unparseable response for {row_id}: {result!r}", flush=True)
                continue

            write_cursor.execute(update_sql, (validated, gard_id, row_id))
            conn.commit()
            inserted_count += 1
            print(f"{validated} inserted for {row_id}", flush=True)

        except Exception as exc:
            print(f"Error/timeout processing GARD: {row['gard_name']} ({row_id}): {exc}", flush=True)

    return inserted_count


"""
    Execute the prompt by calling the LLM, and having it take the original_name and changing it to the processed name. 
"""
def insert_response():
    # Create a connection to MySQL
    conn = db().mysql_conn()

    # If connection can't form, raise an error
    if conn is None:
        raise ConnectionError("Unable to connect to MySQL.")

    # Initialize cursors
    read_cursor = conn.cursor(buffered=True, dictionary=True)
    write_cursor = conn.cursor()

    select_sql = f"""
        SELECT * FROM `{TABLE_NAME}` 
        WHERE `{RELATED}` IS NULL
    """
    read_cursor.execute(select_sql)

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


"""
    Wrapper that keeps restarting insert_response() if it ever raises,
    until there are no more NULL rows left to process (i.e. insert_response
    completes a full pass without error). Each restart creates a fresh
    ThreadPoolExecutor so no state/threads leak across restarts.
"""
def main():
    global _executor

    RESTART_DELAY = 10   # seconds to wait before retrying after a crash
    MAX_CONSECUTIVE_FAILURES = 10
    consecutive_failures = 0

    while True:
        _executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
        try:
            insert_response()
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
"""
    Evaluates the relations between the GARD name and PubMed Abstract or Title. 
    Will provide 1 or 0 for match or mismatch
    This version does one row at a time, used in cases where the threaded version 
    timed out and left rows incomplete. 
"""

import os
import sys
import time
import ollama
import concurrent.futures
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from baseclass.conn import DBConnection as db

# LLM Model selected
MODEL_NAME = "mistral:7b"
TABLE_NAME = "grant_gard_project_process_relations_sip"
BATCH_SIZE = 5
RELATED = "mistral_related_bool"
TIMEOUT_SECONDS = 300  # 5 minutes

# One executor reused across calls.
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

# Helper method to validate the generated response.
def validate_response(result: str):

    text = result.strip()

    if text == "1":
        return 1

    if text == "0":
        return 0


# Helper method that calls to ollama, providing the prompt to Mistral LLM. 
# Attempts to return the LLM's response as 1 or 0.
def _call_ollama(name: str, text: str) -> str:
    start = time.time()
    result = ollama.chat(
        model=MODEL_NAME,
        messages=[
            {
                "role": "user",
                "content": f"""Disease name: {name}
                Text: {text}

                Is the text about the exact disease, {name}?
                
                Answer with exactly one character: 1 or 0
                1 = yes, the text is about this disease
                0 = no, the text is not about this disease

                Your entire response must be a single character: 1 or 0. 
                No words, no punctuation, no explanation.""".strip()
            }
        ],
        options={"num_predict": 2, "temperature": 0}
    )["message"]["content"].strip()
    elapsed = time.time() - start
    print(f"  ollama call took {elapsed:.2f}s -> {result}", flush=True)
    return result


"""
    Use a strict prompt so the model returns only the response.
    If ollama doesn't respond within TIMEOUT_SECONDS, give up on
    this row and return None so it's left as NULL for a future run.
"""
def process_text(name: str, text: str):
    future = _executor.submit(_call_ollama, name, text)

    try:
        # Gets response from mistral. Restricts to time so it doesn't run infinitely
        result = future.result(timeout=TIMEOUT_SECONDS)
        print(f"*{result}", end="\n", flush=True)

        # Clean and validate response. Can only be 1, 0, or null (if response was improper)
        validated = validate_response(result)
        return validated

    except concurrent.futures.TimeoutError:
        print(f"Timed out after {TIMEOUT_SECONDS}s processing GARD: {name}")
        future.cancel()  # won't interrupt an in-flight ollama call
        return None

    except Exception as exc:
        print(f"Error processing synonyms for GARD: {name}: {exc}")
        return None


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

    # Query to get data from rdas table
    select_sql = f"""
        SELECT * FROM `{TABLE_NAME}` 
        WHERE`{RELATED}` IS NULL
    """
    read_cursor.execute(select_sql)

    # Query to insert data to rdas table
    update_sql = f"""
        UPDATE `{TABLE_NAME}`
        SET `{RELATED}` = %s
        WHERE gard_id = %s
            AND id = %s
    """

    # Counters to track progress
    batch_num = 0
    total_updated = 0

    try: 
        while True: 
            rows = read_cursor.fetchmany(BATCH_SIZE)
            if not rows: 
                break
            
            batch_num += 1
            print(f"Batch {batch_num}...", end = "\n", flush = True)
            updated_values = []

            for row in rows:
                try: 
                    # Call helper method to create new organization name.
                    name = row['gard_name']
                    gard_id = row['gard_id']
                    synonyms = row['synonym']
                    if row["source_type"] == "abstract":
                        text = row["abstract_text"]
                    else: 
                        text = row["project_title"]
                    row_id = row["id"]
                    print(f"{row_id}")
                    # Gets the response
                    response = process_text(name, text)
                    # Rows that timed out or errored come back as None,
                    # which is written as NULL and will be retried next run.

                    # Insert the response to the updated values list for insertion later.
                    updated_values.append((response, gard_id, row_id))
                    print(f"{response} inserted for {row_id}\n")

                except Exception as exc: 
                    print(f"Could not finish inserting responses: {exc}\n")
            if updated_values: 
                # Perform the update sql to insert the new values.
                write_cursor.executemany(update_sql, updated_values)
                conn.commit()
                total_updated += len(updated_values)
                print(f"Updated {len(updated_values)} rows. Total: {total_updated}\n")
    except Exception as exc:
        print(f"Could not finish executing the process: {exc}\n") 

    finally:
        print(f"Total rows updated: {total_updated}\n")
        if read_cursor is not None: 
            read_cursor.close()
        if write_cursor is not None: 
            write_cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()



def main(): 
    insert_response()

if __name__ == "__main__":
    raise SystemExit(main())

"""
    Takes the matched terms in the rdas terms table and inserts to the rdas target table. 
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from baseclass.conn import DBConnection as db

# Tables
TARGET_TABLE = "grant_gard_project_process_relations_sip"
TERMS_TABLE = "grant_gard_terms_deduped_sip" 
NEW_COLUMN = "matched_term"
BATCH_SIZE = 2000


# Helper method to add the matched_term column to the target table if it doesn't already exist.
def ensure_column_exists(conn):
    cursor = conn.cursor()

    # Check that the table has the matched_term column
    check_sql = """
        SELECT COUNT(*) AS cnt
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = %s
            AND COLUMN_NAME = %s
    """
    cursor.execute(check_sql, (TARGET_TABLE, NEW_COLUMN))
    exists = cursor.fetchone()[0] > 0

    # Create the new matched_term column if it doesn't exist yet. 
    if not exists:
        alter_sql = f"""
            ALTER TABLE `{TARGET_TABLE}`
            ADD COLUMN `{NEW_COLUMN}` VARCHAR(255) DEFAULT NULL
        """
        cursor.execute(alter_sql)
        conn.commit()
        print(f"Added column `{NEW_COLUMN}` to `{TARGET_TABLE}`.\n")
    else:
        print(f"Column `{NEW_COLUMN}` already exists on `{TARGET_TABLE}`.\n")

    cursor.close()


# Helper method to make sure the gard_id index exist on both tables. If not, create the index.
def ensure_gard_id_indexes(conn):
    cursor = conn.cursor()

    for table, index_name in (
        (TARGET_TABLE, "idx_gard_id_target"),
        (TERMS_TABLE, "idx_gard_id_terms"),
    ):
        check_sql = """
            SELECT COUNT(*) AS cnt
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = %s
                AND COLUMN_NAME = 'gard_id'
        """
        cursor.execute(check_sql, (table,))
        exists = cursor.fetchone()[0] > 0

        if not exists:
            cursor.execute(f"CREATE INDEX `{index_name}` ON `{table}` (gard_id)")
            conn.commit()
            print(f"Created index `{index_name}` on `{table}`(gard_id).\n")
        else:
            print(f"An index on `{table}`(gard_id) already exists -- skipping.\n")

    cursor.close()


# Helper method to get the target table's column names (excluding the primary key `id and the 
# new matched_term column), so we can clone rows generically regardless of the exact schema.
def get_clonable_columns(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (TARGET_TABLE,))

    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()

    excluded = {"id", NEW_COLUMN}
    return [c for c in columns if c not in excluded]


# Helper method that builds a dict of gard_id -> list of matched terms, pulled from the terms table. 
def get_terms_by_gard_id(conn):
    cursor = conn.cursor(buffered=True, dictionary=True)

    cursor.execute(f"""
        SELECT gard_id, matched_term
        FROM `{TERMS_TABLE}`
        ORDER BY gard_id, id
    """)

    terms_by_gard_id = {}
    for row in cursor.fetchall():
        gard_id = row["gard_id"]
        term = row["matched_term"]
        terms_by_gard_id.setdefault(gard_id, []).append(term)

    cursor.close()
    return terms_by_gard_id


# Helper method to get the distinct set of gard_ids that currently exist in the 
# target table. Used to confirm a gard_id from the terms table 
# actually has a row to act on before doing anything with it.
def get_existing_gard_ids(conn):
    cursor = conn.cursor()

    cursor.execute(f"SELECT DISTINCT gard_id FROM `{TARGET_TABLE}`")
    gard_ids = {row[0] for row in cursor.fetchall()}

    cursor.close()
    return gard_ids



# Helper method to insert cloned rows in a single batched statement and joins it against the source table once.
def insert_clones_batch(write_cursor, clonable_columns, clone_values):
    if not clone_values:
        return 0

    columns_sql = ", ".join(f"`{c}`" for c in clonable_columns)
    source_cols_sql = ", ".join(f"t.`{c}`" for c in clonable_columns)

    # Build "SELECT %s AS id, %s AS term" unioned for each pair.
    union_parts = ["SELECT %s AS id, %s AS term" for _ in clone_values]
    union_sql = " UNION ALL ".join(union_parts)

    # clone_values is (term, row_id) -- flip to (row_id, term) to match
    # the "id, term" column order in the UNION ALL above.
    flat_params = []
    for term, row_id in clone_values:
        flat_params.extend([row_id, term])

    insert_sql = f"""
        INSERT INTO `{TARGET_TABLE}` ({columns_sql}, `{NEW_COLUMN}`)
        SELECT {source_cols_sql}, v.term
        FROM `{TARGET_TABLE}` t
        JOIN ({union_sql}) AS v
          ON t.id = v.id
    """

    write_cursor.execute(insert_sql, flat_params)
    return write_cursor.rowcount

# For each row in the target table:
#   - Set matched_term on the original row to the 1st matching term.
#   - Insert cloned rows (copying all other columns) for the remaining terms (2nd, 3rd, ...)
# Rows whose gard_id has no matches are left untouched (matched_term stays NULL).
def expand_rows():
    conn = db().mysql_conn()

    if conn is None:
        raise ConnectionError("Unable to connect to MySQL.")

    ensure_column_exists(conn)
    ensure_gard_id_indexes(conn)
    clonable_columns = get_clonable_columns(conn)
    terms_by_gard_id = get_terms_by_gard_id(conn)
    existing_gard_ids = get_existing_gard_ids(conn)

    # Explicitly identify + report gard_ids from the terms table that have
    # no corresponding rows in the target table. These are skipped entirely --
    # no updates, no inserts, no new gard_id rows created.
    unmatched_gard_ids = set(terms_by_gard_id.keys()) - existing_gard_ids
    if unmatched_gard_ids:
        print(
            f"Skipping {len(unmatched_gard_ids)} gard_id(s) present in "
            f"`{TERMS_TABLE}` but not found in `{TARGET_TABLE}` "
            f"(no changes made for these):"
        )
        for gid in sorted(unmatched_gard_ids):
            print(f"  - {gid}")
        print()

    read_cursor = conn.cursor(buffered=True, dictionary=True)
    write_cursor = conn.cursor()

    select_sql = f"""
        SELECT id, gard_id
        FROM `{TARGET_TABLE}`
        WHERE `{NEW_COLUMN}` IS NULL
    """
    read_cursor.execute(select_sql)

    update_first_term_sql = f"""
        UPDATE `{TARGET_TABLE}`
        SET `{NEW_COLUMN}` = %s
        WHERE id = %s
    """

    batch_num = 0
    total_updated = 0
    total_cloned = 0

    try:
        while True:
            rows = read_cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break

            batch_num += 1
            print(f"Batch {batch_num}...", end="\n", flush=True)

            update_values = []
            clone_values = []

            for row in rows:
                row_id = row["id"]
                gard_id = row["gard_id"]

                # only act on gard_ids that exist in target table.
                if gard_id not in existing_gard_ids:
                    continue

                terms = terms_by_gard_id.get(gard_id)

                if not terms:
                    # No matched terms for this gard_id; leave the row alone.
                    continue

                first_term, remaining_terms = terms[0], terms[1:]

                # Original row gets the first term.
                update_values.append((first_term, row_id))
                print(f"{row_id}: {first_term}")

                # Remaining terms become cloned rows.
                for term in remaining_terms:
                    clone_values.append((term, row_id))

            if update_values:
                write_cursor.executemany(update_first_term_sql, update_values)
                total_updated += len(update_values)

            if clone_values:
                cloned_count = insert_clones_batch(write_cursor, clonable_columns, clone_values)
                total_cloned += cloned_count

            conn.commit()
            print(f"Updated {len(update_values)} rows, cloned {len(clone_values)} rows this batch.\n")

    except Exception as exc:
        print(f"Could not finish executing the process: {exc}\n")

    finally:
        print(f"Total rows updated (first term): {total_updated}")
        print(f"Total rows cloned (remaining terms): {total_cloned}\n")
        if read_cursor is not None:
            read_cursor.close()
        if write_cursor is not None:
            write_cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()


def main():
    expand_rows()


if __name__ == "__main__":
    raise SystemExit(main())
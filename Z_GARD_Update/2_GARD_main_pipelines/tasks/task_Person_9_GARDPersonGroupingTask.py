"""Group pending people once after all GARD source iterations complete.

Invocation: _run_person_grouping_and_graph_updates() calls grouping before Agent
creation. Pending person flags accumulated during per-node extraction provide
the final worklist even though trial/publication source flags are already clear.

Example rows awaiting a group ID:
    people = [
        {"id": 41, "last_name": "O'Neil", "is_new": 1, "rdas_group_id": None},
        {"id": 42, "last_name": "Li", "is_new": 1, "rdas_group_id": None},
        {"id": 43, "last_name": "Éclair", "is_new": 1, "rdas_group_id": None},
        {"id": 44, "last_name": "王", "is_new": 1, "rdas_group_id": None},
    ]

Read actual prefixes such as "O'", "Li", "Éc", and "王" from pending rows, in pages
of 100. This covers punctuation, Unicode, and one-character surnames without an
assumed ASCII alphabet. The inherited grouping logic compares all people with
each affected surname and preserves existing group IDs when it finds a match.
Its CPU work stays in the existing process pool; only the parent writes MySQL.
The examples illustrate prefix coverage, not guaranteed grouping outcomes."""

from pipelines.pipeline_6_person.task_person_4_grouping import NewPersonGroupingTask


class GARDPersonGroupingTask(NewPersonGroupingTask):
    """Reuse the shared process pool while discovering prefixes from pending people."""

    __module__ = NewPersonGroupingTask.__module__
    PREFIX_BATCH_SIZE = 100

    def _iter_last_name_prefixes(self):

        '''
        The final backlog can contain names outside the shared ASCII prefix
        lists, including one-character surnames. Read actual pending prefixes
        in bounded pages so those people are also grouped before their flags
        are cleared. The inherited task still bounds its process-pool queue,
        reuses existing group IDs, and applies database writes in the parent.
        '''
        query = f'''
            SELECT DISTINCT LEFT(last_name, 2) AS prefix
            FROM {self.PERSON_TABLE}
            WHERE is_new = 1
              AND (rdas_group_id IS NULL OR rdas_group_id = '')
              AND last_name IS NOT NULL AND last_name <> ''
              AND (%s IS NULL OR LEFT(last_name, 2) > %s)
            ORDER BY prefix
            LIMIT %s
        '''
        cursor = self.mysql.cursor(dictionary=True, buffered=True)
        last_prefix = None
        try:
            while True:
                cursor.execute(query, (last_prefix, last_prefix, self.PREFIX_BATCH_SIZE))
                rows = cursor.fetchall()
                if not rows:
                    break

                last_prefix = rows[-1]["prefix"]
                for row in rows:
                    yield row["prefix"]
        finally:
            cursor.close()

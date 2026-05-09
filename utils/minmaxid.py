

# Add the project root to the Python path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from baseclass.conn import DBConnection as db

class MinMaxIdLoader:

    def __init__(self):
        pass

    def _fetch_min_max_ids(self, query):
        """Helper method to execute a query and return min/max IDs."""
        conn = None
        cursor = None
        try:
            conn = db().mysql_conn()
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchone()
        except Exception as e:
            print(f"Database error: {e}")
        finally:
            # Safely close cursor and connection
            if cursor:
                try:
                    cursor.close()
                except Exception as e:
                    print(f"Error closing cursor: {e}")
            if conn and conn.is_connected():
                try:
                    conn.close()
                except Exception as e:
                    print(f"Error closing connection: {e}")
        return None, None



    def get_min_max_ids(self, table_name):
        """Get the minimum and maximum ID values from a table."""

        query = f"SELECT MIN(id), MAX(id) FROM {table_name}"
        return self._fetch_min_max_ids(query)



    def get_min_max_ids_by_flag(self, table_name, flag):
        """Get min/max IDs from rows where processed != flag or processed IS NULL."""

        query = (
            f"SELECT MIN(id), MAX(id) FROM {table_name} "
            f"WHERE processed != '{flag}' OR processed IS NULL"
        )
        return self._fetch_min_max_ids(query)

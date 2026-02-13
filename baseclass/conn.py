import os
import mysql.connector
from gqlalchemy import Memgraph
from dotenv import load_dotenv
load_dotenv()

class DBConnection:

    def __init__(self):
        pass
        

    def memgraph_conn(self):
        try:
            return Memgraph(
                host=os.getenv("MEMGRAPH_HOST"),
                port=int(os.getenv("MEMGRAPH_PORT")),
                username=os.getenv("MEMGRAPH_USERNAME"),
                password=os.getenv("MEMGRAPH_PASSWORD"),
                encrypted=True
            ) 
        except ValueError as ve:
            print(f"ValueError during port conversion: {ve}. Ensure MEMGRAPH_PORT is a valid integer.")
            raise  # Re-raise to prevent silent failures
        except ConnectionRefusedError as cre:
            print(f"Connection refused: {cre}. Check if Memgraph is running on {os.getenv('MEMGRAPH_HOST')}:{os.getenv('MEMGRAPH_PORT')}.")
            raise
        except OSError as ose:
            print(f"OS Error: {ose}. This might indicate network issues or an incorrect host.")
            raise
        except Exception as e:
            print(f"An unexpected error occurred during Memgraph connection: {e}")
            raise


    def mysql_conn(self):

        try:
            return mysql.connector.connect(
                host=os.environ.get("MYSQL_HOST"), 
                user=os.environ.get("MYSQL_USER"),
                password=os.environ.get("MYSQL_PASSWORD"),
                database=os.environ.get("MYSQL_DATABASE"),
                collation="utf8mb4_unicode_ci",  # Choose compatible collation
                charset="utf8mb4" # Add this to your connection string
            )
        except mysql.connector.Error as err:
            print(f'MySQL connection error: {err}')

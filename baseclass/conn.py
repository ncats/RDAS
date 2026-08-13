import os
import mysql.connector
from dotenv import load_dotenv
load_dotenv()

class DBConnection:

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
import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv()
 
import re
import csv
from pathlib import Path 
from utils.conn import DBConnection as db
from utils.tools import ask_to_continue, detect_file_encoding, _normalize_tuple

from datetime import datetime
from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()
 
#
# TRUNCATE TABLE grant_patent
#
# -- This deletes all rows and resets AUTO_INCREMENT to 1
# For table grant_patent, the step = 1 
   
def upload_patents(dir_path):

    if not os.path.exists(dir_path):
        print(f"Error: Directory '{dir_path}' does not exist")
        return None    
    # Print directory contents
    #print(f"Contents of '{dir_path}': {os.listdir(dir_path)}") 

    conn = db().mysql_conn()
    cursor = conn.cursor()

    insert ='INSERT INTO grant_patent (PATENT_ID, PROJECT_ID, PATENT_TITLE, PATENT_ORG_NAME) VALUES (%s, %s, %s, %s)'

    # Get all CSV files (case-insensitive)
    csv_files = Path(dir_path).glob('*.[Cc][Ss][Vv]')
   
    for csv_file in csv_files:

        filename = csv_file.name         
        print(f'\n{filename}')

        total = 0
        row_touples_list = [] 

        encoding, confidence = detect_file_encoding(dir_path+'/'+filename)
        print(f"Detected encoding: {encoding} (Confidence: {confidence:.2%})")
        
        with open(csv_file, 'r', newline='', encoding=encoding, errors='replace') as file:

            reader = csv.DictReader(file)  

            for row in list(reader):
                #"","","",""
                data_tuple = (  
                    row['PATENT_ID'] if row['PATENT_ID'] else None,  # PATENT_ID
                    row['PROJECT_ID'] if row['PROJECT_ID'] else None,  # PROJECT_ID
                    row['PATENT_TITLE'] if row['PATENT_TITLE'] else None,  # PATENT_TITLE
                    row['PATENT_ORG_NAME'] if row['PATENT_ORG_NAME'] else None  # PATENT_ORG_NAME
                )   

                # remove unwanted characters
                data_tuple = _normalize_tuple(data_tuple)

                row_touples_list.append(data_tuple)
                total += 1 

                if total % 50 == 0:
                    # Save rows of a csv file into mysql                   
                    cursor.executemany(insert, row_touples_list)   
                    row_touples_list = []
              
                if total % 1000 == 0:
                    conn.commit()
                    print('.', end= ' ', flush=True)


        # Upload the leftover
        cursor.executemany(insert, row_touples_list)
        conn.commit()

        print(f'\n{csv_file.name}:: total = {total}\n')
   
    if cursor:
        cursor.close()

    if conn:
        conn.close 
 


if __name__ == '__main__':

    ok = ask_to_continue(f'*** Upload the grant Patents into MySQL database? *** ')

    if not ok:
        sys.exit(Fore.RED + '\n------------------------ Stopped ------------------------\n'+ Style.RESET_ALL)

    dir = f'{Path(__file__).parent}/data/patents'

    # 1. 
    #convert_csv_files_to_utf8(dir)
    """ 
    If convert_csv_files_to_utf8(dir) doesn't work, manually save as: CSV UTF-8 (Comma delimited)(.csv) 
    """

    # 2.
    #check_column_max_length(dir, ["PATENT_ID","PATENT_TITLE","PROJECT_ID","PATENT_ORG_NAME"])   

    # 3.
    upload_patents(dir)
 
    print(Fore.BLUE + f'\n=**=**=**=**=**=**=**=**=**=**=**=**=**=**=**= All Done  =**=**=**=**=**=**=**=**=**=**=**=**=**=**=**=\n'+ Style.RESET_ALL)
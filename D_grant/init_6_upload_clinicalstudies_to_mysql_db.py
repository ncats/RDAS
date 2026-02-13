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
# TRUNCATE TABLE grant_clinical_study
#
# -- This deletes all rows and resets AUTO_INCREMENT to 1
# For table grant_clinical_study, the step = 1 
   
def upload_clinical_studies(dir_path):

    if not os.path.exists(dir_path):
        print(f"Error: Directory '{dir_path}' does not exist")
        return None    
    # Print directory contents
    #print(f"Contents of '{dir_path}': {os.listdir(dir_path)}") 

    conn = db().mysql_conn()
    cursor = conn.cursor()

    insert ='INSERT INTO grant_clinical_study (core_project_num, nctid, study, study_status) VALUES (%s, %s, %s, %s)'

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
                data_tuple = (  
                    row['Core Project Number'] if row['Core Project Number'] else None,  # Core Project Number -- core_project_num
                    row['ClinicalTrials.gov ID'] if row['ClinicalTrials.gov ID'] else None,  # ClinicalTrials.gov ID -- nctid
                    row['Study'] if row['Study'] else None,  # Study
                    row['Study Status'] if row['Study Status'] else None  # Study Status
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

    ok = ask_to_continue(f'*** Upload the grant Clinical Studies into MySQL database? *** ')

    if not ok:
        sys.exit(Fore.RED + '\n------------------------ Stopped ------------------------\n'+ Style.RESET_ALL)

    dir = f'{Path(__file__).parent}/data/clinical_studies'

    # 1.
    #convert_csv_files_to_utf8(dir)
    """ 
    If convert_csv_files_to_utf8(dir) doesn't work, manually save as: CSV UTF-8 (Comma delimited)(.csv) 
    """

    # 2.
    #check_column_max_length(dir, ["Core Project Number", "ClinicalTrials.gov ID", "Study", "Study Status"])   

    # 3.
    upload_clinical_studies(dir)
 
    print(Fore.BLUE + f'\n=**=**=**=**=**=**=**=**=**=**=**=**=**=**=**= All Done  =**=**=**=**=**=**=**=**=**=**=**=**=**=**=**=\n'+ Style.RESET_ALL)
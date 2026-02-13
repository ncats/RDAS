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
# TRUNCATE TABLE grant_linktable
#
# -- This deletes all rows and resets AUTO_INCREMENT to 1 
# For table grant_linktable, the step = 1 
 

def get_year(filename):

    pattern = r'[A-Za-z0-9_]*?(\d{4}).csv'
    match = re.match(pattern, filename)

    if match:
        year = int(match.group(1))  # Returns the year (4 digits)

        if year > 2025 or year < 1980:
            raise ValueError(f"The Year cannot less than 1980 or greater than 2025")        
        return year 
        
    raise ValueError(f"Filename '{filename}' does not match the expected pattern {pattern}")
    
 

def upload_linktables(dir_path):

    if not os.path.exists(dir_path):
        print(f"Error: Directory '{dir_path}' does not exist")
        return None    
    # Print directory contents
    #print(f"Contents of '{dir_path}': {os.listdir(dir_path)}") 

    conn = db().mysql_conn()
    cursor = conn.cursor()
    
    insert ='INSERT INTO grant_linktable (YEAR, PMID, PROJECT_NUMBER) VALUES (%s, %s, %s)' 

    # Get all CSV files (case-insensitive)
    csv_files = Path(dir_path).glob('*.[Cc][Ss][Vv]')
   
    for csv_file in csv_files:

        filename = csv_file.name        
        year = get_year(filename)
        print(f'\n[Year: {year}]: {filename}')

        total = 0
        row_touples_list = [] 

        encoding, confidence = detect_file_encoding(dir_path+'/'+filename)
        print(f"Detected encoding: {encoding} (Confidence: {confidence:.2%})")
        
        with open(csv_file, 'r', newline='', encoding=encoding, errors='replace') as file:

            reader = csv.DictReader(file)  

            for row in list(reader):
                data_tuple = ( 
                    year,
                    row['PMID'] if row['PMID'] else None,  # PMID 
                    row['PROJECT_NUMBER'] if row['PROJECT_NUMBER'] else None  # PROJECT_NUMBER
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

    ok = ask_to_continue(f'*** Upload the grant Linktables into MySQL database? *** ')

    if not ok:
        sys.exit(Fore.RED + '\n------------------------ Stopped ------------------------\n'+ Style.RESET_ALL)


    dir = f'{Path(__file__).parent}/data/linktables'

    # 1.
    #convert_csv_files_to_utf8(dir)
    """ 
    If convert_csv_files_to_utf8(dir) doesn't work, manually save as: CSV UTF-8 (Comma delimited)(.csv) 
    """

    # 2.
    #check_column_max_length(dir, ['APPLICATION_ID','ABSTRACT_TEXT'])   

    # 3.
    upload_linktables(dir) 
 
    print(Fore.BLUE + f'\n=**=**=**=**=**=**=**=**=**=**=**=**=**=**=**= All Done  =**=**=**=**=**=**=**=**=**=**=**=**=**=**=**=\n'+ Style.RESET_ALL)
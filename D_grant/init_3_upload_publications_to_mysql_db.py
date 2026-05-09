import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv()
 
import csv
from pathlib import Path 
from baseclass.conn import DBConnection as db
from utils.tools import ask_to_continue, detect_file_encoding,  _normalize_tuple

from datetime import datetime
from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()
 
#
# TRUNCATE TABLE grant_publication
#
# -- This deletes all rows and resets AUTO_INCREMENT to 1
#For table grant_publication, the step = 1 
 
  
def upload_publications(dir_path):

    if not os.path.exists(dir_path):
        print(f"Error: Directory '{dir_path}' does not exist")
        return None    
    # Print directory contents
    #print(f"Contents of '{dir_path}': {os.listdir(dir_path)}")

    insert ='''
        INSERT INTO grant_publication (
            AFFILIATION,	AUTHOR_LIST,	COUNTRY,	ISSN,	JOURNAL_ISSUE,	
            JOURNAL_TITLE,	JOURNAL_TITLE_ABBR,	JOURNAL_VOLUME,	LANG, PAGE_NUMBER,	
            PMC_ID,	PMID,	PUB_DATE,	PUB_TITLE,	PUB_YEAR
        )
        VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s)
    '''

    conn = db().mysql_conn()
    cursor = conn.cursor()

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
               
                ''' ValueError: invalid literal for int() with base 10: '6587139.2' '''
                #int(row['PMC_ID']) if row['PMC_ID'] else None,  # PMC_ID

                #Convert PMC_ID to integer (truncate decimal if present)
                pmc_id = int(float(row['PMC_ID'])) if row['PMC_ID'] else None

                data_tuple = ( 
                    row['AFFILIATION'] if row['AFFILIATION'] else None,  # AFFILIATION 
                    row['AUTHOR_LIST'] if row['AUTHOR_LIST'] else None,  # AUTHOR_LIST
                    row['COUNTRY'] if row['COUNTRY'] else None,  # COUNTRY
                    row['ISSN'] if row['ISSN'] else None,  # ISSN
                    row['JOURNAL_ISSUE'] if row['JOURNAL_ISSUE'] else None,  # JOURNAL_ISSUE                    
                    row['JOURNAL_TITLE'] if row['JOURNAL_TITLE'] else None,  # JOURNAL_TITLE
                    row['JOURNAL_TITLE_ABBR'] if row['JOURNAL_TITLE_ABBR'] else None,  # JOURNAL_TITLE_ABBR
                    row['JOURNAL_VOLUME'] if row['JOURNAL_VOLUME'] else None,  # JOURNAL_VOLUME 
                    row['LANG'] if row['LANG'] else None,  # LANG
                    row['PAGE_NUMBER'] if row['PAGE_NUMBER'] else None,  # PAGE_NUMBER

                    pmc_id,
                    #int(row['PMC_ID']) if row['PMC_ID'] else None,  # PMC_ID

                    int(row['PMID']) if row['PMID'] else None,  # PMID
                    row['PUB_DATE'] if row['PUB_DATE'] else None,  # PUB_DATE
                    row['PUB_TITLE'] if row['PUB_TITLE'] else None,  # PUB_TITLE
                    int(row['PUB_YEAR']) if row['PUB_YEAR'] else None,  # PUB_YEAR 
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

    ok = ask_to_continue(f'*** Upload the grant Publications into MySQL database? *** ')

    if not ok:
        sys.exit(Fore.RED + '\n------------------------ Stopped ------------------------\n'+ Style.RESET_ALL)


    dir = f'{Path(__file__).parent}/data/publications'

    # 1.
    #convert_csv_files_to_utf8(dir)
    """ 
    If convert_csv_files_to_utf8(dir) doesn't work, manually save as: CSV UTF-8 (Comma delimited)(.csv) 
    """

    # 2.
    #check_column_max_length(dir, ['AFFILIATION','AUTHOR_LIST','JOURNAL_TITLE','JOURNAL_ISSUE','JOURNAL_TITLE_ABBR','PUB_TITLE','PAGE_NUMBER','PUB_DATE','JOURNAL_VOLUME'])   

    # 3.
    upload_publications(dir) 

    
    print(Fore.BLUE + f'\n=**=**=**=**=**=**=**=**=**=**=**=**=**=**=**= All Done  =**=**=**=**=**=**=**=**=**=**=**=**=**=**=**=\n'+ Style.RESET_ALL)
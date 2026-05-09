import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv()

import re
import csv
from pathlib import Path
from utils.tools import parse_date
from utils.tools import ask_to_continue, detect_file_encoding, _normalize_tuple
from baseclass.conn import DBConnection as db

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()
 
#
# TRUNCATE TABLE grant_project
#
# -- This deletes all rows and resets AUTO_INCREMENT to 1 
#For table grant_project, the step = 1 

def convert_value(value, converter=None):
    """Convert value using converter function, return None if empty"""
    if not value:
        return None
    return converter(value) if converter else value

def upload_projects(dir_path):

    if not os.path.exists(dir_path):
        print(f"Error: Directory '{dir_path}' does not exist")
        return None    
    # Print directory contents
    #print(f"Contents of '{dir_path}': {os.listdir(dir_path)}")

    conn = db().mysql_conn()
    cursor = conn.cursor()

    # Get all CSV files (case-insensitive)
    csv_files = Path(dir_path).glob('RePORTER_PRJ_C_FY*.[Cc][Ss][Vv]')
   
    for csv_file in csv_files:

        filename = csv_file.name
        print(f'\n{filename}')
 
        try:
            year = _get_year(filename)
        except ValueError as e:
            print(e)
              
        total = 0
        row_touples_list = []
        is_year_after_2005 =  year > 2005
        print(f'[Year = {year}]: is_year_after_2005 = {is_year_after_2005}')

        encoding, confidence = detect_file_encoding(dir_path+'/'+filename)
        print(f"Detected encoding: {encoding} (Confidence: {confidence:.2%})")

        with open(csv_file, 'r', newline='', encoding=encoding, errors='replace') as file:

            reader = csv.DictReader(file)  

            for row in list(reader):
                '''
                data_tuple = (
                    int(row['APPLICATION_ID']) if row['APPLICATION_ID'] else None,  # APPLICATION_ID
                    row['ACTIVITY'] if row['ACTIVITY'] else None,  # ACTIVITY
                    row['ADMINISTERING_IC'] if row['ADMINISTERING_IC'] else None,  # ADMINISTERING_IC
                    int(row['APPLICATION_TYPE']) if row['APPLICATION_TYPE'] else None,  # APPLICATION_TYPE
                    row['ARRA_FUNDED'] if row['ARRA_FUNDED'] else None,  # ARRA_FUNDED
                    parse_date(row['AWARD_NOTICE_DATE']),  # AWARD_NOTICE_DATE
                    parse_date(row['BUDGET_START']),  # BUDGET_START
                    parse_date(row['BUDGET_END']),  # BUDGET_END
                    row['CFDA_CODE'] if row['CFDA_CODE'] else None,  # CFDA_CODE
                    row['CORE_PROJECT_NUM'] if row['CORE_PROJECT_NUM'] else None,  # CORE_PROJECT_NUM
                    row['ED_INST_TYPE'] if row['ED_INST_TYPE'] else None,  # ED_INST_TYPE
                    
                    row['FULL_PROJECT_NUM'] if row['FULL_PROJECT_NUM'] else None,  # FULL_PROJECT_NUM
                    row['SUBPROJECT_ID'] if row['SUBPROJECT_ID'] else None,  # SUBPROJECT_ID
                    row['FUNDING_ICs'] if row['FUNDING_ICs'] else None,  # FUNDING_ICs
                    int(row['FY']) if row['FY'] else None,  # FY
                    row['IC_NAME'] if row['IC_NAME'] else None,  # IC_NAME
                    row['NIH_SPENDING_CATS'] if row['NIH_SPENDING_CATS'] else None,  # NIH_SPENDING_CATS
                    row['ORG_CITY'] if row['ORG_CITY'] else None,  # ORG_CITY
                    row['ORG_COUNTRY'] if row['ORG_COUNTRY'] else None,  # ORG_COUNTRY
                    row['ORG_DEPT'] if row['ORG_DEPT'] else None,  # ORG_DEPT
                    row['ORG_DISTRICT'] if row['ORG_DISTRICT'] else None,  # ORG_DISTRICT
                    row['ORG_DUNS'] if row['ORG_DUNS'] else None,  # ORG_DUNS
                    row['ORG_FIPS'] if row['ORG_FIPS'] else None,  # ORG_FIPS
                    row['ORG_NAME'] if row['ORG_NAME'] else None,  # ORG_NAME
                    row['ORG_STATE'] if row['ORG_STATE'] else None,  # ORG_STATE
                    row['ORG_ZIPCODE'] if row['ORG_ZIPCODE'] else None,  # ORG_ZIPCODE
                    row['PHR'] if row['PHR'] else None,  # PHR
                    row['PI_IDS'] if row['PI_IDS'] else None,  # PI_IDS
                    row['PI_NAMEs'] if row['PI_NAMEs'] else None,  # PI_NAMEs
                    row['PROGRAM_OFFICER_NAME'] if row['PROGRAM_OFFICER_NAME'] else None,  # PROGRAM_OFFICER_NAME
                    parse_date(row['PROJECT_START']),  # PROJECT_START
                    parse_date(row['PROJECT_END']),  # PROJECT_END
                    row['PROJECT_TERMS'] if row['PROJECT_TERMS'] else None,  # PROJECT_TERMS
                    row['PROJECT_TITLE'] if row['PROJECT_TITLE'] else None,  # PROJECT_TITLE
                    row['SERIAL_NUMBER'] if row['SERIAL_NUMBER'] else None,  # SERIAL_NUMBER
                    row['STUDY_SECTION'] if row['STUDY_SECTION'] else None,  # STUDY_SECTION
                    row['STUDY_SECTION_NAME'] if row['STUDY_SECTION_NAME'] else None,  # STUDY_SECTION_NAME
                    
                    row['SUFFIX'] if row['SUFFIX'] else None,  # SUFFIX
                    int(row['SUPPORT_YEAR']) if row['SUPPORT_YEAR'] else None,  # SUPPORT_YEAR
                    int(row['TOTAL_COST']) if row['TOTAL_COST'] else None,  # TOTAL_COST
                    int(row['TOTAL_COST_SUB_PROJECT']) if row['TOTAL_COST_SUB_PROJECT'] else None,  # TOTAL_COST_SUB_PROJECT 
                )
                '''
                # Define field configurations: (field_name, converter_function)
                fields = [
                    ('APPLICATION_ID', int),
                    ('ACTIVITY', None),
                    ('ADMINISTERING_IC', None),
                    ('APPLICATION_TYPE', int),
                    ('ARRA_FUNDED', None),
                    ('AWARD_NOTICE_DATE', parse_date),
                    ('BUDGET_START', parse_date),
                    ('BUDGET_END', parse_date),
                    ('CFDA_CODE', None),
                    ('CORE_PROJECT_NUM', None),
                    ('ED_INST_TYPE', None),
                    ('FULL_PROJECT_NUM', None),
                    ('SUBPROJECT_ID', None),
                    ('FUNDING_ICs', None),
                    ('FY', int),
                    ('IC_NAME', None),
                    ('NIH_SPENDING_CATS', None),
                    ('ORG_CITY', None),
                    ('ORG_COUNTRY', None),
                    ('ORG_DEPT', None),
                    ('ORG_DISTRICT', None),
                    ('ORG_DUNS', None),
                    ('ORG_FIPS', None),
                    ('ORG_NAME', None),
                    ('ORG_STATE', None),
                    ('ORG_ZIPCODE', None),
                    ('PHR', None),
                    ('PI_IDS', None),
                    ('PI_NAMEs', None),
                    ('PROGRAM_OFFICER_NAME', None),
                    ('PROJECT_START', parse_date),
                    ('PROJECT_END', parse_date),
                    ('PROJECT_TERMS', None),
                    ('PROJECT_TITLE', None),
                    ('SERIAL_NUMBER', None),
                    ('STUDY_SECTION', None),
                    ('STUDY_SECTION_NAME', None),
                    ('SUFFIX', None),
                    ('SUPPORT_YEAR', int),
                    ('TOTAL_COST', int),
                    ('TOTAL_COST_SUB_PROJECT', int),
                ]

                data_tuple = tuple(convert_value(row[field], converter) for field, converter in fields)


                if is_year_after_2005:
                     # Files after 2005, starts with 2006
                    data_tuple = data_tuple + (
                        row['OPPORTUNITY NUMBER'] if row['OPPORTUNITY NUMBER'] else None,  # OPPORTUNITY NUMBER
                        row['FUNDING_MECHANISM'] if row['FUNDING_MECHANISM'] else None,  # FUNDING_MECHANISM
                        int(row['ORG_IPF_CODE']) if row['ORG_IPF_CODE'] else None,  # ORG_IPF_CODE
                        int(row['DIRECT_COST_AMT']) if row['DIRECT_COST_AMT'] else None,  # DIRECT_COST_AMT
                        int(row['INDIRECT_COST_AMT']) if row['INDIRECT_COST_AMT'] else None  # INDIRECT_COST_AMT
                    )  
                else: 
                    # The files before 2006 has a column 'FOA_NUMBER'
                    data_tuple = data_tuple + (row['FOA_NUMBER'] if row['FOA_NUMBER'] else None,) 

                # remove unwanted characters
                data_tuple = _normalize_tuple(data_tuple)
                   
                row_touples_list.append(data_tuple)
                total += 1 

                if total % 50 == 0:
                    # Save rows of a csv file into mysql                   
                    cursor.executemany(_insert_sql(is_year_after_2005), row_touples_list)   
                    row_touples_list = []
              
                if total % 1000 == 0:
                    conn.commit()
                    print('.', end= ' ', flush=True)


        # Upload the leftover
        cursor.executemany(_insert_sql(is_year_after_2005), row_touples_list)
        conn.commit()

        print(f'\n{csv_file.name}:: total = {total}\n')
   
    if cursor:
        cursor.close()

    if conn:
        conn.close



def _get_year(filename):

    pattern = r'RePORTER_PRJ_C_FY(\d{4})\.[Cc][Ss][Vv]'
    match = re.match(pattern, filename)

    if match:
        year = int(match.group(1))  # Returns the year (4 digits)

        if year > 2025 or year < 1985:
            raise ValueError(f"The Year cannot less than 1985 or greater than 2025")
        
        return year 
        
    raise ValueError(f"Filename '{filename}' does not match the expected pattern 'RePORTER_PRJ_C_FY<year>.CSV'")
    


common_column_names = '''
        APPLICATION_ID,ACTIVITY,ADMINISTERING_IC,APPLICATION_TYPE,ARRA_FUNDED,
        AWARD_NOTICE_DATE,BUDGET_START,BUDGET_END,CFDA_CODE,CORE_PROJECT_NUM,
        ED_INST_TYPE,      FULL_PROJECT_NUM,SUBPROJECT_ID,FUNDING_ICs,
        FY,IC_NAME,NIH_SPENDING_CATS,ORG_CITY,ORG_COUNTRY,
        ORG_DEPT,ORG_DISTRICT,ORG_DUNS,ORG_FIPS,ORG_NAME,
        ORG_STATE,ORG_ZIPCODE,PHR,PI_IDS,PI_NAMEs,
        PROGRAM_OFFICER_NAME,PROJECT_START,PROJECT_END,PROJECT_TERMS,PROJECT_TITLE,
        SERIAL_NUMBER,STUDY_SECTION,STUDY_SECTION_NAME,SUFFIX,SUPPORT_YEAR,
        TOTAL_COST,TOTAL_COST_SUB_PROJECT
    '''

common_column_placehoders = '''
        %s, %s, %s, %s, %s, 
        %s,     %s, %s, %s, 
        %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, 
        %s, %s
'''

def _insert_sql(is_year_after_2005):
 
    # If the 1985 <= year <= 2005, Common clumns + FOA_NUMBER
    insert_sql = f'''
        INSERT INTO grant_project ( {common_column_names}, FOA_NUMBER ) VALUES ( {common_column_placehoders}, %s )
    '''

    if is_year_after_2005: 
        # Common clumns
        # Add: OPPORTUNITY_NUMBER, FUNDING_MECHANISM, ORG_IPF_CODE, DIRECT_COST_AMT, INDIRECT_COST_AMT
        insert_sql = f'''
            INSERT INTO grant_project (
                {common_column_names},
                OPPORTUNITY_NUMBER, FUNDING_MECHANISM, ORG_IPF_CODE, DIRECT_COST_AMT, INDIRECT_COST_AMT
            ) 
            VALUES ( {common_column_placehoders}, %s, %s, %s, %s, %s )
        '''

    return insert_sql

 

if __name__ == '__main__':

    ok = ask_to_continue(f'*** Upload the grant Projects into MySQL database? *** ')

    if not ok:
        sys.exit(Fore.RED + '\n------------------------ Stopped ------------------------\n'+ Style.RESET_ALL)

    base_dir = f'{Path(__file__).parent}/data'

    # 1.
    upload_projects(f'{base_dir}/projects') 

    print(Fore.BLUE + f'\n=**=**=**=**=**=**=**=**=**=**=**=**=**=**=**= All Done  =**=**=**=**=**=**=**=**=**=**=**=**=**=**=**=\n'+ Style.RESET_ALL)
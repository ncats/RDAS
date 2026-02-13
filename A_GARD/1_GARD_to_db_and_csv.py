import sys
import csv
from collections import defaultdict 

import os
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.conn import DBConnection as db
from utils.tools import _try_parse_int, _na

#GARD data file & MySQL
"""
# Action 1: save the GARD csv into database

# Action 2: Generate a intermediate GARD csv file
"""

def save_gard_data_to_database(file_path):

    mysql = db().mysql_conn()
    '''
        SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ', ')
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'rdas' AND TABLE_NAME = 'gard';
    '''
    '''
        ALTER TABLE rdas.gard AUTO_INCREMENT = 1;
    '''
    '''
        SELECT 
                GardID, MONDO_ID, 
                GROUP_CONCAT(distinct ORPHA_Code) as orpha, 
                GROUP_CONCAT(distinct Classification_Level) as classificationLevel, 
                GROUP_CONCAT(distinct Disorder_Type) as disorderType,
                MAX(CASE WHEN Label_Predicate_Type = 'Name' THEN Label END) AS `name`,
                GROUP_CONCAT(CASE WHEN Label_Predicate_Type = 'Synonym' THEN Label END SEPARATOR '$ ') AS `synonyms`,
                GROUP_CONCAT(Label_Xref SEPARATOR ',') AS `xrefs`,
                Label_Source
            FROM  rdas.gard
            WHERE 
                Label_Predicate_Mapping != 'DEPRECATED' 
                -- AND LENGTH(Label) > 3
            GROUP BY 
                GardID, MONDO_ID, Label_Source
    '''

    cursor = mysql.cursor()
    count = 0
    insert = '''
        INSERT INTO gard (
            GardID, MONDO_ID, Label, Label_Predicate_Type, Label_Predicate_Mapping, 
            Label_Predicate, Label_Xref, Label_Source, 
            ORPHA_Code, Disorder_Type, Classification_Level)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as csv_file:
                reader = csv.DictReader(csv_file)
                
                # Print fieldnames for debugging
                print("CSV fieldnames:", reader.fieldnames)
                
                # Process each row
                for row in reader:
                    try:
                        gard_id = row['GARD_ID']
                        mondo_id = row['MONDO_ID']
                        label = row['Label']
                        label_predicate_type = row['Label_Predicate_Type']
                        label_predicate_mapping = row['Label_Predicate_Mapping']
                        label_predicate = row['Label_Predicate'] 
                        xref = row['Label_Xref']
                        source = row['Label_Source']

                        orphaCode = _try_parse_int(row['ORPHA_Code'])

                        classificationLevel = _na(row['ClassificationLevel'])                        
                        disorderType = _na(row['DisorderType'])
                        
                        xref_cleaned = xref.strip('[]')
                        label_predicate_cleaned= label_predicate.strip('[]')
                        label_predicate_mapping_cleaned = label_predicate_mapping.strip('[]')

                        val = (gard_id, mondo_id, label, label_predicate_type, label_predicate_mapping_cleaned, label_predicate_cleaned, xref_cleaned, source, orphaCode, disorderType, classificationLevel)
                        cursor.execute(insert, val)
                        
                        count += 1

                        if count % 500 == 0:
                            mysql.commit()
                            print(f'inserted: {count}')

                    except KeyError as ke:
                        print(f"KeyError in row: {row}")
                        print(f"Missing key: {str(ke)}")
                        continue

    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit() 
    except Exception as e:
        print(f"Error reading file: {str(e)}") 
        sys.exit()

    mysql.commit()
    print(f'--- Total inserted: {count} ---')



#
# Generate a intermediate GARD file from the given raw GARD file, for step 2
#
def parse_and_process_gard_data(file_path):

    # Initialize the result dictionary
    gard_dict = defaultdict(lambda: {
        'GardID': '',
        'MONDO_ID': '',
        'name': '',
        'synonyms': [],
        'xrefs': [],  # Ensure this is always a set
        'source': '',
        'orphaCode': None,
        'disorderType': None,
        'classificationLevel': None
    })
    
    try:
        # Open and read the CSV file with UTF-8-SIG encoding to handle BOM
        '''
        The error occurs because the CSV file appears to have an unexpected byte order mark (BOM) at the beginning of the file, which is causing the first column header 
        be read as '\ufeffGardID' instead of 'GardID'. This is a common issue with UTF-8 encoded files that include a BOM (UTF-8-BOM).
        Let's modify the code to handle this by explicitly specifying the UTF-8-SIG encoding, which properly handles files with BOM. Here's the updated code:
        '''
        with open(file_path, 'r', encoding='utf-8-sig') as csv_file:
            reader = csv.DictReader(csv_file)
            
            # Print fieldnames for debugging
            print("CSV fieldnames:", reader.fieldnames)
            
            # Process each row
            for row in reader:
                try:
                    gard_id = row['GARD_ID'].strip()
                    mondo_id = row['MONDO_ID']
                    label = row['Label']
                    label_predicate = row['Label_Predicate_Type']
                    xref = row['Label_Xref']
                    source = row['Label_Source']

                    orphaCode = _try_parse_int(row['ORPHA_Code'])
                    classificationLevel = _na(row['ClassificationLevel'])                        
                    disorderType = _na(row['DisorderType'])

                    predicate_mapping = row['Label_Predicate_Mapping']
                    notDeprecated = (predicate_mapping != 'DEPRECATED')
 
                    # Set basic info
                    gard_dict[gard_id]['GardID'] = gard_id

                    # Use first MONDO_ID encountered (they should be unique per GardID)
                    if not gard_dict[gard_id]['MONDO_ID']:
                        gard_dict[gard_id]['MONDO_ID'] = mondo_id

                    if not gard_dict[gard_id]['source']:
                        gard_dict[gard_id]['source'] = source
                        
                    # Handle name (if Label_Predicate_Type is "Name")
                    if label_predicate == 'Name':
                        gard_dict[gard_id]['name'] = label
                        
                    # Handle synonyms (only if Label_Predicate_Type is "Synonym")
                    if label_predicate == 'Synonym' and label not in gard_dict[gard_id]['synonyms'] and notDeprecated:
                        gard_dict[gard_id]['synonyms'].append(label)
                        
                    # Handle xrefs - parse and add individual references
                    if xref:
                        # Remove square brackets and split by comma
                        xref_cleaned = xref.strip('[]')
                        # Split by comma and clean each reference 
                        xrefs = gard_dict[gard_id]['xrefs']
                        xrefs.extend(ref.strip() for ref in xref_cleaned.split(',') if ref.strip())
                        gard_dict[gard_id]['xrefs'] = list(set(xrefs)) # unique

                    gard_dict[gard_id]['Predicate_Mapping'] = predicate_mapping

                    if orphaCode:
                        gard_dict[gard_id]['orphaCode'] = orphaCode

                    if disorderType:
                        gard_dict[gard_id]['disorderType'] = disorderType

                    if classificationLevel:
                        gard_dict[gard_id]['classificationLevel'] = classificationLevel

                except KeyError as ke:
                    print(f"KeyError in row: {row}")
                    print(f"Missing key: {str(ke)}")
                    continue
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit() 
    except Exception as e:
        print(f"Error reading file: {str(e)}") 
        sys.exit()

    return  gard_dict


# Example usage
# Replace 'path_to_your_file.csv' with the actual path to your CSV file
file_path = '1_GARD/data/2025/GARD_Nomenclature_eng_2_10_2025_WithOrphanetData.csv'

# Action 1: save the GARD csv into database

from utils.tools import ask_to_continue

ok = ask_to_continue('Insert GARD into MySQL database?')
if not ok:
    sys.exit('------Stopped ------')

#save_gard_data_to_database(file_path)


# Action 2: Generate a intermediate GARD csv file
"""
result = parse_and_process_gard_data(file_path)

if result:
    # Print the result (or do whatever you need with it)
    import json
    # print all
    #print(json.dumps(result, indent=2))

    count = 0
    # filter
    for k, v in result.items(): 
        if 'orphaCode' in v:
            # Check if the value is a int
            if isinstance(v['orphaCode'], int) and v['orphaCode'] > 0:
                #print(v)
                count += 1
            #else:
            #    print("The value of 'orphaCode' is not a int:", type(v['orphaCode']))
        else:
            print("\n'orphaCode' key does not exist in the dictionary")

print(f'Total GARD nodes = {len(result)}')
print(f'Total GARD nodes with orphaCode = {count}')
""" 



import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
load_dotenv()

from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

from utils.base import BaseClass
import string
import csv
 
class PersonHelper(BaseClass):
  
    def __init__(self): 
        super().__init__('', '')

        self.all_person_last_name_and_count_file = '6_person/all_person_last_name_and_count.csv' 



    def _get_last_name_and_count_from_db(self):
        
        query = 'SELECT count(last_name) as cnt, last_name FROM rdas_db.person_of_all_sources group by last_name order by cnt desc'

        self.dict_cursor.execute(query)
        rows = self.dict_cursor.fetchall()
        print(f'Total # of last names: {len(rows)}')

        the_list = []
        for row in rows:
            the_list.append({'last_name': row['last_name'], 'count': row['cnt'], 'processed': 'no'})

        return the_list



    def _generate_last_name_and_count_file(self):

        last_name_and_count_file = self.all_person_last_name_and_count_file
        #if os.path.exists(last_name_and_count_file):
        #    os.remove(last_name_and_count_file)

        if not os.path.exists(last_name_and_count_file):
            print(f'Creating {last_name_and_count_file}')

            the_list = self._get_last_name_and_count_from_db()
            
            with open(last_name_and_count_file, 'w', newline='') as f:

                writer = csv.writer(f)    
                # Write the header row
                writer.writerow(['last_name', 'count', 'processed'])    

                # Write each data row
                for item in the_list:
                    writer.writerow([item["last_name"], item["count"], item["processed"]])

            print(f"Data successfully written to {last_name_and_count_file}")
        else:
            print(f'{last_name_and_count_file} already exists')



    def get_all_person_last_name_and_count(self):
        
        self._generate_last_name_and_count_file()

        list_of_dicts = []
        file_path = self.all_person_last_name_and_count_file 
 
        try:
            with open(file_path, mode='r') as infile:
                reader = csv.DictReader(infile) # Use DictReader to read rows as dictionaries
                for row in reader:
                    list_of_dicts.append(row)

            print(f"Successfully read data from '{file_path}'.")

        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found.")
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")

        return list_of_dicts



    def get_cluser_by_last_name(self, last_name, count, processed):

        if processed == 'yes':
            return []
        
        print(f'last_name: {last_name}, count: {count}, processed: {processed}')

        LIMIT = 100
        # Create dictionary with keys a-z, each mapping to an empty list
        name_dict = {letter: [] for letter in string.ascii_lowercase}

        query = f'SELECT * from person_of_all_sources where last_name = "{last_name}"'
        self.dict_cursor.execute(query)
        rows = self.dict_cursor.fetchall()

        cluster = [] # The list of lists
         
        if count < LIMIT:
            cluster = [list(rows)]
            
        else:
            for row in rows:              
                first_name = row['first_name'] 
 
                # Skip if first_name is None or empty
                if not first_name or not first_name.strip():
                    continue

                # Get the first letter (lowercase) and check if it's a-z
                first_letter = first_name.strip()[0].lower()

                if first_letter in name_dict: 
                    name_dict[first_letter].append(row)
    
            # Note: some lists may be empty
            cluster = [name_dict[letter] for letter in string.ascii_lowercase]
             
        return cluster



    def update_processed_flag_by_last_name(self, lastName):

        updated_rows = []
        file_path = self.all_person_last_name_and_count_file 
        try:
            with open(file_path, 'r', newline='') as infile:

                reader = csv.DictReader(infile) 
                for item in reader:
                    last_name = item['last_name']
                    count = item['count'] 
                     
                    if last_name == lastName: 
                        print(f'Pre: {item}')
                        item['processed'] = 'yes' 
                        print(f"After: {item}\n")
                    
                    updated_rows.append(item)


            # Write all (updated and unchanged) rows back to the file
            # With 'w', it will overwrite the original file
            with open(file_path, mode='w', newline='') as outfile:
                writer = csv.writer(outfile) 
                # Write the header row
                writer.writerow(['last_name', 'count', 'processed'])    

                # Write each data row
                for item in updated_rows:
                    writer.writerow([item["last_name"], item["count"], item["processed"]])

        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found.")
        except Exception as e:
            print(f"An error occurred: {e}")

     

        

if __name__ == '__main__':
    
    ph = PersonHelper()

    # 0.
    ph.get_all_person_last_name_and_count()


    # 1.
    '''
    cluseter = ph.get_cluser_by_last_name('Cross', 4030, 'no')
    print(f'Cluster.length = {len(cluseter)}')

    for lst in cluseter:
        print(f'\n------ lst.length = {len(lst)}------')
        for item in lst:
            print(item)
    '''

    # 2. update
    ph.update_processed_flag_by_last_name('Cross') #4030

    ph.update_processed_flag_by_last_name('Zhaorong') #3


    

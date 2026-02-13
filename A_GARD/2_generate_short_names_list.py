import os
import sys
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import date
from utils.tools import ask_to_continue,read_csv_as_dict
from utils.conn import DBConnection

from utils.applogger import AppLogger
logger = AppLogger().get_logger()

#Memgraph
"""
# Check some short, low-quality synonyms in the GARD, generate a exclude_words list for project owner to check.
#
"""

class GARDSynonymChecker:

    def __init__(self):

        self.memgraph = DBConnection().memgraph_conn()
        self.DATA_DIR = os.path.join(os.path.join(os.path.dirname(__file__), ''), 'data')

  

    def check_synonyms(self):

        query = '''
            MATCH (n: GARD) 
            return  n.GardId as GardId,  n.Name as GardName,  COALESCE(n.Synonyms, '') as Synonyms 
        '''
       
        results = self.memgraph.execute_and_fetch(query)

        filename = f'{self.DATA_DIR}/short-synonyms-{date.today().strftime("%Y%m%d")}.txt'      
        if os.path.exists(filename):
            os.remove(filename)

        short_synonyms_data = [] 

        # Process each result and populate the dictionary
        for disease in results:
            gard_id = disease["GardId"]
            gard_name = disease["GardName"]
            synonyms = disease["Synonyms"]

            for syn in synonyms:
                if len(syn) <= 10:
                    # Store the data as a tuple.
                    # The order is important for sorting and writing later.
                    short_synonyms_data.append((gard_id, gard_name, syn))

        # Sort the list. The key for sorting is the length of the synonym (the third element in the tuple).
        # The 'len' function is used to get the length, and the lambda function tells 'sort' to use this as the key.
        short_synonyms_data.sort(key=lambda item: len(item[2]))


        with open(filename, 'w') as file: 

            file.write(f'GARD_ID\tGARD_NAME\tSYNONYM\n')

            # Process the now-sorted data and write to the file
            for gard_id, gard_name, syn in short_synonyms_data:

                row = f'{gard_id}\t{gard_name}\t{syn}'

                print(row)
                file.write(f'{row}\n')

 
if __name__ == '__main__':

    ok = ask_to_continue('Check some short, low-quality synonyms in the GARD, generate a exclude_words list for project owner to check?')
    if not ok:
        sys.exit('------Stopped ------')

    initlzr = GARDSynonymChecker()
 

    initlzr.check_synonyms()


    print('\n------------------ Done ---------------------\n')



    

import os
import sys

_dir = os.path.dirname(__file__)
sys.path.extend([
    os.path.abspath(os.path.join(_dir, '..')),
    os.path.abspath(os.path.join(_dir, '../..'))
])
 
from colorama import init, Fore, Style
init()

import csv
import requests
from io import StringIO
from utils.file_appender import FileAppender
from baseclass.init_base import InitBase
from utils.tools import _date_string, _clean, _make_hash_key, _curr_timestamp

class ArticleIsGeneReviewUpdater(InitBase):


    def __init__(self):
        super().__init__('publication_article', 'Article')

        class_name = type(self).__name__
        self.log_file = f'{self.log_dir}/G-{class_name}-{_date_string()}.log'
        self.appender = FileAppender(self.log_file)


    def fetch_and_parse_generviews(self):
        """
        Fetch and parse the GeneReviews title file from NCBI FTP        
        Returns:
            list: List of dictionaries with keys: GR_shortname, GR_Title, NBK_id, PMID
        """
        url = "https://ftp.ncbi.nih.gov/pub/GeneReviews/GRtitle_shortname_NBKid.txt"
        
        try:
            print(f"Fetching data from {url}...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse the tab-delimited file
            content = response.text
            
            # Use csv.DictReader to parse tab-delimited data
            # The header starts with #, so we need to handle it
            reader = csv.DictReader(StringIO(content), delimiter='\t')
            
            data = []
            for row in reader:
                # Clean up the header key if it has '#'
                cleaned_row = {}
                for key, value in row.items():
                    clean_key = key.lstrip('#')
                    cleaned_row[clean_key] = value

                data.append(cleaned_row)
            
            print(f"Successfully parsed {len(data)} records")
            return data
            
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        except Exception as e:
            print(f"Error parsing data: {e}")
            return None
        

    def update(self):
        # Implementation for updating GeneReview status

        batch_cypher_update = '''

        UNWIND $pmids AS pmid
        MATCH(a:Article {pubmedId: pmid})
        SET a.isGeneReview = true     
        RETURN count(a) AS updated_count
        '''
        
        data_list = self.fetch_and_parse_generviews()

        if not data_list:
            self.appender.log_stdout(f"{Fore.RED}\n\n{'-'*50} No data to update {'-'*50}\n{Style.RESET_ALL}")
            return
        
        pmid_list = [int(data['PMID']) for data in data_list if data['PMID'] and data['PMID'].isdigit()]

        self.appender.log_stdout(f'\nTotal {Fore.BLUE}{len(pmid_list)}{Style.RESET_ALL} PMIDs retrived from https://ftp.ncbi.nih.gov/pub/GeneReviews/GRtitle_shortname_NBKid.txt\n')

        batch_size = 100
        total_updated = 0
        batches_count = (len(pmid_list) + batch_size - 1) // batch_size

        for i in range(0, len(pmid_list), batch_size):

            batch = pmid_list[i:i+batch_size]
            batch_num = i // batch_size + 1

            try:
                # ✅ Use execute_and_fetch() to properly get results
                results = self.memgraph.execute_and_fetch(batch_cypher_update, {"pmids": batch})
                
                for row in results:

                    updated_count = row['updated_count']
                    total_updated += updated_count

                    
                    self.appender.log_stdout(
                        f"Batch #{batch_num}/{batches_count}: "
                        f"Updated {updated_count} articles | "
                        f"Total so far: {total_updated}"
                    )

            except Exception as e:
                self.appender.log_stdout(f"{Fore.RED}Error in batch #{batch_num}: {e}{Style.RESET_ALL}")
                raise
            

        self.appender.log_stdout(f"\n{Fore.BLUE}{'='*30} Done! Total articles updated: {total_updated} {'='*30}{Style.RESET_ALL}\n")
        self.appender.close()

        

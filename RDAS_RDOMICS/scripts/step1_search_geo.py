"""
Step1 Search the geo keywords for counting the amount of series each gard id
"""

import os
import pandas as pd
import time
from Bio import Entrez
from urllib.error import HTTPError, URLError

Entrez.email = os.environ.get("ENTREZ_EMAIL", "")

# Function to perform eSearch query to get dataset IDs for a batch of keywords
def search_geo_datasets(keywords, max_retries=3):
    """
    Searches GEO datasets for given keywords and returns a list of unique dataset IDs.
    
    Args:
        keywords (list): List of keywords to search for.
        max_retries (int): Maximum number of retries in case of rate limits or errors.
    
    Returns:
        list: List of unique GEO series IDs.
    """
    retries = 0
    id_list = []
    # Combine keywords into a single search term
    search_term = ' OR '.join([f'("{kw}"[MeSH Terms] OR {kw}[All Fields])' for kw in keywords]) + ' AND "gse"[Filter]'
    retstart = 0
    batch_size = 100
    print("search_term: ", search_term)
    while retries < max_retries:
        try:
            while True:
                # Fetch records in batches
                with Entrez.esearch(
                    db="gds",
                    term=search_term,
                    retstart=retstart,
                    retmax=batch_size,
                    usehistory="y",
                ) as handle:
                    search_results = Entrez.read(handle)
                
                # Extract IDs and add them to the list    
                batch_id_list = search_results["IdList"]
                #query_key = search_results["QueryKey"]
                #print("query_key:", query_key)
                #web_env = search_results["WebEnv"]
                #print("web_env:", web_env)
                id_list.extend(batch_id_list)

                # Check if we have reached the end of results
                if len(batch_id_list) < batch_size:
                    break

                # Update the starting point for the next batch
                retstart += batch_size
            
            # Remove duplicates from the accumulated IDs
            unique_ids = list(set(id_list))
            print(f"Found {len(unique_ids)} unique series for keywords: {keywords}")
            return unique_ids
        except HTTPError as e:
            if e.code == 429:
                print(f"Rate limit exceeded. Retrying after delay...({retries + 1}/{max_retries})")
                time.sleep(5)
                retries += 1
            else:
                print(f"HTTPError occurred: {e}")
                break
        except URLError as e:
            print(f"URLError occurred: {e.reason}")
            retries += 1
            time.sleep(5)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

    print(f"Failed to fetch data for keywords '{keywords}' after {max_retries} attempts.")
    return []

def process_disease_file(input_file, output_file, batch_size=10):
    """
    Processes a disease list file to count GEO datasets for each disease and saves the updated file.
    
    Args:
        input_file (str): Path to the input Excel file.
        output_file (str): Path to save the updated Excel file.
        batch_size (int): Number of keywords to process per batch.
    """
    df = pd.read_excel(input_file)
    # Initialize a list to store GEO counts for each disease
    series_counts = []
    
    for disease in df['GARD_Disease']:
        all_ids = [] # Initialize a list to accumulate all dataset IDs for the current disease
        keywords = disease.split('; ') # Split the GARD_Disease into individual keywords

        # Process keywords in batches
        for i in range(0, len(keywords), batch_size):
            batch_keywords = keywords[i:i + batch_size] # Get the current batch of keywords
            ids = search_geo_datasets(batch_keywords) # Get the dataset IDs for this batch
            all_ids.extend(ids) # Accumulate the IDs
            time.sleep(1) # Adding a small delay between batch requests to reduce load

        # Remove duplicates from the accumulated IDs and count them
        unique_ids = list(set(all_ids))
        series_counts.append(len(unique_ids))
        print(f"Processed Disease: {len(unique_ids)} unique series found.")

    # Add the accumulated GEO counts to the dataframe
    df['Series_Count'] = series_counts
    df.to_excel(output_file, index=False)
    #print(f"Updated Excel file saved to:{output_file}")

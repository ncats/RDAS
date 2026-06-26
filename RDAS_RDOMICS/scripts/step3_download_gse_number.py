"""
Step3: Extracting all the label and content from web page directly and create GSE tables, instead of using the download matrix file from step2
i. extract all the information needed from matrix
ii. parse from web link
iii. refine to get the final GSE table

"""
import os
import time
import requests
import pandas as pd
from pathlib import Path
from urllib.parse import urlencode, quote
from urllib.error import HTTPError, URLError
import gzip
import shutil
from tqdm import tqdm
import ftplib
import json

# Entrez email setup for API usage
from Bio import Entrez
Entrez.email = os.environ.get("ENTREZ_EMAIL", "")

# Function to format GARD_ID by padding the numeric part with leading zeros to 7 digits
def format_gard_id(gard_id):
    prefix, numeric = gard_id.split(":")
    padded_numeric = numeric.zfill(7) # Pad numeric part to 7 digits
    return f"{prefix}:{padded_numeric}"


# Function to perform eSearch and get series IDs for a batch of keywords
def perform_esearch(keywords, max_retries=3, batch_size=300):
    """
    Performs an eSearch query to retrieve all series IDs for given keywords.

    Args:
        keywords (list): List of keywords to search for.
        max_retries (int): Number of retry attempts in case of errors.
        batch_size (int): Number of IDs to retrieve per request.

    Returns:
        list: A list of all unique GEO series IDs.
    """
    
    search_term = ' OR '.join([f'("{kw}"[MeSH Terms] OR {kw}[All Fields])' for kw in keywords]) + ' AND "gse"[Filter]'
    retries = 0
    gse_set = set() # Use a set to store unique GSE IDs
    retstart = 0 # Start point for fetching results
    total_results = 0  # Total number of results to fetch

    while retries < max_retries:
        try:
            # Perform an initial query to get the total number of results
            with Entrez.esearch(db="gds", term=search_term, usehistory="y", retmax=1) as handle:
                search_results = Entrez.read(handle)
                total_results = int(search_results.get("Count", 0))
            
            print(f"Found {total_results} results for the search term.")

            with tqdm(total=total_results, desc="Processing GEO series IDs", unit="id") as pbar:
                while retstart < total_results:
                
                    # Fetch results in batches using retstart
                    with Entrez.esearch(
                        db="gds",
                        term=search_term,
                        usehistory="y",
                        retmax=batch_size,
                        retstart=retstart
                    ) as handle:
                        search_results = Entrez.read(handle)
                    
                    # Extract IDs and process them into GSE IDs
                    batch_id_list = search_results["IdList"]
                    for id_str in batch_id_list:
                        try:
                            series_id = int(id_str[-6:])
                            gse_set.add(f"GSE{series_id}")
                        except ValueError:
                            print(f"Skipping invalid ID: {id_str}")

                    # Update progress bar based on the number of processed results
                    retstart += len(batch_id_list)
                    pbar.update(len(batch_id_list))

                    if len(batch_id_list) < batch_size:
                        break # No more results to fetch
            
            return list(gse_set)

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
    raise Exception("Failed to perform eSearch after multiple attempts.")


# Main function to process diseases and download matrix files
def record_gse_number(input_file, output_file, batch_size=10):
    """
    Processes diseases from the input file and saves GSE IDs into a CSV file.

    Args:
        input_file (str): Path to the input Excel file with disease data.
        output_csv (str): Path to save the output CSV file.
        batch_size (int): Number of keywords to process per batch.
    """

    df = pd.read_excel(input_file)
    # Filter rows where Series_Count > 0
    df = df[df['Series_Count'] > 0] 

    gard_to_gse = {}

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing Diseases"):
        gard_id = format_gard_id(row['GARD_ID'])
        keywords = row['GARD_Disease'].split('; ')
        print(f"Processing GARD_ID: {gard_id}, Keywords: {keywords}")


        gard_to_gse[gard_id] = []

        print(f"Processing GARD_ID: {gard_id}, Keywords: {keywords}")
        try:
            # Process keywords in batches
            for i in range(0, len(keywords), batch_size):
                batch_keywords = keywords[i:i + batch_size] # Get the current batch of keywords
                gse_ids = perform_esearch(batch_keywords)
                gard_to_gse[gard_id].extend(gse_ids)
                time.sleep(1)

    
        except Exception as e:
            print(f"Error processing {gard_id}: {e}")
        
        # Save the output
        with open(output_file, "w") as json_file:
            json.dump(gard_to_gse, json_file, indent=4)
        print(f"Saved GSE IDs to {output_file}")

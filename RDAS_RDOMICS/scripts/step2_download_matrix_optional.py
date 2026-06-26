"""
Step2: Downloading all the series matrix files per GardID based on Entrez esearch in GEO Datasets using keywords with filter gse.

i. esearch
https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=gds&term=("{kw}"[MeSH Terms]+OR+"{kw}"[All Fields])+AND+"gse"[Filter]&retmax=10&usehistory=y
We can get IdList from this page.


ii. download matrix files 
https://ftp.ncbi.nlm.nih.gov/geo/series/GSE47nnn/GSE47603/matrix/
For instance, GSE47603, we can download its file using the link above.

search_term = ' OR '.join([f'("{kw}"[MeSH Terms] OR {kw}[All Fields])' for kw in keywords]) + ' AND "gse"[Filter]'

"""
# Since the maximum digits for GSE is 6, we can extract gse directly from the IdList
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

# Entrez email setup for API usage
from Bio import Entrez
Entrez.email = os.environ.get("ENTREZ_EMAIL", "")

# Function to format GARD_ID by padding the numeric part with leading zeros to 7 digits
def format_gard_id(gard_id):
    prefix, numeric = gard_id.split(":")
    padded_numeric = numeric.zfill(7) # Pad numeric part to 7 digits
    return f"{prefix}:{padded_numeric}"

# Function to extract GSE number from IdList
def extract_gse_number(id_list):
    gse_ids = []
    for id_str in id_list:
        series_id = int(id_str[-6:]) # Extract last six digits
        gse_id = f"GSE{series_id}"
        gse_ids.append(gse_id)
    return gse_ids

# Function to determine the correct FTP directory format based on the series ID
def format_series_numeric(series_id):
    try:
        series_num = int(series_id[3:]) # Extract the numeric part of the series ID 
        if series_num < 1000:
            return f"GSEnnn"
        elif 1000 <= series_num < 10000:
            return f"GSE{series_id[3]}nnn"
        elif 10000 <= series_num < 100000:
            return f"GSE{series_id[3:5]}nnn"
        else:
            return f"GSE{series_id[3:6]}nnn"
    except Exception as e:
        print(f"Error formatting series ID {series_id}: {e}")
        return None



# Function to download matrix files
def download_matrix_file(gse_id, output_dir):
    try:
        series_numeric = format_series_numeric(gse_id)
        matrix_url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series_numeric}/{gse_id}/matrix/{gse_id}_series_matrix.txt.gz"
        print(f"Attempting to download: {matrix_url}")

        # Create the output directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        matrix_file_path = Path(output_dir) / f"{gse_id}_series_matrix.txt.gz"

        # Skip download if the file already exists
        if matrix_file_path.with_suffix("").exists():  
            print(f"File already exists: {matrix_file_path.with_suffix('')}")
            return
        
        # Fetch and save the file
        response = requests.get(matrix_url, stream=True)
        response.raise_for_status() # Raise an exception for HTTP errors

        # Save the file
        with open(matrix_file_path, "wb") as f:
            f.write(response.content)
        
        # Extract the .gz file
        with gzip.open(matrix_file_path, "rb") as f_in:
            with open(matrix_file_path.with_suffix(""), "wb") as f_out: # remove the file suffix(extension) of a path
                shutil.copyfileobj(f_in, f_out)
        
        # Remove the .gz file after extraction
        os.remove(matrix_file_path)
        print(f"Downloaded and extracted: {matrix_file_path}")

    except requests.exceptions.HTTPError:
        print(f"File not found: {matrix_url}. Downloading all files in the matrix directory...")
        try:
            # Define the FTP path
            series_path = f"/geo/series/{series_numeric}/{gse_id}/matrix/"
            ftp = ftplib.FTP("ftp.ncbi.nlm.nih.gov")
            ftp.login()
            ftp.cwd(series_path)
            
            # List all files in the directory and download them
            files = ftp.nlst()
            for file_name in files:
                local_path = Path(output_dir) / file_name
                print(f"Downloading: {file_name}")
                
                with open(local_path, "wb") as f:
                    ftp.retrbinary(f"RETR {file_name}", f.write)
                
                # Extract the .gz file
                if file_name.endswith(".gz"):
                    with gzip.open(local_path, "rb") as f_in:
                        with open(local_path.with_suffix(""), "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    os.remove(local_path)  # Remove the .gz file after extraction
            
            ftp.quit()
            print(f"All matrix files downloaded and extracted for {gse_id}.")
        except Exception as ftp_error:
            print(f"Failed to download files {gse_id} from matrix directory: {ftp_error}")           
    except Exception as e:
        print(f"An unexpected error occurred while downloading {gse_id}: {e}")

        

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
def process_diseases_and_download_matrix(input_file, output_dir, batch_size=10):
    """
    Processes diseases from the input file and downloads matrix files.

    Args:
        input_file (str): Path to the input Excel file with disease data.
        output_dir (str): Base path to save the downloaded files.
        batch_size (int): Number of keywords to process per batch.
    """

    df = pd.read_excel(input_file)
    # Filter rows where Series_Count > 0
    df = df[df['Series_Count'] > 0] 

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing Diseases"):
        gard_id = format_gard_id(row['GARD_ID'])
        keywords = row['GARD_Disease'].split('; ')
        disease_output_path = Path(output_dir) / gard_id

        # Keep track of downloaded IDs to avoid duplicates
        downloaded_ids = set()

        print(f"Processing GARD_ID: {gard_id}, Keywords: {keywords}")
        #all_ids = [] # Initialize a list to accumulate all dataset IDs for the current disease
        try:
            # Process keywords in batches
            for i in range(0, len(keywords), batch_size):
                batch_keywords = keywords[i:i + batch_size] # Get the current batch of keywords
                gse_ids = perform_esearch(batch_keywords)
                if not gse_ids:
                    continue

                # Convert IdList to GSE IDs
                #gse_ids = extract_gse_number(batch_ids)

                # Download the matrix files for each unique GSE ID not already downloaded
                for gse_id in gse_ids:
                    if gse_id not in downloaded_ids:
                        print("Start downloading:", gse_id)
                        download_matrix_file(gse_id, disease_output_path)
                        downloaded_ids.add(gse_id)
                time.sleep(1)

        except Exception as e:
            print(f"Error processing {gard_id}: {e}")

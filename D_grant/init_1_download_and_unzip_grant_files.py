import os
import sys
import json
# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import zipfile
from tqdm import tqdm  # Optional: Adds a progress bar
import requests
from pathlib import Path
from utils.tools import convert_csv_files_to_utf8

'''
    1. cd 4_grant/
    2. python init_1_download_and_unzip_grant_files.py
'''

BASE_DIR = 'data'
# Base URL for ExPORTER project files
BASE_URL = "https://reporter.nih.gov/exporter"


# Function to download a file
def download_file(url, filename):
    
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    else:
        print(f"Failed to download {url} (Status code: {response.status_code})")
        return False


def export_by_category(category, start_year, end_year):
    years = range(start_year, end_year)

    # Directory to save the files
    output_dir = f"{BASE_DIR}/{category}"
    os.makedirs(output_dir, exist_ok=True)  # Create directory if it doesn’t exist


    # Batch download files with a progress bar
    for year in tqdm(years, desc=f"Downloading NIH {category} Files"):

        file_url = f"{BASE_URL}/{category}/download/{year}"
        print(file_url)
        
        output_path = os.path.join(output_dir, f"nih_{category}_{year}.zip")
        
        # Skip if file already exists (optional)
        if os.path.exists(output_path):
            print(f"Skipping {year} - File already exists")
            continue
        
        print(f"Downloading {category} for {year}...")
        success = download_file(file_url, output_path)
        if success:
            print(f"Saved {output_path}")



#--------------------------- UNZIP the downloaded ZIP files ---------------------------------------------------------------------------------------

def unzip_files(input_dir):

    # input_dir: Directory containing the ZIP files  
    unzip_files_from_to(input_dir, input_dir)


def unzip_files_from_to(input_dir, output_dir):

    # Create output directory if it doesn’t exist
    os.makedirs(output_dir, exist_ok=True)  

    # Function to unzip a single file
    def unzip_a_file(zip_path, extract_to):

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extract to a folder named after the ZIP file (without .zip)
            '''
            extract_folder = os.path.join(extract_to, os.path.splitext(os.path.basename(zip_path))[0])
            os.makedirs(extract_folder, exist_ok=True)
            zip_ref.extractall(extract_folder)
            '''
            # Extract to the same dir as the zip file.
            zip_ref.extractall(extract_to)

        return extract_to

    # Get all ZIP files in the input directory
    zip_files = [f for f in os.listdir(input_dir) if f.endswith('.zip')]

    # Batch unzip with a progress bar
    for zip_file in tqdm(zip_files, desc="Unzipping Files"):

        zip_path = os.path.join(input_dir, zip_file)

        print(f"Unzipping {zip_file}...")
        extract_to = unzip_a_file(zip_path, output_dir)

        print(f"Extracted to {extract_to}")

    print(f"Batch unzip directory: {input_dir} complete!")




if __name__ == '__main__':

    # Define the range of fiscal years to download (e.g., 2015 to 2024)
    start_year = 1985
    end_year = 2025

    # 1.
    category = 'projects'
    # https://reporter.nih.gov/exporter/projects/download/2010  

    # 2.
    category = 'abstracts'
    # https://reporter.nih.gov/exporter/abstracts/download/2016

    # 3.
    category = 'publications'
    #start_year = 1980
    # https://reporter.nih.gov/exporter/publications/download/2016

    # 4.
    category = 'linktables'
    #start_year = 1980
    # https://reporter.nih.gov/exporter/linktables/download/2016

    ''' '''
    #export_by_category(category, start_year, end_year)

    # 5. Manually download Patents
    # https://reporter.nih.gov/exporter/patents
    # https://reporter.nih.gov/exporter/patents/download

    # 6. Manually download Clinical Studies
    # https://reporter.nih.gov/exporter/clinicalstudies
    # https://reporter.nih.gov/exporter/clinicalstudies/download
     


    # 4. UNZIP the downloaded ZIP files --------------------------------------------------------

    dir_names = ['projects', 'abstracts', 'publications', 'linktables']
  
    '''
    for dir in dir_names:        
        zip_dir = f'{BASE_DIR}/{dir}'
        #unzip_files(zip_dir)
    '''

    # 5. convert_csv_files_to_utf8 
    base_dir = f'{Path(__file__).parent}/data'

    for dir in dir_names:        
        files_dir = f'{base_dir}/{dir}'
        #convert_csv_files_to_utf8(files_dir)
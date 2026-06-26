"""
Now we get the GSE numbers, next we want download all the information we need to form our table
"""
import os
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

#********************
# Load data functions
#********************
def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def load_table_template(template_path):
    return pd.read_excel(template_path, sheet_name=0)

#******************
# Folder management
#******************
def create_output_folder(base_path, gard_id):
    folder_path = os.path.join(base_path, gard_id)
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

# Create a new requests seesion for time efficiency
def get_requests_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
#***************************************************************************************
# Web extraction functions
# Below includes all the functions for extracting information from GEO website 
#***************************************************************************************

# Main function for the extracting part
def extract_gse_data(gse_id, session):
    """Extract relevant GSE information from the GEO page"""
    base_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={gse_id}"
    try:
        response = session.get(base_url)
        response.raise_for_status() # ensure we catch HTTP errors
    except requests.exceptions.ConnectionError:
        # Recreate session in case of session expiration
        print(f"Session expired. Recreating session for {gse_id}...")
        session = get_requests_session()
        response = session.get(base_url)
        response.raise_for_status()

    
    series_soup = BeautifulSoup(response.text, "html.parser")
    # GSEnnn for directory
    gse_numeric = format_geo_numeric(gse_id)

    # Extract Experiment Content
    series_geo_accession, series_citations, series_supplementary_file = extract_series_content(series_soup, gse_numeric, gse_id, session)
    
    # Platform -> GPL, 
    platform_title, platform_manufacturer = extract_platform_content(series_soup, session)

    # Sample -> GSM
    sample_geo_accession, sample_title, sample_organism, sample_source_name, sample_supplementary_file, sample_library_strategy, sample_extracted_molecule, sample_characteristics, sample_biosample, sample_srr = extract_sample_content(series_soup, session) 
    #sample_external_reference_id = f"Bio-sample id: {sample_biosample or ''}; SRA id: {sample_srr or ''}"

    # Project -> Series_Relation
    series_project_id, project_title, project_abstract, project_data_type, project_submission = extract_project_content(series_soup, session)    
   


    result = {
        "Series_geo_accession": series_geo_accession,
        #"Series_status": series_status,
        "Series_citations": series_citations, #Pubmed_id
        "Series_supplementary_file": series_supplementary_file,
        #"Series_platform_id": series_platform_id,
        "Platform_title": platform_title,
        #"Platform_technology": platform_technology,
        "Platform_manufacturer": platform_manufacturer,
        #"Platform_supplementary_file": platform_supplementary_file,
        "Sample_geo_accession": sample_geo_accession,
        "Sample_title": sample_title,
        "Sample_organism": sample_organism,
        "Sample_source_name": sample_source_name, # Disease_status
        "Sample_supplementary_file": sample_supplementary_file,
        "Sample_library_strategy": sample_library_strategy,  # sequencing_type
        "Sample_extracted_molecule": sample_extracted_molecule,   # sequencing_library
        "Sample_characteristics": sample_characteristics,
        "Sample_biosample": sample_biosample,
        "Sample_srr": sample_srr,
        #"Sample_external_reference_id": sample_external_reference_id,
        "Series_project_id": series_project_id,
        "Project_title": project_title,
        "Project_abstract": project_abstract,
        "Project_data_type": project_data_type,     #Omics_type
        "Project_submission": project_submission   # Data_release_date
        #"Project_relevance": project_relevance,
    }

    #result.update(gds_details_list)

    return result

# Helper function to determine the correct FTP directory format based on the GEO ID (series/platform)
def format_geo_numeric(geo_id):
    try:
        if not geo_id.startswith(("GSE", "GSM", "GPL")):
            raise ValueError(f"Invalid GEO ID prefix: {geo_id}")
        
        prefix = geo_id[:3]
        geo_num = geo_id[3:] # Extract the numeric part of the ID 
        
        if not geo_num.isdigit():
            raise ValueError(f"Non-numeric GEO ID: {geo_id}")
        
        geo_num = int(geo_num)

        if geo_num < 1000:
            return f"{prefix}nnn"
        elif 1000 <= geo_num < 10000:
            return f"{prefix}{geo_id[3]}nnn"
        elif 10000 <= geo_num < 100000:
            return f"{prefix}{geo_id[3:5]}nnn"
        else:
            return f"{prefix}{geo_id[3:6]}nnn"
    except ValueError as ve:
        print(f"Error formatting series ID {geo_id}:{ve}")
        raise
        #return None
    except Exception as e:
        print(f"Unexpected error with GEO ID {geo_id}: {e}")
        raise
        #return None
    
def extract_supplementary_files(soup, geo_numeric, geo_id):
    """Extract all supplementary file links."""
    files = []
    prefix = geo_id[:3]
    # Find the header row for 'Supplementary file'
    header_row = soup.find("td", align="middle", text="Supplementary file")
    if not header_row:
        return files # return empty if not found
    
    # Find the parent table containing supplementary files
    table = header_row.find_parent("table")
    if not table:
        return files
    
    # Extract file names from rows below the header
    for row in table.find_all("tr", valign="top"):
        file_tag = row.find("td", bgcolor=True)
        base_url = None
        if file_tag and (file_tag.text.endswith(".gz") or file_tag.text.endswith(".tar")):
            file_name = file_tag.text.strip()            
            if prefix == "GSE":
                base_url = f"ftp://ftp.ncbi.nlm.nih.gov/geo/series/{geo_numeric}/{geo_id}/suppl/{file_name}"
            elif prefix == "GPL":
                base_url = f"ftp://ftp.ncbi.nlm.nih.gov/geo/platforms/{geo_numeric}/{geo_id}/suppl/{file_name}"            
            if base_url:
                files.append(base_url)
            
    return files

# Extracting helper function groups
def extract_series_content(series_soup, gse_numeric, gse_id, session):
    """Extract the publication status."""
    series_geo_accession = [gse_id]
    
    citations_tag = series_soup.find("td", text="Citation(s)")
    citations = [citations_tag.find_next_sibling("td").text.strip()] if citations_tag and citations_tag.find_next_sibling("td") else []

    series_supplementary_file = extract_supplementary_files(series_soup, gse_numeric, gse_id)
  
    return series_geo_accession, citations, series_supplementary_file


def extract_platform_content(series_soup, session):
    # Extract platform id first on series page
    #platform_id_tag = series_soup.find("td", string=lambda text: text and "Platforms" in text)
    platform_id_tag = series_soup.find(lambda tag: tag.name == "td" and "Platforms " in tag.get_text())

    #platform_id_tag = series_soup.find("td", text=re.compile(r"Platforms \(\d+\)"))
    #print("platform_id_tag: ", platform_id_tag)
    platform_ids = []

    if platform_id_tag:
        # Navigate to the parent <tr> and then find all <a> tags within it
        platform_tr = platform_id_tag.find_parent("tr")
        #print("platform_tr:", platform_tr)
        if platform_tr:
            platform_links = platform_tr.find_all("a", href=re.compile(r"acc=GPL"))
            platform_ids = [link.text.strip() for link in platform_links]
        
    #print("platform_ids: ", platform_ids)
    platform_title = []
    #platform_technology = []
    platform_manufacturer = []
    #platform_supplementary_file = []

    for platform_id in platform_ids:
        #gpl_numeric = format_geo_numeric(platform_id)

        # Extract from GPL page 
        platform_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={platform_id}"
        response = session.get(platform_url)
        platform_soup = BeautifulSoup(response.text, "html.parser")
        
        platform_title_tag = platform_soup.find("td", text="Title")
        #platform_title[platform_title_tag.find_next_sibling("td").text.strip()] if platform_title_tag and platform_title_tag.find_next_sibling("td") else []
        platform_title.append(
            platform_title_tag.find_next_sibling("td").text.strip() if platform_title_tag and platform_title_tag.find_next_sibling("td") else None
        )

        #platform_technology_tag = platform_soup.find("td", text="Technology type")
        #platform_technology.append(
        #    platform_technology_tag.find_next_sibling("td").text.strip() if platform_technology_tag and platform_technology_tag.find_next_sibling("td") else None
        #)

        platform_manufacturer_tag = platform_soup.find("td", text="Manufacturer")
        #platform_manufacturer = [platform_manufacturer_tag.find_next_sibling("td").text.strip()] if platform_manufacturer_tag and platform_manufacturer_tag.find_next_sibling("td") else []
        platform_manufacturer.append(
            platform_manufacturer_tag.find_next_sibling("td").text.strip() if platform_manufacturer_tag and platform_manufacturer_tag.find_next_sibling("td") else None
        )

        #platform_supplementary_file.append(
        #    extract_supplementary_files(platform_soup, gpl_numeric, platform_id)
        #)
    
    return platform_title, platform_manufacturer # platform_ids, platform_technology,platform_supplementary_file
"""
# Configure a requests session with retries
def get_requests_session():
    session = requests.Session()
    retries = Retry(
        total=5,  # Retry up to 5 times
        backoff_factor=0.5,  # Wait 0.5s, then 1s, 2s, etc.
        status_forcelist=[500, 502, 503, 504],  # Retry on these HTTP status codes
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
"""
def extract_sample_content(series_soup, session):
    #session = get_requests_session() # Use session with retries

    # Extract sample_ids
    sample_text = series_soup.find(text=re.compile(r"^Samples \(\d+"))
    sample_ids = []

    if sample_text:
        # Navigate to its parent <td>
        sample_id_tag = sample_text.find_parent("td")
        if sample_id_tag and sample_id_tag.find_next_sibling("td"):
            sample_id_td = sample_id_tag.find_next_sibling("td")
            # Find all <a> tags containing platform IDs
            sample_links = sample_id_td.find_all("a")
            sample_ids = [link.text.strip() for link in sample_links if link.text.startswith("GSM")]

        
    # Initialize lists    
    sample_title = []
    sample_organism = []
    sample_source_name = []
    sample_supplementary_file = []
    sample_library_strategy = []
    sample_extracted_molecule = []
    sample_characteristics = []
    sample_biosample = []
    sample_srr = []

    for sample_id in sample_ids:
        if not sample_id.startswith("GSM"):
            print("******Here is the sample_id*****: ", sample_id)
        gsm_numeric = format_geo_numeric(sample_id) # GSMnnn

        # Extract from GSM page 
        sample_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={sample_id}"
        try:
            response = session.get(sample_url, timeout=10)
            response.raise_for_status() # Raise HTTPError for bad responses
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data for {sample_id}: {e}")
            continue
        #response = requests.get(sample_url)
        sample_soup = BeautifulSoup(response.text, "html.parser")
        
        # Extract required fields
        sample_title_tag = sample_soup.find("td", text="Title")
        sample_title.append(
            sample_title_tag.find_next_sibling("td").text.strip() if sample_title_tag and sample_title_tag.find_next_sibling("td") else None
        )
        
        sample_organism_tag = sample_soup.find("td", text="Organism")
        sample_organism.append(
            sample_organism_tag.find_next_sibling("td").text.strip() if sample_organism_tag and sample_organism_tag.find_next_sibling("td") else None
        )
        
        sample_source_name_tag = sample_soup.find("td", text="Source name")
        sample_source_name.append(
            sample_source_name_tag.find_next_sibling("td").text.strip() if sample_source_name_tag and sample_source_name_tag.find_next_sibling("td") else None
        )

        sample_extracted_molecule_tag = sample_soup.find("td", text="Extracted molecule")
        sample_extracted_molecule.append(
            sample_extracted_molecule_tag.find_next_sibling("td").text.strip() if sample_extracted_molecule_tag and sample_extracted_molecule_tag.find_next_sibling("td") else None
        )

        sample_library_strategy_tag = sample_soup.find("td", text="Library strategy")
        sample_library_strategy.append(
            sample_library_strategy_tag.find_next_sibling("td").text.strip() if sample_library_strategy_tag and sample_library_strategy_tag.find_next_sibling("td") else None
        )
        

        # Sample Characteristics
        sample_characteristics_tag = sample_soup.find("td", text="Characteristics")
        sample_characteristics_td = sample_characteristics_tag.find_next_sibling("td") if sample_characteristics_tag else None
        characteristics = ""
        if sample_characteristics_td:
            # Extract text and replace <br> with ;
            characteristics = sample_characteristics_td.get_text(separator="; ").strip()
        sample_characteristics.append(characteristics)


        # Sample supplementary files
        sample_supplementary_file.append(
            extract_supplementary_files(sample_soup, gsm_numeric, sample_id)
        )

        # Sample BioProject        
        sample_biosample_tag = sample_soup.find("td", text="BioProject")
        sample_biosample.append(
            sample_biosample_tag.find_next_sibling("td").text.strip() if sample_biosample_tag and sample_biosample_tag.find_next_sibling("td") else None
        )
                
        # Sample SRA -> SRR
        sample_sra_link = None
        sample_srr_number = None
        # Find the SRA link
        sample_sra_tag = sample_soup.find("td", text="SRA", valign="top")
        if sample_sra_tag:
            next_td = sample_sra_tag.find_next_sibling("td")
            if next_td:
                sample_sra_link = next_td.find("a")["href"]
        # Check if the link was found before making a request
        if sample_sra_link:
            response = requests.get(sample_sra_link)
            sample_sra_soup = BeautifulSoup(response.text, "html.parser")
            sample_srr_tag = sample_sra_soup.find("a", text=re.compile(r'^SRR\d+'))
            #print("sample_srr_tag:", sample_srr_tag)
            sample_srr_number = sample_srr_tag.get_text(strip=True) if sample_srr_tag else None            
        sample_srr.append(sample_srr_number)       

    return sample_ids, sample_title, sample_organism, sample_source_name, sample_supplementary_file, sample_library_strategy, sample_extracted_molecule, sample_characteristics, sample_biosample, sample_srr

def extract_project_content(series_soup, session):
    # Initialize variables with default None values
    project_id, project_title, project_abstract, project_data_type, project_submission = ([],) * 5
    
    project_id_tag = series_soup.find("td", text="BioProject")
    project_link = None
    #print("project_id_tag: ", project_id_tag)

    if project_id_tag and project_id_tag.find_next_sibling("td"):
        project_id = [project_id_tag.find_next_sibling("td").text.strip()]

        # Extract project link
        link_tag = project_id_tag.find_next_sibling("td").find("a")
        if link_tag and link_tag["href"]:
            project_link = link_tag["href"]
            
            # Handle incomplete link
            if project_link.startswith("/"):
                project_link = f"https://www.ncbi.nlm.nih.gov{project_link}"

    if project_link:
        response = session.get(project_link)
        project_soup = BeautifulSoup(response.text, "html.parser")
        # Extract title, abstract, datatype, submission, relevance, geo dataset
        # Title
        title_div = project_soup.find('div', class_='Title')
        if title_div:
            project_title = [title_div.get_text(separator=' ', strip=True)]

        #print("project_title: ", project_title)

        # abstract
        abstract_div = project_soup.find('div', id='DescrAll') or project_soup.find('div', class_='Description')
        if abstract_div:
            abstract_text = abstract_div.get_text(separator=' ', strip=True).split(' Less...')[0]
            project_abstract = [abstract_text]

        # Extract data type
        data_type_tag = project_soup.find('td', class_='CTtitle', text='Data Type')
        if data_type_tag:
            data_type_content = data_type_tag.find_next_sibling('td', class_='CTcontent')
            if data_type_content:
                project_data_type = [data_type_content.get_text(strip=True)]

        # Extract submission date
        submission_tag = project_soup.find('td', class_='CTtitle', text='Submission')
        if submission_tag:
            submission_content = submission_tag.find_next_sibling('td', class_='CTcontent')
            if submission_content:
                project_submission = [submission_content.get_text(separator=' ', strip=True)]

        # Extract relevance
        #relevance_tag = project_soup.find('td', class_='CTtitle', text='Relevance')
        #if relevance_tag:
        #    relevance_content = relevance_tag.find_next_sibling('td', class_='CTcontent')
        #    if relevance_content:
        #        project_relevance = [relevance_content.get_text(strip=True)]
        
        # Extract dataset link
        #gds_details_list = extract_dataset_content(project_soup)

    return project_id, project_title, project_abstract, project_data_type, project_submission
   
"""
def extract_dataset_content(project_soup):
    gds_details_list = []

    # Search GEO DataSets and get gds_search_soup, then find the gds_soup, then extract the contents
    #dataset_search_link = None
    dataset_search_link_tag = project_soup.find('a', class_='brieflinkpopperctrl', text='GEO DataSets')
    if not dataset_search_link_tag or not dataset_search_link_tag['href']:
        return gds_details_list
    
    # Construct the full link
    dataset_search_link = f"https://www.ncbi.nlm.nih.gov{dataset_search_link_tag['href']}"
    response = requests.get(dataset_search_link)
    dataset_search_soup = BeautifulSoup(response.text, "html.parser")

    # Extract all GDS numbers from dataset_search_soup
    # Find all occurrences of 'Accession:' in <dt> tags
    accession_tags = dataset_search_soup.find_all('dt', text='Accession:')
    # Loop through and extract corresponding <dd> values
    gds_numbers = [tag.find_next_sibling('dd').text.strip() 
                   for tag in accession_tags 
                   if tag.find_next_sibling('dd') and tag.find_next_sibling('dd').text.startswith('GDS')]
    
    
    result_gds_list = {
        "Title": [],
        "Summary": [],
        "Organism": [],
        "Platform": [],
        "Citation": [],
        "Reference_Series": [],
        "Sample_Count": [],
        "Value_Type": [],
        "Series_Published": [],
        "GDS_Number": []
    }

    # Visit each GDS page and Extract GDS details
    for gds_number in gds_numbers:
        gds_url = f"https://www.ncbi.nlm.nih.gov/sites/GDSbrowser?acc={gds_number}"
        response = requests.get(gds_url)
        dataset_soup = BeautifulSoup(response.text, "html.parser")
        gds_details = extract_gds_details(dataset_soup)
        gds_details["GDS_Number"] = gds_number

        for key,value in gds_details:
            result_gds_list[key].append(value)
    print("result_gds_list: ",result_gds_list)
    return result_gds_list

        #gds_details_list.append(gds_details)
    #eturn gds_details_list

# Extract details from the dataset_soup table
def extract_gds_details(dataset_soup):
    details = {
        "Title": None,
        "Summary": None,
        "Organism": None,
        "Platform": None,
        "Citation": None,
        "Reference_Series": None,
        "Sample_Count": None,
        "Value_Type": None,
        "Series_Published": None,
    }
    
    # Helper function to extract text based on header label
    def extract_detail(label):
        header = dataset_soup.find('th', class_='not_caption', text=label)
        if header:
            td = header.find_next_sibling('td')
            if td:
                return td.get_text(separator=' ', strip=True)
        return None

    # Extract each field
    details["Title"] = extract_detail("Title:")
    details["Summary"] = extract_detail("Summary:")
    details["Organism"] = extract_detail("Organism:")
    details["Platform"] = extract_detail("Platform:")
    details["Citation"] = extract_detail("Citation:")
    details["Reference Series"] = extract_detail("Reference Series:")
    details["Sample Count"] = extract_detail("Sample count:")
    details["Value Type"] = extract_detail("Value type:")
    details["Series Published"] = extract_detail("Series published:")

    return details
"""
#******************
# Table management
#******************

def fill_table_with_data(template, data, gard_id, disease_name_list):
    table = template.copy()

    cleaned_gard_id = f"GARD:{gard_id.split(':')[1].lstrip('0')}"
    #print("cleaned_gard_id: ", cleaned_gard_id)
    #print("disease_name_list: ", disease_name_list)
    disease_df = pd.read_excel(disease_name_list)
    disease_row = disease_df[disease_df['GARD_ID'] == cleaned_gard_id]
    #print("disease_row: ",disease_row)
    # Extract the first part of 'GARD_Disease' separated by ';'
    if not disease_row.empty:
        condition_name = disease_row.iloc[0]['GARD_Disease'].split(';')[0].strip()
        #print("Here condition name:", condition_name)
    else:
        condition_name = ''  # Default if not found
    #print("condition_name: ", condition_name)

    # Fill the Content column with extracted data, Use apply() to insert lists directly
    # Series
    # Platform
    # Define mappings between attributes and data keys
    attribute_data_mapping = {
        'External_experiment_source_id': 'Series_geo_accession',
        #'Publish_date': 'Series_status',
        'Pubmed_id': 'Series_citations',
        'Experiment_data_link': 'Series_supplementary_file',

        #'Source_platform_id': 'Series_platform_id',
        'Platform_name': 'Platform_title',
        #'Platform_technology': 'Platform_technology',
        'Platform_manufacturer': 'Platform_manufacturer',
        #'Platform_supplementary_file': 'Platform_supplementary_file',

        'External_sample_source_id':'Sample_geo_accession',
        'Sample_name': 'Sample_title',
        'Sample_organism': 'Sample_organism',
        #'Specimen_type': 'Sample_source_name',
        'Disease_status': 'Sample_source_name',
        'Sample_data_link': 'Sample_supplementary_file',
        'Sequencing_type': 'Sample_library_strategy',
        'Sequencing_library': 'Sample_extracted_molecule',
        'Sample_characteristics': 'Sample_characteristics',
        'Biosample_id': 'Sample_biosample',
        'SRA_id': 'Sample_srr',
        'External_project_source_id': 'Series_project_id',
        'Project_title': 'Project_title',
        'Project_description': 'Project_abstract',
        'Omics_type': 'Project_data_type',    
        'Data_release_date': 'Project_submission'
        #'Project_relevance': 'Project_relevance',

        
    }
    #print("data.keys():", data.keys())
    # Apply updates for each attribute
    for attribute, data_key in attribute_data_mapping.items():
        mask = table['Attribute'] == attribute
        if mask.any():
            table.loc[mask, 'Content'] = table.loc[mask, 'Content'].apply(lambda _: data[data_key])


     # Add GardId
    mask_gard = table['Attribute'] == 'GardId'
    if mask_gard.any():
        table.loc[mask_gard, 'Content'] = table.loc[mask_gard, 'Content'].apply(lambda _: [f'{gard_id}'])

    # Add Hosting_repository
    mask_rare = table['Attribute'] == 'Hosting_repository'
    if mask_rare.any():
        table.loc[mask_rare, 'Content'] = table.loc[mask_rare, 'Content'].apply(lambda _: ['GEO'])

    # Add Project_type
    mask_rare = table['Attribute'] == 'Project_type'
    if mask_rare.any():
        table.loc[mask_rare, 'Content'] = table.loc[mask_rare, 'Content'].apply(lambda _: ['Disease Characterization'])

    # Add Condition_name
    #print("gard_id: ", gard_id) # gard_id:  GARD:0004647
    mask_condition_name = table['Attribute'] == 'Condition_name'
    if mask_condition_name.any():
        table.loc[mask_condition_name, 'Content'] = table.loc[mask_condition_name, 'Content'].apply(lambda _: [f'{condition_name}'])





    """
    # Ensure all rows in 'Content' are lists even if empty
    # table['Content'] = table['Content'].apply(lambda x: x if isinstance(x, list) else [])

    """
    return table

def save_table(folder_path, gse_id, table):
    output_file = os.path.join(folder_path, f"{gse_id}_table.csv")
    table.to_csv(output_file, index=False)


def process_gse(gse_id, gard_id, table_template, disease_name_list, output_dir, session):
    try:
        print(f"Processing GSE ID: {gse_id} for GARD ID: {gard_id}")  # Log the GSE ID and GARD ID
        gard_folder = create_output_folder(output_dir, gard_id)
        gse_data = extract_gse_data(gse_id, session)
        filled_table = fill_table_with_data(table_template, gse_data, gard_id, disease_name_list)
        save_table(gard_folder, gse_id, filled_table)
    except Exception as e:
        print(f"Error processing GSE ID {gse_id} for GARD ID {gard_id}: {e}")
        raise
#***************************************************************************************
# Main function for this .py file
#***************************************************************************************
def extract_to_table(input_file, table_template, disease_name_list, output_dir):
    
    gard_to_gse = load_json(input_file)
    table_template = load_table_template(table_template)
    session = get_requests_session()

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for gard_id, gse_list in gard_to_gse.items():

            for gse_id in gse_list:
                futures.append(executor.submit(
                    process_gse, gse_id, gard_id, table_template, disease_name_list, output_dir, session
                ))
        for future in tqdm(as_completed(futures), total=len(futures), desc = "Processing GSEs"):
            future.result()
    print("Extraction and table creation completed")
    """
    session = get_requests_session()
    
    base_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE4303"
    try:
        response = session.get(base_url)
        response.raise_for_status() # ensure we catch HTTP errors
    except requests.exceptions.ConnectionError:
        # Recreate session in case of session expiration
        session = get_requests_session()
        response = session.get(base_url)
        response.raise_for_status()

    
    series_soup = BeautifulSoup(response.text, "html.parser")
    platform_title, platform_manufacturer = extract_platform_content(series_soup, session)
    print("platform_title")
    for value in platform_title:
        print(value)

    print("platform_manufacturer")
    for value in platform_manufacturer:
        print(value)
    
    """
    
"""
def extract_to_table(input_file, table_template, disease_name_list, output_dir):
    gard_to_gse = load_json(input_file)
    table_template = load_table_template(table_template)
    session = get_requests_session() # create a single session

    # Track progress for total GARD IDs
    with tqdm(total=len(gard_to_gse), desc="Processing GARD IDs") as gard_bar:
        # Iterate through each GARD ID and corresponding GSEs        
        for gard_id, gse_list in gard_to_gse.items():
            gard_folder = create_output_folder(output_dir, gard_id)
            
            # Track progress for each GSE list under the current GARD ID
            with tqdm(total=len(gse_list), desc=f"{gard_id}", leave=False) as gse_bar:
                for gse_id in gse_list:
                    print(f"Processing {gse_id} for {gard_id}...")
                    gse_data = extract_gse_data(gse_id)
                    filled_table = fill_table_with_data(table_template, gse_data, gard_id, disease_name_list)
                    save_table(gard_folder, gse_id, filled_table)
                    gse_bar.update(1)

            gard_bar.update(1)
    print("Extraction and table creation completed.")
"""
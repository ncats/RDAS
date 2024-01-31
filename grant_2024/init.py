import sys
import os
import sysvars
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
import methods as rdas
from AlertCypher import AlertCypher
import json
from time import sleep
from datetime import date
import grant_2024.methods as rdas
from datetime import datetime

def main ():
    today = datetime.now().strftime("%m/%d/%Y")

    path = f'{sysvars.base_path}grant_2024/src/raw/all_projects.json'

    print(f"[CT] Database Selected: {sysvars.gnt_db}\nContinuing with script in 5 seconds...")
    sleep(5)

    # Connect to the Neo4j database
    db = AlertCypher(sysvars.gnt_db)

    # Clear the grant Neo4j for database recreation
    #db.run('MATCH (x) -[r] -> () DELETE x, r')
    #db.run('MATCH (x) DELETE x')

    # Copy over Gard data to Grant Database
    rdas.create_gard_nodes(db)

    # Retrieve all files from NIH Exporter
    rdas.download_nih_data()

    # Convert/Combine NIH CSV files to JSON
    rdas.convert_csv_data()

    # Retrieve NIH Exporter JSON Data
    with open(f"{path}","r") as file:
        NIH_data = json.load(file)
    
    # Begin GARD to Project Mapping
    for entry in NIH_data:
        gard_ids = rdas.GardNameExtractor(entry['project_title'], entry['phr_text'], entry['abstract_text'])

        if gard_ids:
            rdas.create_data_model(db, gard_ids, entry, today)

    # Calculate total cost for all projects under each core project
            
    # Run MetaMap to get annotation data

    print('NIH Funded Project Database Creation Finished')
        

main()
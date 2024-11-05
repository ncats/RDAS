import os,sys

workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
# sys.path.append(os.getcwd())
sys.path.append('/home/aom2/RDAS')
import pandas as pd
import sysvars
from RDAS_GARD import methods as rdas
from AlertCypher import AlertCypher
from time import sleep

def start_update():
    print(f"[CT] Database Selected: {sysvars.gard_db}\nContinuing with script in 5 seconds...")
    sleep(5)

    db = AlertCypher(f'{sysvars.gard_db}')
    rdas.retrieve_gard_data()
    gard = pd.read_csv(f'{sysvars.gard_files_path}GARD.csv', index_col=False)
    classification = pd.read_csv(f'{sysvars.gard_files_path}GARD_classification.csv', index_col=False)
    xrefs = pd.read_csv(f'{sysvars.gard_files_path}GARD_xrefs.csv', index_col=False)
    genes = pd.read_csv(f'{sysvars.gard_files_path}GARD_genes.csv', index_col=False)
    phenotypes = pd.read_csv(f'{sysvars.gard_files_path}GARD_phenotypes.csv', index_col=False)
    data = {'gard':gard, 'classification':classification, 'xrefs':xrefs, 'genes': genes, 'phenotypes': phenotypes}
    rdas.generate(db, data)
import pandas as pd
import sysvars
from gard import methods as rdas
from AlertCypher import AlertCypher

def main():
    db = AlertCypher('gard')
    rdas.retrieve_gard_data(db)
    gard = pd.read_csv(f'{sysvars.gard_files_path}GARD.csv', index_col=False)
    classification = pd.read_csv(f'{sysvars.gard_files_path}GARD_classification.csv', index_col=False)
    xrefs = pd.read_csv(f'{sysvars.gard_files_path}GARD_xrefs.csv', index_col=False)
    genes = pd.read_csv(f'{sysvars.gard_files_path}GARD_genes.csv', index_col=False)
    phenotypes = pd.read_csv(f'{sysvars.gard_files_path}GARD_phenotypes.csv', index_col=False)
    data = {'gard':gard, 'classification':classification, 'xrefs':xrefs, 'genes': genes, 'phenotypes': phenotypes}
    rdas.generate(db, data)

if __name__ == '__main__':
    main()

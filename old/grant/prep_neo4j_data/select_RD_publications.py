import glob
import pandas as pd

ENCODING = 'latin1'


pub_path = '../../data_raw/publications/'
pub_files = glob.glob(pub_path + '*.csv')

lnk_path = '../../data_neo4j/link_tables/'
lnk_files = glob.glob(lnk_path + '*.csv')

for pub_file in pub_files:
    pub = pd.read_csv(pub_file, encoding=ENCODING)
    mask = [False for i in range(pub.shape[0])]
    
    for lnk_file in lnk_files:
        lnk = pd.read_csv(lnk_file)
        pmid_lst = lnk['PMID'].unique()
        
        is_rd = pub['PMID'].isin(pmid_lst).tolist()
        mask = [mask or is_rd for mask, is_rd in zip(mask, is_rd)]
        
    output_file = '../../data_neo4j/publications/RD_PUB_' + pub_file[43:47] + '.csv'
    pub = pub[mask]
    pub.to_csv(output_file, index=False)
    print("Finished ", output_file)


import glob
import pandas as pd


ENCODING = 'latin1'


def find_RD_apps(input_file, rd_ids):
    '''
    Extract the applications that are rare disease related 
    
    Parameters:
    input_file: path and filename of the file
    rd_ids: a list of rare disease related application IDs
    '''
    
    apps = pd.read_csv(input_file, encoding=ENCODING, low_memory=False)

    # Get RD-related applications
    rd_related = apps['APPLICATION_ID'].isin(rd_ids)
    apps = apps[rd_related]
    apps.sort_values(by=['APPLICATION_ID'], inplace=True)

    return apps


# Read the list of RD Application IDs
rd_ids = pd.read_csv('../../data_neo4j/NormMap_mapped_app_ids.csv')
rd_ids = rd_ids['APPLICATION_ID'].tolist()
       
# Get CSV files lists from a folder
input_path = '../../data_neo4j/projects_with_funds/'
files = glob.glob(input_path + '*.csv')

for file in files:

    output_file = '../../data_neo4j/projects/RD_PROJECTS_' + file[54:58] + '.csv'

    apps = find_RD_apps(file, rd_ids)
    apps.to_csv(output_file, index=False, encoding=ENCODING)
    print('Finished ', output_file)

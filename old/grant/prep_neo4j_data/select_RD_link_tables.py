import glob
import pandas as pd


ENCODING = 'latin1'


def find_RD_core_project(input_file, col_name_to_replace, core_proj_nums):
    '''
    Extract the applications that are rare disease related 
    
    Parameters:
    input_file: path and filename of the file
    core_proj_nums: a list of rare disease related core project numbers
    '''
    
    proj = pd.read_csv(input_file, encoding=ENCODING, low_memory=False)
    headers = proj.columns
    proj.columns = list(map(lambda x: x.replace(col_name_to_replace, 'CORE_PROJECT_NUM'), headers))
    
    # Get RD-related applications
    rd_related = proj['CORE_PROJECT_NUM'].isin(core_proj_nums)
    proj = proj[rd_related]
    proj.sort_values(by=['CORE_PROJECT_NUM'], inplace=True)

    return proj



# Read the list of RD related core project numbers
core_proj_nums = pd.read_csv('../../data_neo4j/RD_coreProjNum.csv')
core_proj_nums = core_proj_nums['CORE_PROJECT_NUM'].tolist()

##### For files in a folder #####
input_path = '../../data_raw/link_tables/'
files = glob.glob(input_path + '*.csv')

for file in files:
    proj = find_RD_core_project(file, 'PROJECT_NUMBER', core_proj_nums)

    output_file = '../../data_neo4j/link_tables/RD_LINK_TABLE_' + file[45:49] + '.csv'
    proj.to_csv(output_file, index=False, encoding=ENCODING)
    print('Finished ', output_file)

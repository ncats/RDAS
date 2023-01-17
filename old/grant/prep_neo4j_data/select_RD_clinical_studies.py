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

file = '../../data_raw/clinical_studies/ClinicalStudies_1659286507775.csv'
proj = find_RD_core_project(file, 'Core Project Number', core_proj_nums)
output_file = '../../data_neo4j/clinical_studies/RD_CLINICAL_STUDIES.csv'

proj.to_csv(output_file, index=False, encoding=ENCODING)
print('Finished!')

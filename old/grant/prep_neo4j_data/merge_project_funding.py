import pandas as pd

ENCODING = 'latin1'

input_file_path = "../../data_raw/projects/"
output_file_path = "../../data_neo4j/projects_with_funds/"

for year in range(1985, 2000, 1):
    funding_file_name = input_file_path + "RePORTER_PRJFUNDING_C_FY" + str(year) + ".csv"
    funding = pd.read_csv(funding_file_name, encoding=ENCODING)
    funding.columns = ['APPLICATION_ID','FULL_PROJECT_NUM','FUNDING_ICs','FY','ORG_DUNS','SUBPROJECT_ID','TOTAL_COST','TOTAL_COST_SUB_PROJECT']
    funding.sort_values('APPLICATION_ID', inplace=True)
    
    project_file_name = input_file_path + "RePORTER_PRJ_C_FY" + str(year) + ".csv"
    project = pd.read_csv(project_file_name, encoding=ENCODING, low_memory=False)
    project.sort_values('APPLICATION_ID', inplace=True)
   
    match_col = 'APPLICATION_ID'
    replace_cols = ['FULL_PROJECT_NUM','FUNDING_ICs','FY','ORG_DUNS','SUBPROJECT_ID','TOTAL_COST','TOTAL_COST_SUB_PROJECT']
    
    project.loc[project[match_col].isin(funding[match_col]), replace_cols] = funding.loc[funding[match_col].isin(project[match_col]), replace_cols].values
    
    output_file_name = output_file_path + "RePORTER_PRJ_C_FY" + str(year) + "_cleaned.csv"
    project.to_csv(output_file_name, index=False, encoding=ENCODING)
    
    print("Finished", year)


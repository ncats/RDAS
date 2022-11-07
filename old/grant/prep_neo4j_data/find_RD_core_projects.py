import glob
import pandas as pd

ENCODING = 'latin1'


apps = pd.read_csv('../../data_neo4j/NormMap_mapped_app_ids.csv')

match_col = 'APPLICATION_ID'
new_col = 'CORE_PROJECT_NUM'

input_path = '../../data_neo4j/projects/'
files = glob.glob(input_path + '*.csv')

for file in files:
    proj = pd.read_csv(file, usecols=['APPLICATION_ID', 'CORE_PROJECT_NUM'], encoding=ENCODING)
    proj.sort_values('APPLICATION_ID', inplace=True)
    apps.loc[apps[match_col].isin(proj[match_col]), new_col] = proj.loc[proj[match_col].isin(apps[match_col]), new_col].values

# Export RD related APPLICATION_ID and CORE_PROJECT_NUM pairs
apps.to_csv("../../data_neo4j/RD_appID_coreProjNum.csv", index=False)


# Export unique CORE_PROJECT_NUM
core_proj_num = apps['CORE_PROJECT_NUM'].unique()
core_proj_num_df = pd.DataFrame(core_proj_num)
core_proj_num_df.columns = ['CORE_PROJECT_NUM']
core_proj_num_df.sort_values('CORE_PROJECT_NUM', inplace=True)
core_proj_num_df.to_csv("../../data_neo4j/RD_coreProjNum.csv", index=False)
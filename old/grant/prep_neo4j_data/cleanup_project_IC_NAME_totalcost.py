import glob
import pandas as pd


ENCODING = 'latin1'

# Get CSV files lists from a folder
input_path = '../../data_neo4j/projects/'
files = glob.glob(input_path + '*.csv')
cols_to_read = ['APPLICATION_ID' , 'APPLICATION_TYPE', 'CORE_PROJECT_NUM', 'FY', 'IC_NAME',
                'ORG_NAME', 'ORG_STATE', 'PHR', 'PI_IDS', 'PI_NAMEs', 
                'PROJECT_TERMS', 'PROJECT_TITLE', 'SUBPROJECT_ID', 'TOTAL_COST', 'TOTAL_COST_SUB_PROJECT']

# Build Agent names lookup dictionary
agents = pd.read_csv('agent_names.csv')
agent_lkup = dict(zip(agents['IC_NAME_OLD'], agents['IC_NAME_NEW']))

# Clean all files
output_path = '../../data_neo4j/projects/'

for file in files:
    # Clean Agent names
    app = pd.read_csv(file, usecols=cols_to_read, encoding=ENCODING, low_memory=False)
    app['IC_NAME'] = app['IC_NAME'].fillna('Unknown')
    app['IC_NAME'] = app['IC_NAME'].map(agent_lkup)
    
    # Combine TOTAL_COST and TOTAL_COST_SUB_PROJECT
    app.loc[app['TOTAL_COST'].isnull(), 'TOTAL_COST'] = app['TOTAL_COST_SUB_PROJECT']
    app.drop(columns=['TOTAL_COST_SUB_PROJECT'], inplace=True)

    output_file = output_path + "RD_PROJECTS_" + file[38:42] + '.csv'
    app.to_csv(output_file, index=False)
    print('Finished', output_file)

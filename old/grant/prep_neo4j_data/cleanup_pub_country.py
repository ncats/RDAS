import glob
import pandas as pd


ENCODING = 'latin1'

# Get CSV files lists from a folder
input_path = '../../data_neo4j/publications/'
files = glob.glob(input_path + '*.csv')

# Build country lookup dictionary
countries = pd.read_csv('countries.csv')
country_lkup = dict(zip(countries['COUNTRY_OLD'], countries['COUNTRY_NEW']))

# Clean all files
output_path = '../../data_neo4j/publications/'
for file in files:
    pub = pd.read_csv(file, encoding=ENCODING, low_memory=False)
    pub['COUNTRY'] = pub['COUNTRY'].fillna('Unknown')
    pub['COUNTRY'] = pub['COUNTRY'].map(country_lkup)

    output_file = output_path + "RD_PUB_" + file[37:41] + '.csv'
    pub.to_csv(output_file, index=False)
    print('Finished', output_file)

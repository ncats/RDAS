import glob
import pandas as pd


ENCODING = 'latin1'

# Get CSV files lists from a folder
input_path = '../../data_neo4j/annotation_files/'
files = glob.glob(input_path + '*.csv')
cols_to_read = ['APPLICATION_ID' , 'SOURCE']

# Clean all files
output_path = '../../data_neo4j/annotation_files/'

for file in files:
    app = pd.read_csv(file, usecols=cols_to_read, encoding=ENCODING, )
    output_file = output_path + "RD_ANNOTATE_SRC_" + file[50:54] + '.csv'
    app.to_csv(output_file, index=False)
    print('Finished', output_file)

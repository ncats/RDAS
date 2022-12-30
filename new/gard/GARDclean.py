import pandas as pd

df = pd.read_csv("C:\\Users\\shanm\\OneDrive - Axle Informatics\\Axle Informatics\\Alert\\alert\\new\\gard\\GARD_disease_list.csv", index_col=False, encoding='latin-1')
print(df)
df.rename(columns = {'ï»¿GardID':'GardID'}, inplace = True)
print(df)
r,c = df.shape
print(df.info())
for i in range(r):
    row = df.iloc[i]
    gard = str(row['GardID'])
    length = len(gard)
    missing = 7 - length

    gard = 'GARD:' + ('0' * missing) + gard
    print(gard) 

    df.at[i,'GardID'] = gard

df.to_csv('GARD.csv', index=False)
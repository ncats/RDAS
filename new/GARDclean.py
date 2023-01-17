import pandas as pd

df = pd.read_csv("GARD.csv", index_col=False, encoding='latin-1')
r,c = df.shape

for i in range(r):
    row = df.iloc[i]
    gard = str(row['GardID'])
    length = len(gard)
    missing = 7 - length

    gard = 'GARD:' + ('0' * missing) + gard
    print(gard) 

    df.at[i,'GardID'] = gard

df.to_csv('GARD.csv', index=False)
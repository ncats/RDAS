import pandas as pd
import ast

term_df = pd.read_csv('/home/leadmandj/RDAS/RDAS_CTKG_REMAKE/acronym_test_expansion_term.csv', index_col=False)
concept_df = pd.read_csv('/home/leadmandj/RDAS/RDAS_CTKG_REMAKE/acronym_test_expansion_concept.csv', index_col=False)
none_df = pd.read_csv('/home/leadmandj/RDAS/RDAS_CTKG_REMAKE/acronym_test_expansion_none.csv', index_col=False)

def compare (df1,df2):
    r,c = df1.shape
    for idx in range(r):
        greaterVal = None
        compare_lst = None
        row1 = df1.iloc[idx]
        row2 = df2.iloc[idx]

        gard1 = row1['GARD']
        gard2 = row2['GARD']

        terms1 = row1['ORIG_TERMS']
        terms2 = row2['ORIG_TERMS']

        trials1 = ast.literal_eval(row1['FILTERED_TRIALS'])[0]
        trials2 = ast.literal_eval(row2['FILTERED_TRIALS'])[0]

        if len(trials1) > len(trials2):
            greaterVal = 'term'
            compare_lst = list(set(trials1) - set(trials2))
        elif len(trials2) > len(trials1):
            greaterVal = 'concept'
            compare_lst = list(set(trials2) - set(trials1))
        else:
            greaterVal = 'same size'
            compare_lst = list(set(trials2) - set(trials1))

        print(gard1, idx)
        print(terms1)
        print(compare_lst)
        print(greaterVal)

        df2.at[idx,'isLarger'] = greaterVal
        df2.at[idx,'NEW_TRIALS'] = str(compare_lst)

        print('|||||||||||||||||')

    print('----------------')
    df2.to_csv('/home/leadmandj/RDAS/RDAS_CTKG_REMAKE/acronym_test_expansion_concept_new_trials.csv', index=False)


compare(term_df, concept_df)
print('----------------------------')
#compare(term_df, none_df)
import spacy
from AlertCypher import AlertCypher
import html
import requests
import pandas as pd
from spacy.matcher import Matcher
import re

def drug_normalize(drug):
    print(drug)
    new_val = drug.encode("ascii", "ignore")
    updated_str = new_val.decode()
    updated_str = re.sub('\W+',' ', updated_str)
    print(updated_str)
    return updated_str

def create_drug_connection(rxdata):
    cui = rxdata['RxCUI']
    return cui

def get_rxnorm_data(drug):
    rq = 'https://rxnav.nlm.nih.gov/REST/rxcui.json?name={drug}&search=2'.format(drug=drug)
    response = requests.get(rq)
    try:
        rxdata = dict()
        response = response.json()['idGroup']['rxnormId'][0]
        rxdata['RxNormID'] = response

        rq2 = 'https://rxnav.nlm.nih.gov/REST/rxcui/{rxnormid}/allProperties.json?prop=codes+attributes+names+sources'.format(rxnormid=response)
        response = requests.get(rq2)
        response = response.json()['propConceptGroup']['propConcept']

        for r in response:
            if r['propName'] in rxdata:
                rxdata[r['propName']].append(r['propValue'])
            else:
                rxdata[r['propName']] = [r['propValue']]

        return rxdata

    except KeyError as e:
        return
    except ValueError as e:
        print('ERROR')
        print(drug)
        return


def nlp_to_drug(doc,matches,drug_name):
    for match_id, start, end in matches:
        span = doc[start:end].text
        rxdata = get_rxnorm_data(span.replace(' ','+'))

        if rxdata:
            return create_drug_connection(rxdata)
        else:
            print('Map to RxNorm failed for intervention name: {drug_name}'.format(drug_name=drug_name))

def rxnorm_map(df):
    df['RxCUI'] = None

    print('Starting RxNorm data mapping to Drug Interventions')
    nlp = spacy.load('en_ner_bc5cdr_md')
    pattern = [{'ENT_TYPE':'CHEMICAL'}]
    matcher = Matcher(nlp.vocab)
    matcher.add('DRUG',[pattern])

    r,c = df.shape
    for index,row in df.iterrows():
        drug = row['Order_Name']
        drug = drug_normalize(drug)
        drug_url = drug.replace(' ','+')
        rxdata = get_rxnorm_data(drug_url)
        print(drug)
        if rxdata:
            cui = create_drug_connection(rxdata)
        else:
            doc = nlp(drug)
            matches = matcher(doc)
            cui = nlp_to_drug(doc,matches,drug)

        if cui:
            df.at[index,'RxCUI'] = cui[0]

    return df
        
df = pd.read_csv('/home/leadmandj/github/alert/scripts/output_orders.csv',index_col=False)
df = rxnorm_map(df)
df.to_csv('/home/leadmandj/github/alert/scripts/output_orders_mapped.csv',index=False)

import os
import sys
import requests
workspace = os.path.dirname(os.path.abspath(__file__))
sys.path.append(workspace)
sys.path.append('/home/leadmandj/RDAS/')
import sysvars
from AlertCypher import AlertCypher
from bs4 import BeautifulSoup
#import RDAS_CTKG.methods as rdas
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from RDAS_CTKG.src import data_model as dm
from datetime import date,datetime
import re
import nltk
import pandas as pd
from time import sleep
from spacy.matcher import Matcher
from nltk.corpus import words as nltk_words
import spacy
import string
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
nltk.download('averaged_perceptron_tagger')

'''
def is_acronym(words):
    """
    Checks if a word is an acronym.
    Args:
        word (str): The word to be checked.

    Returns:
        bool: True if the word is an acronym, False otherwise.

    Example:
        result = is_acronym("NASA")
        print(result)  # Output: True
    """
    if len(words.split()) > 1: return False

    for word in words.split():
        # Check if the word follows the pattern of an acronym
        if bool(re.match(r'\w*[A-Z]\w*', word[:len(word)-1])) and (word[len(word)-1].isupper() or word[len(word)-1].isnumeric()): # aGG2
            print('ACRONYM REMOVED::', words)
            return True
    return False
'''

def get_full_studies(nctid_dict):
    for term,nctids in nctid_dict.items():
        for nctid in nctids:
            query = f'https://clinicaltrials.gov/api/v2/studies/{nctid}'
            response = requests.get(query)

            try:
                response_txt = response.json()
            except Exception:
                response_txt = None
            #response_txt = parse_trial_fields(response_txt)
            yield [term, response_txt]


def call_get_nctids (query, pageToken=None):
    try:
        if pageToken: query += f'&pageToken={pageToken}'
        response = requests.get(query)
        response_txt = response.json()
    except Exception as e:
        print('Unable to Process Query')
        response_txt = None
    return response_txt

def get_nctids(names, lastupdate):
    all_trials = dict()
    for name in names:
        trials = list()
        name = name.replace('"','\"')

        initial_query = f'https://clinicaltrials.gov/api/v2/studies?query.cond=(EXPANSION[Term]{name} OR AREA[DetailedDescription]EXPANSION[Term]{name} OR AREA[BriefSummary]EXPANSION[Term]{name}) AND AREA[LastUpdatePostDate]RANGE[{lastupdate},MAX]&fields=NCTId&pageSize=1000&countTotal=true'
        #https://clinicaltrials.gov/api/v2/studies?query.cond=(EXPANSION[Concept]{name} OR AREA[DetailedDescription]EXPANSION[Concept]{name} OR AREA[BriefSummary]EXPANSION[Concept]{name}) AND AREA[LastUpdatePostDate]RANGE[{lastupdate},MAX]&fields=NCTId&pageSize=1000&countTotal=true'
        print(initial_query)
        try:
            pageToken = None
            while True:
                response_txt = call_get_nctids(initial_query, pageToken=pageToken)
                if response_txt:
                    trials_list = response_txt['studies']
                    
                    for trial in trials_list:
                        nctid = trial['protocolSection']['identificationModule']['nctId']
                        trials.append(nctid)
                    all_trials[name] = trials
                    if not 'nextPageToken' in response_txt:
                        break
                    else:
                        pageToken = response_txt['nextPageToken']
                else:
                    break
        
        except Exception as e:
            print(e)

    all_trials = {k:list(set(v)) for k,v in all_trials.items()}
    return all_trials


def get_nctids2(names, lastupdate):
    all_trials = list()
    for name in names:
        trials = list()
        name = name.replace('"','\"')

        initial_query = f'https://clinicaltrials.gov/api/v2/studies?query.cond=(EXPANSION[Term]{name} OR AREA[DetailedDescription]EXPANSION[Term]{name} OR AREA[BriefSummary]EXPANSION[Term]{name}) AND AREA[LastUpdatePostDate]RANGE[{lastupdate},MAX]&fields=NCTId&pageSize=1000&countTotal=true'
        #https://clinicaltrials.gov/api/v2/studies?query.cond=(EXPANSION[Concept]{name} OR AREA[DetailedDescription]EXPANSION[Concept]{name} OR AREA[BriefSummary]EXPANSION[Concept]{name}) AND AREA[LastUpdatePostDate]RANGE[{lastupdate},MAX]&fields=NCTId&pageSize=1000&countTotal=true'
        print(initial_query)
        try:
            pageToken = None
            while True:
                response_txt = call_get_nctids(initial_query, pageToken=pageToken)
                if response_txt:
                    trials_list = response_txt['studies']
                    
                    for trial in trials_list:
                        nctid = trial['protocolSection']['identificationModule']['nctId']
                        trials.append(nctid)
                    all_trials += trials
                    if not 'nextPageToken' in response_txt:
                        break
                    else:
                        pageToken = response_txt['nextPageToken']
                else:
                    break
        
        except Exception as e:
            print(e)

    return list(set(all_trials))


def rxnorm_map(nlp, intervention):
    def cypher_Drug(rxdata,intervention_name,wspacy=False):
        rxnormid = rxdata['RxNormID']

        # Create or merge Drug node with RxNormID
        query = 'MERGE (x:Drug {{RxNormID: {rxnormid} }}) WITH x MATCH (y:Intervention {{InterventionName: \"{intervention_name}\" }}) MERGE (y)-[:mapped_to_rxnorm {{WITH_SPACY: {wspacy} }}]->(x)'.format(rxnormid=rxnormid, intervention_name=intervention_name, wspacy=wspacy)
        yield query

        # Set additional properties on the Drug node
        for k,v in rxdata.items():
            key = k.replace(' ','')
            query = ('MATCH (y:Drug {{RxNormID: {rxnormid} }}) SET y.{key} = {value}'.format(rxnormid=rxnormid, key=key, value=v))
            yield query

    def nlp_to_drug(doc,matches,drug_name):
        for match_id, start, end in matches:
            span = doc[start:end].text

            # Retrieve RxNorm data for the drug name
            rxdata = get_rxnorm_data(span.replace(' ','+'))

            if rxdata:
                # Create connections in the database using RxNorm data
                for query in cypher_Drug(rxdata,drug_name,wspacy=True): yield query
            else:
                print('Map to RxNorm failed for intervention name: {drug_name}'.format(drug_name=drug_name))

    def get_rxnorm_data(url):
        # Form RxNav API request to get RxNormID based on drug name
        rq = 'https://rxnav.nlm.nih.gov/REST/rxcui.json?name={drug}&search=2'.format(drug=url)
        response = requests.get(rq)
        try:
            rxdata = dict()
            # Extract RxNormID from the response
            response = response.json()['idGroup']['rxnormId'][0]
            rxdata['RxNormID'] = response
            # Form RxNav API request to get all properties of the drug using RxNormID
            rq2 = 'https://rxnav.nlm.nih.gov/REST/rxcui/{rxnormid}/allProperties.json?prop=codes+attributes+names+sources'.format(rxnormid=response)
            response = requests.get(rq2)
            response = response.json()['propConceptGroup']['propConcept']
            # Extract and organize properties of the drug
            for r in response:
                if r['propName'] in rxdata:
                    rxdata[r['propName']].append(r['propValue'])
                else:
                    rxdata[r['propName']] = [r['propValue']]
            return rxdata
        
        except Exception as e:
            return

    def drug_normalize(drug_name):
        # Remove non-ASCII characters
        new_val = drug_name.encode("ascii", "ignore")
        # Decode the bytes to string
        updated_str = new_val.decode()
        # Replace non-word characters with spaces
        updated_str = re.sub('\W+',' ', updated_str)
        return updated_str

    drug = drug_normalize(intervention)
    drug_url = drug.replace(' ','+')

    # Retrieve RxNorm data for the drug name
    rxdata = get_rxnorm_data(drug_url)

    if rxdata:
        # Create connections in the database using RxNorm data
        for query in cypher_Drug(rxdata, drug): yield query
    else:
        # If RxNorm data not found, use SpaCy NLP to detect drug names and map to RxNorm
        doc = nlp(drug)
        matches = matcher(doc)
        for query in nlp_to_drug(doc,matches,drug): yield query

def clean_data_extract(data):
    temp = data
    for k,v in data.items():
        if v == str() or v == list() or v == dict():
            temp[k] = '\"\"'
        elif type(v) == str:
            text = re.sub(r'[^\w\s\-\/@.+]+', '', v)
            temp[k] = f'\"{text}\"'
        else:
            temp[k] = v
    return temp


def cypher_GARD_populate():
    gard_response = gard_db.run('MATCH (x:GARD) RETURN x.GardId as gid, x.GardName as gname, x.Synonyms as syns').data()
    for response in gard_response:
        name = response['gname']
        gid = response['gid']
        syns = response['syns']

        gard_node = {'GardId':gid, 'GardName':name, 'Synonyms':syns}
        gard_query = cypher_GARD(gard_node)
        db.run(gard_query)


def cypher_GARD(gard_node):
    gardid = gard_node['GardId']
    gardname = gard_node['GardName']
    syns = gard_node['Synonyms']

    query = """
    MERGE (x:GARD {{GardId: \"{gardid}\"}})
    ON CREATE
    SET x.GardName = \"{gardname}\"
    SET x.Synonyms = {syns}
    """.format(
        gardid=gardid,
        gardname=gardname,
        syns=syns
    )

    return query

def cypher_ClinicalTrial(db, study, gard_node, today, term_matched, update=False):
    data_extract = dict()
    #gardid = gard_node['GardId']
    identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    status_module = study.get('protocolSection',dict()).get('statusModule',dict())
    description_module = study.get('protocolSection',dict()).get('descriptionModule',dict())
    design_module = study.get('protocolSection',dict()).get('designModule',dict())
    ipd_module = study.get('protocolSection',dict()).get('ipdSharingStatementModule',dict())
    contact_module = study.get('protocolSection',dict()).get('contactsLocationsModule',dict())

    if status_module == dict() and description_module == dict() and design_module == dict() and ipd_module == dict() and contact_module == dict():
        return None

    # Identification Module
    nctid = identification_module.get('nctId', '')
    data_extract['NCTIdAlias'] = identification_module.get('nctIdAliases', list())
    data_extract['Acronym'] = identification_module.get('acronym', '')
    data_extract['BriefTitle'] = identification_module.get('briefTitle', '')
    data_extract['OfficialTitle'] = identification_module.get('officialTitle', '')

    # Status Module
    data_extract['LastKnownStatus'] = status_module.get('lastKnownStatus','')
    data_extract['CompletionDate'] = status_module.get('completionDateStruct', dict()).get('date', '')
    data_extract['CompletionDateType'] = status_module.get('completionDateStruct', dict()).get('type', '')
    data_extract['LastUpdatePostDate'] = status_module.get('lastUpdatePostDateStruct', dict()).get('date', '')
    data_extract['LastUpdatePostDateType'] = status_module.get('lastUpdatePostDateStruct', dict()).get('type', '')
    data_extract['LastUpdateSubmitDate'] = status_module.get('lastUpdateSubmitDate', '')
    data_extract['OverallStatus'] = status_module.get('overallStatus', '')
    data_extract['PrimaryCompletionDate'] = status_module.get('completionDateStruct', dict()).get('date', '')
    data_extract['PrimaryCompletionDateType'] = status_module.get('completionDateStruct', dict()).get('type', '')
    data_extract['ResultsFirstPostDate'] = status_module.get('studyFirstPostDateStruct', dict()).get('date', '')
    data_extract['ResultsFirstPostDateType'] = status_module.get('studyFirstPostDateStruct', dict()).get('type', '')
    data_extract['ResultsFirstPostedQCCommentsDate'] = status_module.get('studyFirstSubmitQcDate', '')
    data_extract['StartDate'] = status_module.get('startDateStruct', dict()).get('date', '')
    data_extract['StartDateType'] = status_module.get('startDateStruct', dict()).get('type', '')
    data_extract['StudyFirstPostDate'] = status_module.get('studyFirstPostDateStruct', dict()).get('date', '')
    data_extract['StudyFirstPostDateType'] = status_module.get('studyFirstPostDateStruct', dict()).get('type', '')

    # Description Module
    data_extract['BriefSummary'] = description_module.get('briefSummary', '')

    # Design Module
    phase_parse = design_module.get('phases', list())
    if not phase_parse or phase_parse == "":
        phase_parse = "NA"
    else:
        phase_parse = ",".join(phase_parse)

    data_extract['Phase'] = phase_parse
    data_extract['StudyType'] = design_module.get('studyType','')
    data_extract['PatientRegistry'] = design_module.get('patientRegistry',False)

    # IPD Module
    data_extract['IPDSharing'] = ipd_module.get('ipdSharing','')
    data_extract['IPDSharingDescription'] = ipd_module.get('description','')
    data_extract['IPDSharingInfoType'] = ipd_module.get('infoTypes',list())
    data_extract['IPDSharingTimeFrame'] = ipd_module.get('timeFrame','')
    data_extract['IPDSharingAccessCriteria'] = ipd_module.get('accessCriteria','')

    # Remove existing relationships if CT node already exists and requires an update
    if update: db.run(f'MATCH (x:ClinicalTrial)-[r]-() WHERE x.NCTId = \"{nctid}\" AND NOT TYPE(r) = \"mapped_to_gard\" DELETE r')

    data_extract = clean_data_extract(data_extract)

    query = """
    MATCH (x:GARD) WHERE x.GardId = \"{gardid}\"
    MERGE (y:ClinicalTrial {{NCTId: \"{nctid}\"}})
    ON CREATE
    SET y.StudyType = {studyType},
    y.LastKnownStatus = {lknownstat},
    y.NCTIdAlias = {nct_alias},
    y.Acronym = {acro},
    y.BriefTitle = {btitle},
    y.BriefSummary = {bsummary},
    y.OfficialTitle = {otitle},
    y.CompletionDate = {cdate},
    y.CompletionDateType = {cdatetype},
    y.LastUpdatePostDate = {lupdatedate},
    y.LastUpdatePostDateType = {lupdatetype},
    y.LastUpdateSubmitDate = {lupdatesubmitdate},
    y.OverallStatus = {overall},
    y.PrimaryCompletionDate = {pcompletedate},
    y.PrimaryCompletionDateType = {pcompletetype},
    y.ResultsFirstPostDate = {rfirstdate},
    y.ResultsFirstPostDateType = {rfirsttype},
    y.ResultsFirstPostedQCCommentsDate = {qcdate},
    y.StartDate = {startdate},
    y.LastUpdatedRDAS = \"{curdate}\",
    y.DateCreatedRDAS = \"{curdate}\",
    y.IPDSharing = {ipd},
    y.IPDSharingDescription = {desc}, 
    y.IPDSharingInfoType = {info},
    y.IPDSharingTimeFrame = {frame},
    y.IPDSharingAccessCriteria = {criteria},
    y.PatientRegistry = {register},
    y.StartDateType = {startdatetype}
    ON MATCH
    SET y.StudyType = {studyType},
    y.LastKnownStatus = {lknownstat},
    y.NCTIdAlias = {nct_alias},
    y.Acronym = {acro},
    y.BriefTitle = {btitle},
    y.BriefSummary = {bsummary},
    y.OfficialTitle = {otitle},
    y.CompletionDate = {cdate},
    y.Phase = {phases},
    y.CompletionDateType = {cdatetype},
    y.LastUpdatePostDate = {lupdatedate},
    y.LastUpdatePostDateType = {lupdatetype},
    y.LastUpdateSubmitDate = {lupdatesubmitdate},
    y.OverallStatus = {overall},
    y.PrimaryCompletionDate = {pcompletedate},
    y.PrimaryCompletionDateType = {pcompletetype},
    y.ResultsFirstPostDate = {rfirstdate},
    y.ResultsFirstPostDateType = {rfirsttype},
    y.ResultsFirstPostedQCCommentsDate = {qcdate},
    y.StartDate = {startdate},
    y.LastUpdatedRDAS = \"{curdate}\",
    y.IPDSharing = {ipd},
    y.IPDSharingDescription = {desc}, 
    y.IPDSharingInfoType = {info},
    y.IPDSharingTimeFrame = {frame},
    y.IPDSharingAccessCriteria = {criteria},
    y.PatientRegistry = {register},
    y.StartDateType = {startdatetype}
    MERGE (x)<-[:mapped_to_gard {{MatchedTermRDAS: \"{tmatched}\"}}]-(y)
    RETURN ID(y) AS ct_id
    """.format(
    studyType = data_extract['StudyType'],
    lknownstat=data_extract['LastKnownStatus'],
    nct_alias=data_extract['NCTIdAlias'],
    acro=data_extract['Acronym'],
    gardid=gard_node['GardId'], 
    nctid=nctid,
    tmatched = term_matched,
    btitle=data_extract['BriefTitle'],
    bsummary=data_extract['BriefSummary'],
    phases=data_extract['Phase'],
    otitle=data_extract['OfficialTitle'],
    cdate=data_extract['CompletionDate'],
    cdatetype=data_extract['CompletionDateType'],
    lupdatedate=data_extract['LastUpdatePostDate'],
    lupdatetype=data_extract['LastUpdatePostDateType'],
    lupdatesubmitdate=data_extract['LastUpdateSubmitDate'],
    overall=data_extract['OverallStatus'],
    pcompletedate=data_extract['PrimaryCompletionDate'],
    pcompletetype=data_extract['PrimaryCompletionDateType'],
    rfirstdate=data_extract['ResultsFirstPostDate'],
    rfirsttype=data_extract['ResultsFirstPostDateType'],
    qcdate=data_extract['ResultsFirstPostedQCCommentsDate'],
    startdate=data_extract['StartDate'],
    startdatetype=data_extract['StartDateType'],
    ipd=data_extract['IPDSharing'],
    desc=data_extract['IPDSharingDescription'],
    info=data_extract['IPDSharingInfoType'],
    frame=data_extract['IPDSharingTimeFrame'],
    criteria=data_extract['IPDSharingAccessCriteria'],
    register=data_extract['PatientRegistry'],
    curdate=today
    )

    ct_id = db.run(query).data()[0]['ct_id']

    centralContacts = contact_module.get('centralContacts',list())
    if centralContacts == list(): return None

    for contact in centralContacts:
        data_extract['ContactName'] = contact.get('name','')
        data_extract['ContactRole'] = contact.get('role','')
        data_extract['ContactPhone'] = contact.get('phone','')
        data_extract['ContactPhoneExt'] = contact.get('phoneExt','')
        data_extract['ContactEmail'] = contact.get('email','')

        data_extract = clean_data_extract(data_extract)

        query = """
        MATCH (x:ClinicalTrial) WHERE ID(x) = {ct_id}
        MERGE (y:Contact {{
            ContactName: {name},
            ContactRole: {role},
            ContactPhone: {phone},
            ContactPhoneExt: {phoneExt},
            ContactEmail: {email},
            ContactScope: \"Central\"
            }})
        MERGE (x)-[:has_contact]->(y)
        """.format(
        ct_id=ct_id,
        name=data_extract['ContactName'],
        role=data_extract['ContactRole'],
        phone=data_extract['ContactPhone'],
        phoneExt=data_extract['ContactPhoneExt'],
        email=data_extract['ContactEmail']
        )

        yield query

def cypher_IndividualPatientData(study):
    data_extract = dict()

    identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    ipd_module = study.get('protocolSection',dict()).get('ipdSharingStatementModule',dict())

    if ipd_module == dict():
        return None
    
    nctid = identification_module.get('nctId', '')

    data_extract['IPDSharing'] = ipd_module.get('ipdSharing','')
    data_extract['IPDSharingDescription'] = ipd_module.get('description','')
    data_extract['IPDSharingInfoType'] = ipd_module.get('infoTypes',list())
    data_extract['IPDSharingTimeFrame'] = ipd_module.get('timeFrame','')
    data_extract['IPDSharingAccessCriteria'] = ipd_module.get('accessCriteria','')

    data_extract = clean_data_extract(data_extract)

    query = """
    MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\"
    MERGE (y:IndividualPatientData {{
        IPDSharing: {ipd},
        IPDSharingDescription: {desc}, 
        IPDSharingInfoType: {info},
        IPDSharingTimeFrame: {frame},
        IPDSharingAccessCriteria: {criteria}
        }})
    MERGE (x)-[:has_individual_patient_data]->(y)
    """.format(
    nctid=nctid,
    ipd=data_extract['IPDSharing'],
    desc=data_extract['IPDSharingDescription'],
    info=data_extract['IPDSharingInfoType'],
    frame=data_extract['IPDSharingTimeFrame'],
    criteria=data_extract['IPDSharingAccessCriteria'],
    )

    return query


def cypher_Investigator(study):
    identification_module = study.get('protocolSection', dict()).get('identificationModule', dict())
    contact_module = study.get('protocolSection', dict()).get('contactsLocationsModule', dict())
    
    nctid = identification_module.get('nctId', '')
    officials = contact_module.get('overallOfficials', list())

    if officials == list():
        return None

    for official in officials:
        data_extract = dict()
        data_extract['OfficialName'] = official.get('name','')
        data_extract['OfficialAffiliation'] = official.get('affiliation','')
        data_extract['OfficialRole'] = official.get('role','')

        data_extract = clean_data_extract(data_extract)

        query = """
        MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\"
        MATCH (z:Contact) WHERE z.ContactName = {official_name}
        MERGE (y:Investigator {{
            OfficialName: {official_name},
            OfficialAffiliation: {aff},
            OfficialRole: {role} }})
        MERGE (x)<-[:investigates]-(y)
        MERGE (z)<-[:has_contact]-(y)
        """.format(
        nctid=nctid,
        official_name=data_extract['OfficialName'],
        aff=data_extract['OfficialAffiliation'],
        role=data_extract['OfficialRole']
        )

        yield query


def cypher_Condition(db, study, gard_names_dict):
    identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    conditions_module = study.get('protocolSection',dict()).get('conditionsModule',dict())
    
    nctid = identification_module.get('nctId', '')

    conditions = conditions_module.get('conditions', list())

    if conditions == list():
        return None

    for condition in conditions:
        condition_normalized = gard_text_normalize(condition)
        query = """
        MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\"
        MERGE (y:Condition {{Condition: \"{cond}\"}})
        MERGE (x)-[:investigates_condition]->(y)
        RETURN ID(y) AS cond_id
        """.format(
        nctid=nctid,
        cond=condition_normalized,
        )
        cond_id = db.run(query).data()[0]['cond_id']

        # Exact match with GARD node
        #condition_normalized = gard_text_normalize(condition)
        for k,v in gard_names_dict.items():
            for term in v:
                if condition_normalized == term:
                    print('MATCH::', condition_normalized)
                    query = """
                    MATCH (x:GARD) WHERE x.GardId = \"{gardid}\"
                    MATCH (y:Condition) WHERE ID(y) = {cond_id}
                    MERGE (y)-[:mapped_to_gard]->(x)
                    """.format(
                    gardid=k,
                    cond_id=cond_id
                    )
                    yield query
        ###

def cypher_AssociatedEntity(study):
    def generate_entity_query(nctid, data, node_type):
        data_extract = clean_data_extract(data)

        query = """
        MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\"
        MERGE (y:AssociatedEntity {{EntityName: {ename}, EntityClass: {eclass}, EntityType: \"{etype}\" }})
        MERGE (x)<-[:associated_with]-(y)
        """.format(
        nctid=nctid,
        ename=data_extract['Name'],
        eclass=data_extract['Class'],
        etype=node_type
        )

        return query

    identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    collab_module = study.get('protocolSection',dict()).get('sponsorCollaboratorsModule',dict())

    nctid = identification_module.get('nctId', '')
    organization = identification_module.get('organization', dict())
    collaborators = collab_module.get('collaborators', list())
    leadSponsor = collab_module.get('leadSponsor',dict())

    if not organization == dict():
        data_extract = dict()
        data_extract['Name'] = identification_module.get('organization',dict()).get('fullName','')
        data_extract['Class'] = identification_module.get('organization',dict()).get('class','')
        yield generate_entity_query(nctid, data_extract, 'Organization')

    if not leadSponsor == dict():
        data_extract = dict()
        data_extract['Name'] = leadSponsor.get('name', '')
        data_extract['Class'] = leadSponsor.get('class', '')
        yield generate_entity_query(nctid, data_extract, 'Sponsor')

    if not collaborators == list():
        for collaborator in collaborators: 
            data_extract = dict()
            data_extract['Name'] = collaborator.get('name','')
            data_extract['Class'] = collaborator.get('class','')
            yield generate_entity_query(nctid, data_extract, 'Collaborator')

    

def cypher_StudyDesign(study):
    data_extract = dict()

    identification_module = study.get('protocolSection', dict()).get('identificationModule', dict())
    design_module = study.get('protocolSection', dict()).get('designModule', dict())
    desc_module = study.get('protocolSection', dict()).get('descriptionModule', dict())
    status_module = study.get('protocolSection','').get('statusModule','')
    
    nctid = identification_module.get('nctId', '')

    designInfo = design_module.get('designInfo', dict())
    data_extract['DesignObservationalModel'] = designInfo.get('observationalModel','')
    data_extract['DesignTimePerspective'] = designInfo.get('timePerspective','')
    data_extract['DesignAllocation'] = designInfo.get('allocation','')
    data_extract['DesignInterventionModel'] = designInfo.get('interventionModel','')
    data_extract['DesignPrimaryPurpose'] = designInfo.get('primaryPurpose','')
    data_extract['DesignInterventionModel'] = designInfo.get('interventionModel','')
    data_extract['DesignInterventionModelDescription'] = designInfo.get('interventionModelDescription','')

    maskingInfo = designInfo.get('maskingInfo',dict())
    data_extract['DesignMasking'] = maskingInfo.get('masking','')

    expandedAccessInfo = status_module.get('expandedAccessInfo',dict())
    data_extract['HasExpandedAccess'] = expandedAccessInfo.get('hasExpandedAccess','')

    data_extract['DetailedDescription'] = desc_module.get('detailedDescription','')

    if designInfo == dict() and maskingInfo == dict() and expandedAccessInfo == dict():
        return None
    
    data_extract = clean_data_extract(data_extract)

    query = """
    MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\"
    MERGE (y:StudyDesign {{
        DesignObservationalModel: {observe},
        DesignInterventionModel: {int_model},
        DesignInterventionModelDescription: {int_desc},
        DesignTimePerspective: {persp}, 
        DesignAllocation: {alloc},
        DesignPrimaryPurpose: {purp},
        DesignInterventionModel: {intervention},
        DesignMasking: {mask},
        DetailedDescription: {det_desc},
        HasExpandedAccess: {access}
        }})
    MERGE (x)-[:has_study_design]->(y)
    """.format(
    nctid=nctid,
    observe=data_extract['DesignObservationalModel'],
    persp=data_extract['DesignTimePerspective'],
    alloc=data_extract['DesignAllocation'],
    purp=data_extract['DesignPrimaryPurpose'],
    intervention=data_extract['DesignInterventionModel'],
    mask=data_extract['DesignMasking'],
    int_model=data_extract['DesignInterventionModel'],
    int_desc=data_extract['DesignInterventionModelDescription'],
    det_desc=data_extract['DetailedDescription'],
    access=data_extract['HasExpandedAccess']
    )
    
    return query


def cypher_PrimaryOutcome(study):
    identification_module = study.get('protocolSection', dict()).get('identificationModule', dict())
    outcomes_module = study.get('protocolSection', dict()).get('outcomesModule', dict())
    
    nctid = identification_module.get('nctId', '')
    primaryOutcomes = outcomes_module.get('primaryOutcomes', list())

    if primaryOutcomes == list():
        return None

    for outcome in primaryOutcomes:
        data_extract = dict()

        data_extract['PrimaryOutcomeMeasure'] = outcome.get('measure','')
        data_extract['PrimaryOutcomeTimeFrame'] = outcome.get('timeFrame','')
        data_extract['PrimaryOutcomeDescription'] = outcome.get('description','')

        data_extract = clean_data_extract(data_extract)

        query = """
        MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\"
        MERGE (y:PrimaryOutcome {{PrimaryOutcomeMeasure: {measure}, PrimaryOutcomeTimeFrame: {timeframe}, PrimaryOutcomeDescription: {desc}}})
        MERGE (x)-[:has_outcome]->(y)
        """.format(
        nctid=nctid,
        measure=data_extract['PrimaryOutcomeMeasure'],
        timeframe=data_extract['PrimaryOutcomeTimeFrame'],
        desc=data_extract['PrimaryOutcomeDescription']
        )

        yield query


def cypher_Participant(study):
    data_extract = dict()

    identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    design_module = study.get('protocolSection',dict()).get('designModule',dict())
    eligibility_module = study.get('protocolSection',dict()).get('eligibilityModule',dict())
    
    nctid = identification_module.get('nctId', '')

    data_extract['EligibilityCriteria'] = eligibility_module.get('eligibilityCriteria', '')
    data_extract['HealthyVolunteers'] = eligibility_module.get('healthyVolunteers', '')
    data_extract['Gender'] = eligibility_module.get('sex', '')
    data_extract['StdAge'] = eligibility_module.get('stdAges', '')
    data_extract['MinimumAge'] = eligibility_module.get('minimumAge', '')
    data_extract['MaximumAge'] = eligibility_module.get('maximumAge', '')

    data_extract['EnrollmentCount'] = design_module.get('enrollmentInfo', dict()).get('count', '')
    data_extract['EnrollmentType'] = design_module.get('enrollmentInfo', dict()).get('type', '')

    if eligibility_module == dict() and design_module == dict():
        return None
    
    data_extract = clean_data_extract(data_extract)

    query = """
    MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\"
    MERGE (y:Participant {{
        EligibilityCriteria: {criteria},
        HealthyVolunteers: {healthy},
        Gender: {gender},
        StdAge: {age},
        MinimumAge: {minage},
        MaximumAge: {maxage},
        EnrollmentCount: {cnt},
        EnrollmentType: {enrolltype} }})
    MERGE (x)-[:has_participant_info]->(y)
    """.format(
    nctid=nctid,
    criteria=data_extract['EligibilityCriteria'],
    healthy=data_extract['HealthyVolunteers'],
    gender=data_extract['Gender'],
    minage=data_extract['MinimumAge'],
    maxage=data_extract['MaximumAge'],
    age=data_extract['StdAge'],
    cnt=data_extract['EnrollmentCount'],
    enrolltype=data_extract['EnrollmentType'],
    )

    return query


def cypher_Intervention(study, nlp):
    identification_module = study.get('protocolSection', dict()).get('identificationModule', dict())
    intervention_module = study.get('protocolSection', dict()).get('armsInterventionsModule', dict())
    
    nctid = identification_module.get('nctId', '')
    interventions = intervention_module.get('interventions', list())

    if interventions == list():
        return None

    for intervention in interventions:
        data_extract = dict()

        data_extract['InterventionName'] = intervention.get('name','')
        intervention_name = data_extract['InterventionName']
        data_extract['InterventionType'] = intervention.get('type','')
        intervention_type = data_extract['InterventionType']
        data_extract['InterventionDescription'] = intervention.get('description','')

        data_extract = clean_data_extract(data_extract)

        query = """
        MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\"
        MERGE (y:Intervention {{
            InterventionName: {name},
            InterventionType: {type},
            InterventionDescription: {desc} 
            }})
        MERGE (x)-[:has_intervention]->(y)
        """.format(
        nctid=nctid,
        name=data_extract['InterventionName'],
        type=data_extract['InterventionType'],
        desc=data_extract['InterventionDescription']
        )

        yield query

        if intervention_type == 'DRUG':
            for rxnorm_query in rxnorm_map(nlp, intervention_name): yield rxnorm_query


def cypher_Location(db, study):
    identification_module = study.get('protocolSection', dict()).get('identificationModule', dict())
    loc_module = study.get('protocolSection', dict()).get('contactsLocationsModule', dict())
    
    nctid = identification_module.get('nctId', '')
    locations = loc_module.get('locations', list())

    if locations == list():
        return None

    for loc in locations:
        data_extract = dict()

        data_extract['LocationFacility'] = loc.get('facility','')
        data_extract['LocationStatus'] = loc.get('status','')
        data_extract['LocationCity'] = loc.get('city','')
        data_extract['LocationZip'] = loc.get('zip','')
        data_extract['LocationCountry'] = loc.get('country','')
        data_extract['LocationState'] = loc.get('state','')

        data_extract = clean_data_extract(data_extract)

        query = """
        MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\"
        MERGE (y:Location {{
            LocationCity: {city},
            LocationCountry: {country},
            LocationFacility: {facility},
            LocationState: {state},
            LocationStatus: {status},
            LocationZip: {zipcode}
            }})
        MERGE (x)-[:in_locations]->(y)
        RETURN ID(y) AS loc_id, ID(x) as ct_id
        """.format(
        nctid=nctid,
        city=data_extract['LocationCity'],
        country=data_extract['LocationCountry'],
        facility=data_extract['LocationFacility'],
        state=data_extract['LocationState'],
        status=data_extract['LocationStatus'],
        zipcode=data_extract['LocationZip']
        )
        loc_id = db.run(query).data()[0]['loc_id']
        ct_id = db.run(query).data()[0]['ct_id']

        data_extract = dict()
        loc_contacts = loc.get('contacts',list())
        if loc_contacts == list(): continue

        for contact in loc_contacts:
            data_extract['ContactName'] = contact.get('name','')
            data_extract['ContactRole'] = contact.get('role','')
            data_extract['ContactPhone'] = contact.get('phone','')
            data_extract['ContactPhoneExt'] = contact.get('phoneExt','')
            data_extract['ContactEmail'] = contact.get('email','')

            data_extract = clean_data_extract(data_extract)

            query = """
            MATCH (z:ClinicalTrial) WHERE ID(z) = {ct_id}
            MATCH (x:Location) WHERE ID(x) = {loc_id}
            MERGE (y:Contact {{
                ContactName: {name},
                ContactRole: {role},
                ContactPhone: {phone},
                ContactPhoneExt: {phoneExt},
                ContactEmail: {email},
                ContactScope: \"Location\"
                }})
            MERGE (z)-[:has_contact]->(y)
            MERGE (y)-[:contact_for_location]->(x)
            """.format(
            loc_id=loc_id,
            ct_id=ct_id,
            name=data_extract['ContactName'],
            role=data_extract['ContactRole'],
            phone=data_extract['ContactPhone'],
            phoneExt=data_extract['ContactPhoneExt'],
            email=data_extract['ContactEmail']
            )

            yield query


def cypher_Reference(study):
    identification_module = study.get('protocolSection', dict()).get('identificationModule', dict())
    ref_module = study.get('protocolSection', dict()).get('referencesModule', dict())
    
    nctid = identification_module.get('nctId', '')
    refs = ref_module.get('references', list())

    if refs == list():
        return None

    for ref in refs:
        data_extract = dict()

        data_extract['ReferencePMID'] = ref.get('pmid','')
        data_extract['ReferenceType'] = ref.get('type','')
        data_extract['Citation'] = ref.get('citation','')

        data_extract = clean_data_extract(data_extract)

        query = """
        MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{nctid}\"
        MERGE (y:Reference {{
            Citation: {cite},
            ReferencePMID: {pmid},
            ReferenceType: {ref_type}
            }})
        MERGE (x)<-[:is_about]-(y)
        """.format(
        nctid=nctid,
        cite=data_extract['Citation'],
        pmid=data_extract['ReferencePMID'],
        ref_type=data_extract['ReferenceType']
        )

        yield query


def gard_text_normalize(text):
    text = text.lower()
    text = re.sub('\W+',' ', text)
    text = re.sub(' +', ' ', text)
    text = text.strip()

    return text

def is_under_char_threshold(syn):
    if len(syn.split()) == 1:
        if len(syn) < 5:
            print('WORD UNDER CHAR LIMIT::', syn)
            return True
        else:
            return False
    else:
        return False


def is_english(syn):
    tokens = syn.lower().split()
    if len(tokens) == 1:
        if tokens[0] in wordset:
            print('ENGLISH WORD FOUND::', syn)
            return True
        else:
            return False
    else:
        return False

def get_GARD_names_syns(db):
    temp = dict()
    response = db.run('MATCH (x:GARD) RETURN x.GardId as gid, x.GardName as gname, x.Synonyms as syns').data()
    for res in response:
        gardid = res['gid']
        gardname = res['gname']
        gardsyns = res['syns']

        #faconi anemia, face, FACE
        # [face]
        # [FACE]
        # [faconia anemia, face, FACE] - [face]
        # [faconia anemia, face, FACE] - [FACE]

        gardsyns_eng = [syn for syn in gardsyns if is_english(syn)]
        gardsyns_char_threshold = [syn for syn in gardsyns if is_under_char_threshold(syn)]
        filtered_syns = [x for x in gardsyns if not x in gardsyns_eng]
        filtered_syns = [x for x in filtered_syns if not x in gardsyns_char_threshold]

        termlist = [gardname] + filtered_syns

        termlist = [gard_text_normalize(term) for term in termlist]

        temp[gardid] = termlist

    return temp


def generate_queries(db, nlp, study, gard_node, gard_names_dict, today, term_matched, update=False):
    # IF update=True it will remove all relationships from the CT node and recreate the connected nodes
    # IF update=False it assumes the CT node doesnt exist and will create connected nodes normally
    # Extract and populate GARD info
    ###yield cypher_GARD(gard_node)
    # Extract and populate ClinicalTrial info
    for query in cypher_ClinicalTrial(db, study, gard_node, today, term_matched, update=update): yield query
    # Extract AssociatedEntity info
    for query in cypher_AssociatedEntity(study): yield query
    # Extract and populate Location info
    for query in cypher_Location(db, study): yield query
    # Extract and populate Investigator info
    for query in cypher_Investigator(study): yield query
    # Extract and populate Condition info
    for query in cypher_Condition(db, study, gard_names_dict): yield query
    # Extract and populate StudyDesign info
    yield cypher_StudyDesign(study)
    # Extract and populate PrimaryOutcome info
    for query in cypher_PrimaryOutcome(study): yield query
    # Extract and populate Participant info
    yield cypher_Participant(study)
    # Extract and populate Intervention info
    for query in cypher_Intervention(study, nlp): yield query
    # Extract and populate Reference info
    for query in cypher_Reference(study): yield query

###########################################################
#START

# Connect to the Neo4j database
db = AlertCypher(sysvars.ct_db)
gard_db = AlertCypher(sysvars.gard_db)

# Setup NLP for RxNORM Mapping
nlp = spacy.load('en_ner_bc5cdr_md')
pattern = [{'ENT_TYPE':'CHEMICAL'}]
matcher = Matcher(nlp.vocab)
matcher.add('DRUG',[pattern])

# Setup NLTK for english word parsing for synonym filtering
wordset = set(nltk_words.words())

# Get last updated date and current date
today = date.today().strftime('%m/%d/%y')
lastupdate_str = db.getConf('UPDATE_PROGRESS','rdas.ctkg_update')
lastupdate = datetime.strptime(lastupdate_str, "%m/%d/%y")
lastupdate = lastupdate.strftime('%m/%d/%Y')

def start_update():
    print(f"[CT] Database Selected: {sysvars.ct_db}\nContinuing with script in 5 seconds...")
    sleep(5)

    in_progress = db.getConf('UPDATE_PROGRESS', 'clinical_in_progress')
    print(f'in_progress:: {in_progress}')
    if in_progress == 'True':
        clinical_disease_progress = db.getConf('UPDATE_PROGRESS', 'clinical_disease_progress')
        if not clinical_disease_progress == '':
            clinical_disease_progress = int(clinical_disease_progress)
        else:
            clinical_disease_progress = 0

        clinical_rxnorm_progress = db.getConf('UPDATE_PROGRESS', 'clinical_rxnorm_progress')
        if not clinical_rxnorm_progress == '':
            clinical_rxnorm_progress = int(clinical_rxnorm_progress)
        else:
            clinical_rxnorm_progress = 0
        clinical_current_step = db.getConf('UPDATE_PROGRESS', 'clinical_current_step')
    else:
        clinical_disease_progress = 0
        clinical_rxnorm_progress = 0
        clinical_current_step = ''
        db.setConf('UPDATE_PROGRESS', 'clinical_in_progress', 'True')
        cypher_GARD_populate()

    if clinical_current_step == '':
        # Gets list used to exact map Conditions to GARD, normalized and acros and single english words removed
        gard_names_dict = get_GARD_names_syns(gard_db)
        # Gets list used for gettings trials and making nodes, not normalized but acros and single english words removed
        gard_response = gard_db.run('MATCH (x:GARD) RETURN x.GardId as gid, x.GardName as gname, x.Synonyms as syns').data()
        for idx,response in enumerate(gard_response):
            if idx < clinical_disease_progress:
                continue

            name = response['gname']
            gid = response['gid']
            syns = response['syns']

            gard_node = {'GardId':gid, 'GardName':name, 'Synonyms':syns}
            gard_query = cypher_GARD(gard_node)
            db.run(gard_query)

            #names_no_filter = [name] + syns
            gardsyns_eng = [syn for syn in syns if is_english(syn)]
            gardsyns_char_threshold = [syn for syn in syns if is_under_char_threshold(syn)]
            filtered_syns = [x for x in syns if not x in gardsyns_eng]
            filtered_syns = [x for x in filtered_syns if not x in gardsyns_char_threshold]
            names = [name] + filtered_syns

            nctids = get_nctids(names, lastupdate)

            print(str(idx) + f' -------- {name} -------- {gid} --- {len(nctids)} Trials')

            if len(nctids) > 0:
                for term_matched, full_study in get_full_studies(nctids):
                    if full_study:

                        api_nctid = full_study.get('protocolSection',dict()).get('identificationModule',dict()).get('nctId',None)
                        check_for_trial = db.run(f'MATCH (x:ClinicalTrial) WHERE x.NCTId = \"{api_nctid}\" RETURN ID(x) AS trial_id').data()
                        
                        if len(check_for_trial) > 0:
                            # Initiates Node Update
                            node_update = True
                            print('UPDATE TRUE::', api_nctid)
                        else:
                            # Initiates Node Creation
                            node_update = False
                            print('CREATE TRUE::', api_nctid)

                        for query in generate_queries(db, nlp, full_study, gard_node, gard_names_dict, today, term_matched, update=node_update):
                            if query: db.run(query)
                        print('created')
                    else:
                        print('Error in add for finding full trial data')

            db.setConf('UPDATE_PROGRESS', 'clinical_disease_progress', str(idx))
    

import re
import json
import time
import requests
import asyncio
import aiohttp
import spacy
from spacy.matcher import Matcher
# Setup NLP for RxNORM Mapping
nlp = spacy.load('en_ner_bc5cdr_md')
pattern = [{'ENT_TYPE':'CHEMICAL'}]
matcher = Matcher(nlp.vocab)
matcher.add('DRUG',[pattern])

from utils.tools import _is_english, _is_under_char_threshold, _gard_text_normalize, _date_of_days_ago, _clean, _clean_data_extract


#
#
#
# Depracted: see init_ClinicalTrail_all.py
#
#
#

def _add_gard_node(db, node):

        query = f"""
            MERGE (x:GARD {{GardId: "{node['GardId']}"}})
            ON CREATE
            SET x.GardName = "{node['GardName']}"
            SET x.Synonyms = {node['Synonyms']}
        """

        db.run(query)

# Overwrite the old clinical trial GARD node
def _update_gard_node(db, node):

        query = f"""
            MATCH (x:GARD)
            WHERE x.GardId = "{node['GardId']}"
            SET x.GardName = "{node['GardName']}"
            SET x.Synonyms = {node['Synonyms']}
        """
        db.run(query)


def _get_GARD_names_syns(conn):

    temp = dict()
    response = conn.execute_and_fetch('MATCH (x:GARD) RETURN x.GardId AS GardId, x.Name AS GardName, x.Synonyms AS Synonyms')
    for res in response:
        gardid = res['GardId']
        gardname = res['GardName']
        gardsyns = res['Synonyms']

        gardsyns_eng = [syn for syn in gardsyns if _is_english(syn)]
        gardsyns_char_threshold = [syn for syn in gardsyns if _is_under_char_threshold(syn)]
        
        filtered_syns = [x for x in gardsyns if not x in gardsyns_eng]
        filtered_syns = [x for x in filtered_syns if not x in gardsyns_char_threshold]

        termlist = [gardname] + filtered_syns

        termlist = [_gard_text_normalize(term) for term in termlist]

        temp[gardid] = termlist

    return temp



def unwind_init_cypher_create_ClinicalTrial_node(nctid, study):
    
    identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    status_module = study.get('protocolSection',dict()).get('statusModule',dict())
    description_module = study.get('protocolSection',dict()).get('descriptionModule',dict())
    design_module = study.get('protocolSection',dict()).get('designModule',dict())
    ipd_module = study.get('protocolSection',dict()).get('ipdSharingStatementModule',dict())
    contact_module = study.get('protocolSection',dict()).get('contactsLocationsModule',dict())

    if status_module == dict() and description_module == dict() and design_module == dict() and ipd_module == dict() and contact_module == dict():
        return None
 
    #data_extract['StudyFirstPostDate'] = status_module.get('studyFirstPostDateStruct', dict()).get('date', '')
    #data_extract['StudyFirstPostDateType'] = status_module.get('studyFirstPostDateStruct', dict()).get('type', '')
 
    # Design Module
    phase_parse = design_module.get('phases', list())
    if not phase_parse or phase_parse == "":
        phase_parse = "NA"
    else:
        phase_parse = ",".join(phase_parse)  

    today = _date_of_days_ago(0)

    obj = {
        "NCTId": nctid,
        "StudyType": _clean(design_module.get('studyType','')),
        "LastKnownStatus":  _clean(status_module.get('lastKnownStatus','')),
        "NCTIdAlias":  identification_module.get('nctIdAliases', list()),
        "Acronym":  _clean(identification_module.get('acronym', '')),
        "BriefTitle":  _clean(identification_module.get('briefTitle', '')),
        "BriefSummary":  _clean(description_module.get('briefSummary', '')),
        "OfficialTitle":  _clean(identification_module.get('officialTitle', '')),
        "CompletionDate":  _clean(status_module.get('completionDateStruct', dict()).get('date', '')),       
        "CompletionDateType":  _clean(status_module.get('completionDateStruct', dict()).get('type', '')),
        "LastUpdatePostDate":  _clean(status_module.get('lastUpdatePostDateStruct', dict()).get('date', '')),
        "LastUpdatePostDateType":  _clean(status_module.get('lastUpdatePostDateStruct', dict()).get('type', '')),
        "LastUpdateSubmitDate":  _clean(status_module.get('lastUpdateSubmitDate', '')),
        "OverallStatus":  _clean(status_module.get('overallStatus', '')),
        "PrimaryCompletionDate":  _clean(status_module.get('completionDateStruct', dict()).get('date', '')),
        "PrimaryCompletionDateType":  _clean(status_module.get('completionDateStruct', dict()).get('type', '')),
        "ResultsFirstPostDate":  _clean(status_module.get('studyFirstPostDateStruct', dict()).get('date', '')),
        "ResultsFirstPostDateType":  _clean(status_module.get('studyFirstPostDateStruct', dict()).get('type', '')),
        "ResultsFirstPostedQCCommentsDate":  _clean(status_module.get('studyFirstSubmitQcDate', '')),
        "StartDate":  _clean(status_module.get('startDateStruct', dict()).get('date', '')),
         "Phase": _clean(phase_parse),
        "PatientRegistry":  design_module.get('patientRegistry',False),
        "StartDateType":  _clean(status_module.get('startDateStruct', dict()).get('type', '')) ,
        
        "LastUpdatedRDAS": today,
        "DateCreatedRDAS": today      
    }

    ''' # removed from the above 'set_items'
        y.IPDSharing = "{ _clean(ipd_module.get('ipdSharing',''))}",
        y.IPDSharingDescription = "{ _clean(ipd_module.get('description',''))}", 
        y.IPDSharingInfoType = { ipd_module.get('infoTypes',list())},
        y.IPDSharingTimeFrame = "{ _clean(ipd_module.get('timeFrame',''))}",
        y.IPDSharingAccessCriteria = "{ _clean(ipd_module.get('accessCriteria',''))}",
    '''
    return obj



def init_cypher_create_ClinicalTrial_node(nctid, study):
    
    identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    status_module = study.get('protocolSection',dict()).get('statusModule',dict())
    description_module = study.get('protocolSection',dict()).get('descriptionModule',dict())
    design_module = study.get('protocolSection',dict()).get('designModule',dict())
    ipd_module = study.get('protocolSection',dict()).get('ipdSharingStatementModule',dict())
    contact_module = study.get('protocolSection',dict()).get('contactsLocationsModule',dict())

    if status_module == dict() and description_module == dict() and design_module == dict() and ipd_module == dict() and contact_module == dict():
        return None
 
    #data_extract['StudyFirstPostDate'] = status_module.get('studyFirstPostDateStruct', dict()).get('date', '')
    #data_extract['StudyFirstPostDateType'] = status_module.get('studyFirstPostDateStruct', dict()).get('type', '')
 
    # Design Module
    phase_parse = design_module.get('phases', list())
    if not phase_parse or phase_parse == "":
        phase_parse = "NA"
    else:
        phase_parse = ",".join(phase_parse)  

    today = _date_of_days_ago(0)

    set_items = f'''
        y.StudyType = "{ _clean(design_module.get('studyType',''))}",
        y.LastKnownStatus = "{ _clean(status_module.get('lastKnownStatus',''))}",
        y.NCTIdAlias = "{ identification_module.get('nctIdAliases', list())}",
        y.Acronym = "{ _clean(identification_module.get('acronym', ''))}",
        y.BriefTitle = "{ _clean(identification_module.get('briefTitle', ''))}",
        y.BriefSummary = "{ _clean(description_module.get('briefSummary', ''))}",
        y.OfficialTitle = "{ _clean(identification_module.get('officialTitle', ''))}",
        y.CompletionDate = "{ _clean(status_module.get('completionDateStruct', dict()).get('date', ''))}",       
        y.CompletionDateType = "{ _clean(status_module.get('completionDateStruct', dict()).get('type', ''))}",
        y.LastUpdatePostDate = "{ _clean(status_module.get('lastUpdatePostDateStruct', dict()).get('date', ''))}",
        y.LastUpdatePostDateType = "{ _clean(status_module.get('lastUpdatePostDateStruct', dict()).get('type', ''))}",
        y.LastUpdateSubmitDate = "{ _clean(status_module.get('lastUpdateSubmitDate', ''))}",
        y.OverallStatus = "{ _clean(status_module.get('overallStatus', ''))}",
        y.PrimaryCompletionDate = "{ _clean(status_module.get('completionDateStruct', dict()).get('date', ''))}",
        y.PrimaryCompletionDateType = "{ _clean(status_module.get('completionDateStruct', dict()).get('type', ''))}",
        y.ResultsFirstPostDate = "{ _clean(status_module.get('studyFirstPostDateStruct', dict()).get('date', ''))}",
        y.ResultsFirstPostDateType = "{ _clean(status_module.get('studyFirstPostDateStruct', dict()).get('type', ''))}",
        y.ResultsFirstPostedQCCommentsDate = "{ _clean(status_module.get('studyFirstSubmitQcDate', ''))}",
        y.StartDate = "{ _clean(status_module.get('startDateStruct', dict()).get('date', ''))}",
        y.LastUpdatedRDAS = "{today}",
        
        y.PatientRegistry = "{ design_module.get('patientRegistry',False)}",
        y.StartDateType = "{ _clean(status_module.get('startDateStruct', dict()).get('type', ''))}",
    '''

    ''' # removed from the above 'set_items'
        y.IPDSharing = "{ _clean(ipd_module.get('ipdSharing',''))}",
        y.IPDSharingDescription = "{ _clean(ipd_module.get('description',''))}", 
        y.IPDSharingInfoType = { ipd_module.get('infoTypes',list())},
        y.IPDSharingTimeFrame = "{ _clean(ipd_module.get('timeFrame',''))}",
        y.IPDSharingAccessCriteria = "{ _clean(ipd_module.get('accessCriteria',''))}",
    '''

    query = f''' 
        CREATE (y:ClinicalTrial {{NCTId: "{nctid}"}})
        
        SET {set_items},
            y.DateCreatedRDAS = "{today}",
            y.Phase = "{ _clean(phase_parse)}"
        return y
    ''' 
    return query


def cypher_ClinicalTrial(nctid, study, gard_id, term_matched):
    
    identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    status_module = study.get('protocolSection',dict()).get('statusModule',dict())
    description_module = study.get('protocolSection',dict()).get('descriptionModule',dict())
    design_module = study.get('protocolSection',dict()).get('designModule',dict())
    ipd_module = study.get('protocolSection',dict()).get('ipdSharingStatementModule',dict())
    contact_module = study.get('protocolSection',dict()).get('contactsLocationsModule',dict())

    if status_module == dict() and description_module == dict() and design_module == dict() and ipd_module == dict() and contact_module == dict():
        return None
 
    #data_extract['StudyFirstPostDate'] = status_module.get('studyFirstPostDateStruct', dict()).get('date', '')
    #data_extract['StudyFirstPostDateType'] = status_module.get('studyFirstPostDateStruct', dict()).get('type', '')
 
    # Design Module
    phase_parse = design_module.get('phases', list())
    if not phase_parse or phase_parse == "":
        phase_parse = "NA"
    else:
        phase_parse = ",".join(phase_parse)  

    today = _date_of_days_ago(0)

    set_items = f'''
        y.StudyType = "{ _clean(design_module.get('studyType',''))}",
        y.LastKnownStatus = "{ _clean(status_module.get('lastKnownStatus',''))}",
        y.NCTIdAlias = "{ identification_module.get('nctIdAliases', list())}",
        y.Acronym = "{ _clean(identification_module.get('acronym', ''))}",
        y.BriefTitle = "{ _clean(identification_module.get('briefTitle', ''))}",
        y.BriefSummary = "{ _clean(description_module.get('briefSummary', ''))}",
        y.OfficialTitle = "{ _clean(identification_module.get('officialTitle', ''))}",
        y.CompletionDate = "{ _clean(status_module.get('completionDateStruct', dict()).get('date', ''))}",       
        y.CompletionDateType = "{ _clean(status_module.get('completionDateStruct', dict()).get('type', ''))}",
        y.LastUpdatePostDate = "{ _clean(status_module.get('lastUpdatePostDateStruct', dict()).get('date', ''))}",
        y.LastUpdatePostDateType = "{ _clean(status_module.get('lastUpdatePostDateStruct', dict()).get('type', ''))}",
        y.LastUpdateSubmitDate = "{ _clean(status_module.get('lastUpdateSubmitDate', ''))}",
        y.OverallStatus = "{ _clean(status_module.get('overallStatus', ''))}",
        y.PrimaryCompletionDate = "{ _clean(status_module.get('completionDateStruct', dict()).get('date', ''))}",
        y.PrimaryCompletionDateType = "{ _clean(status_module.get('completionDateStruct', dict()).get('type', ''))}",
        y.ResultsFirstPostDate = "{ _clean(status_module.get('studyFirstPostDateStruct', dict()).get('date', ''))}",
        y.ResultsFirstPostDateType = "{ _clean(status_module.get('studyFirstPostDateStruct', dict()).get('type', ''))}",
        y.ResultsFirstPostedQCCommentsDate = "{ _clean(status_module.get('studyFirstSubmitQcDate', ''))}",
        y.StartDate = "{ _clean(status_module.get('startDateStruct', dict()).get('date', ''))}",
        y.LastUpdatedRDAS = "{today}",
        
        y.PatientRegistry = "{ design_module.get('patientRegistry',False)}",
        y.StartDateType = "{ _clean(status_module.get('startDateStruct', dict()).get('type', ''))}"
    '''

    ''' # removed from the above 'set_items'
        y.IPDSharing = "{ _clean(ipd_module.get('ipdSharing',''))}",
        y.IPDSharingDescription = "{ _clean(ipd_module.get('description',''))}", 
        y.IPDSharingInfoType = { ipd_module.get('infoTypes',list())},
        y.IPDSharingTimeFrame = "{ _clean(ipd_module.get('timeFrame',''))}",
        y.IPDSharingAccessCriteria = "{ _clean(ipd_module.get('accessCriteria',''))}",
    '''

    query = f'''
        MATCH (x:GARD) WHERE x.GardId = "{gard_id}"
        MERGE (y:ClinicalTrial {{NCTId: "{nctid}"}})
        ON CREATE
        SET {set_items},
            y.DateCreatedRDAS = "{today}"

        ON MATCH
        SET {set_items},            
            y.Phase = "{ _clean(phase_parse)}"

        MERGE (x)<-[:has_clinical_trial {{MatchedTermRDAS: "{term_matched}"}}]-(y)
        RETURN id(y) AS ct_id
    ''' 
    return query


def cypher_ClinicalTrial_map_to_GARD(gardid, nctid, term_matched):

    query = f'''
            MATCH (x:GARD {{GardId: "{gardid}"}})
            MATCH (y:ClinicalTrial {{NCTId: "{nctid}"}})
            MERGE (x)<-[:has_clinical_trial {{MatchedTermRDAS: "{term_matched}"}}]-(y)
            '''
    return query
    

def cypher_ClinicalTrial_contacts_mapping(nctid, study):

    contact_module = study.get('protocolSection',dict()).get('contactsLocationsModule',dict())
    centralContacts = contact_module.get('centralContacts',list())
    if centralContacts == list(): return None

    for contact in centralContacts:

        query = f"""
            MATCH (x:ClinicalTrial) WHERE x.NCTId = '{nctid}'
            MERGE (y:Contact {{
                ContactName: "{ _clean( contact.get('name','') )}",
                ContactRole: "{ _clean( contact.get('role','') )}",                
                ContactEmail: "{ _clean( contact.get('email','') )}"
            }})
            MERGE (x)-[:has_contact]->(y)
        """
        #ContactPhone: "{ _clean( contact.get('phone','') )}",
        #ContactPhoneExt: "{ _clean( contact.get('phoneExt','') )}",

        yield query



def cypher_AssociatedEntity(study):

    def generate_entity_query(nctid, name, classs, node_type):

        query = f"""
            MATCH (x:ClinicalTrial) WHERE x.NCTId = "{nctid}"
            MERGE (y:AssociatedEntity {{
                EntityName: "{ _clean(name)}", 
                EntityClass: "{ _clean(classs)}", 
                EntityType: "{node_type}"
            }})          

            MERGE (x)-[:has_associated_organization]->(y)
        """

        return query
    

    identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    collab_module = study.get('protocolSection',dict()).get('sponsorCollaboratorsModule',dict())

    nctid = identification_module.get('nctId', '')
    organization = identification_module.get('organization', dict())
    collaborators = collab_module.get('collaborators', list())
    leadSponsor = collab_module.get('leadSponsor',dict())

    if not organization == dict():
       
        name = identification_module.get('organization',dict()).get('fullName','')
        classs = identification_module.get('organization',dict()).get('class','')

        yield generate_entity_query(nctid, name, classs, 'Organization')

    if not leadSponsor == dict():
        name= leadSponsor.get('name', '')
        classs = leadSponsor.get('class', '')

        yield generate_entity_query(nctid, name, classs, 'Sponsor')

    if not collaborators == list():
        for collaborator in collaborators: 
            name = collaborator.get('name','')
            classs = collaborator.get('class','')
            
            yield generate_entity_query(nctid, name, classs, 'Collaborator')


def cypher_Location(study):

    identification_module = study.get('protocolSection', dict()).get('identificationModule', dict())
    loc_module = study.get('protocolSection', dict()).get('contactsLocationsModule', dict())
    
    nctid = identification_module.get('nctId', '')
    locations = loc_module.get('locations', list())

    if locations == list():
        return None

    for loc in locations:

        query = f"""
            MATCH (x:ClinicalTrial) WHERE x.NCTId = "{nctid}"
            MERGE (y:Location {{  
                LocationCity: "{ _clean( loc.get('city','') )}",
                LocationCountry: "{ _clean( loc.get('country','') )}",
                LocationFacility: "{ _clean( loc.get('facility','') )}",
                LocationState: "{ _clean( loc.get('state','') )}",
                LocationStatus: "{ _clean( loc.get('status','') )}",
                LocationZip: "{ _clean( loc.get('zip','') )}"
            }}) 

            MERGE (x)-[:in_locations]->(y)
            RETURN id(y) AS location_id, id(x) AS clinicaltrial_id
        """ 

        yield query, loc
 


def cypher_Location_contacts_mapping(nctid, location_interal_id, location):
    
    loc_contacts = location.get('contacts',list())
    if loc_contacts == list(): 
        pass

    for contact in loc_contacts: 

        query = f"""
            MATCH (z:ClinicalTrial) WHERE z.NCTId = '{nctid}'
            MATCH (x:Location) WHERE id(x) = {location_interal_id}
            MERGE (y:Contact {{
                ContactName: "{ _clean( contact.get('name','') )}",
                ContactRole: "{ _clean( contact.get('role','') )}",
                ContactEmail: "{ _clean( contact.get('email','') )}"
            }})
            MERGE (z)-[:has_contact]->(y)
            MERGE (y)-[:contact_for_location]->(x)
        """ 

        #ContactPhone: "{ _clean( contact.get('phone','') )}",
        #ContactPhoneExt: "{ _clean( contact.get('phoneExt','') )}",
        yield query


def cypher_Investigator(study):

    identification_module = study.get('protocolSection', dict()).get('identificationModule', dict())
    contact_module = study.get('protocolSection', dict()).get('contactsLocationsModule', dict())
    
    nctid = identification_module.get('nctId', '')
    officials = contact_module.get('overallOfficials', list())

    if officials == list():
        return None

    for official in officials:

        query = f"""
            MATCH (x:ClinicalTrial) WHERE x.NCTId = "{nctid}"
            MATCH (z:Contact) WHERE z.ContactName = "{ _clean( official.get('name','') )}"
            MERGE (y:Investigator {{
                OfficialName: "{ _clean( official.get('name','') )}",
                OfficialAffiliation: "{ _clean( official.get('affiliation','') )}",
                OfficialRole: "{ _clean( official.get('role','') )}"
            }})
            MERGE (x)<-[:investigates]-(y)
            MERGE (z)<-[:has_contact]-(y)
        """

        yield query


def cypher_Condition(study):

    identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    conditions_module = study.get('protocolSection',dict()).get('conditionsModule',dict())
    
    nctid = identification_module.get('nctId', '')

    conditions = conditions_module.get('conditions', list())

    if conditions == list():
        return None

    for condition in conditions:
        condition_normalized = _gard_text_normalize(condition)

        # Condition maps both ClinicalTrial & GARD, 'Condition' nodes are unique
        query = f"""
            MATCH (x:ClinicalTrial) WHERE x.NCTId = "{nctid}"
            MERGE (y:Condition {{Condition: "{condition_normalized}"}})
            MERGE (x)-[:has_investigated_condition]->(y)
            RETURN y AS cond, id(y) AS cond_id
        """ 

        yield query, condition_normalized
        


def cypher_Condition_mapping(cond_internal_id, condition_normalized, gard_id_names_dict):

    for gardid,v in gard_id_names_dict.items():

        for term in v:
            if condition_normalized == term: 
                query = f"""
                    MATCH (x:GARD) WHERE x.GardId = "{gardid}"
                    MATCH (y:Condition) WHERE id(y) = {cond_internal_id}
                    MERGE (y)-[:has_mapped_condition]->(x)
                    return y
                """
                yield query 


def cypher_StudyDesign(study): 

    identification_module = study.get('protocolSection', dict()).get('identificationModule', dict())
    design_module = study.get('protocolSection', dict()).get('designModule', dict())
    desc_module = study.get('protocolSection', dict()).get('descriptionModule', dict())
    status_module = study.get('protocolSection','').get('statusModule',dict())
    
    nctid = identification_module.get('nctId', '')

    designInfo = design_module.get('designInfo', dict())
    
    maskingInfo = designInfo.get('maskingInfo',dict()) 

    expandedAccessInfo = status_module.get('expandedAccessInfo',dict()) 
    if designInfo == dict() and maskingInfo == dict() and expandedAccessInfo == dict():
        return None     

    query = f"""
        MATCH (x:ClinicalTrial) WHERE x.NCTId = "{nctid}"
        MERGE (y:StudyDesign {{
            DesignObservationalModel: "{ _clean( designInfo.get('observationalModel','')  )}",
            DesignInterventionModel:  "{ _clean(  designInfo.get('interventionModel','') )}",
            DesignInterventionModelDescription:  "{ _clean( designInfo.get('interventionModelDescription','')  )}",
            DesignTimePerspective:  "{ _clean( designInfo.get('timePerspective','')  )}", 
            DesignAllocation:  "{ _clean( designInfo.get('allocation','')  )}",
            DesignPrimaryPurpose:  "{ _clean( designInfo.get('primaryPurpose','')  )}",
            DesignMasking:  "{ _clean(  maskingInfo.get('masking','') )}",
            DetailedDescription:  "{ _clean(  desc_module.get('detailedDescription','') )}",
            HasExpandedAccess:  "{ _clean(  expandedAccessInfo.get('hasExpandedAccess','') )}"
        }})

        MERGE (x)-[:has_study_design]->(y) 
    """

    
    return query




def cypher_PrimaryOutcome(nctid, study):
    #identification_module = study.get('protocolSection', dict()).get('identificationModule', dict())
    outcomes_module = study.get('protocolSection', dict()).get('outcomesModule', dict())
    
    primaryOutcomes = outcomes_module.get('primaryOutcomes', list())

    if primaryOutcomes == list():
        return None

    for outcome in primaryOutcomes:

        query = f"""
            MATCH (x:ClinicalTrial) WHERE x.NCTId = "{nctid}"
            MERGE (y:PrimaryOutcome {{ 
                PrimaryOutcomeMeasure: "{ _clean(outcome.get('measure',''))}", 
                PrimaryOutcomeTimeFrame: "{ _clean(outcome.get('timeFrame',''))}", 
                PrimaryOutcomeDescription: "{ _clean( outcome.get('description',''))}"
            }})            

            MERGE (x)-[:has_outcome]->(y)
        """
        yield query


def cypher_Participant(nctid, study):

    #identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    design_module = study.get('protocolSection',dict()).get('designModule',dict())
    eligibility_module = study.get('protocolSection',dict()).get('eligibilityModule',dict())

    if eligibility_module == dict() and design_module == dict():
        return None 

    # Ignore the 'Gender'
    # Gender: "{ _clean(eligibility_module.get('sex', ''))}",
    query = f'''
        MATCH (x:ClinicalTrial) WHERE x.NCTId = "{nctid}"
        MERGE (y:Participant {{ 
            EligibilityCriteria: "{ _clean(eligibility_module.get('eligibilityCriteria', ''))}",
            HealthyVolunteers: "{ _clean(eligibility_module.get('healthyVolunteers', ''))}", 
            
            StdAge: "{eligibility_module.get('stdAges', '')}",
            MinimumAge: "{ _clean(eligibility_module.get('minimumAge', ''))}",
            MaximumAge: "{ _clean(eligibility_module.get('maximumAge', ''))}",
            EnrollmentCount: "{ _clean(design_module.get('enrollmentInfo', dict()).get('count', ''))}",
            EnrollmentType: "{ _clean(design_module.get('enrollmentInfo', dict()).get('type', ''))}"
        }})
        MERGE (x)-[:has_participant_info]->(y)
    '''
     
    return query


def cypher_IndividualPatientData(study):

    identification_module = study.get('protocolSection',dict()).get('identificationModule',dict())
    ipd_module = study.get('protocolSection',dict()).get('ipdSharingStatementModule',dict())

    if ipd_module == dict():
        return None
    
    nctid = identification_module.get('nctId', '')

    query = f"""
        MATCH (x:ClinicalTrial) WHERE x.NCTId = "{nctid}"
        MERGE (y:IndividualPatientData {{
            IPDSharing: "{_clean(ipd_module.get('ipdSharing', ''))}",
            IPDSharingDescription: "{_clean(ipd_module.get('description', ''))}",
            IPDSharingInfoType: {ipd_module.get('infoTypes', [])},
            IPDSharingTimeFrame: "{_clean(ipd_module.get('timeFrame', ''))}",
            IPDSharingAccessCriteria: "{_clean(ipd_module.get('accessCriteria', ''))}"
        }})
        MERGE (x)-[:has_individual_patient_data]->(y)
    """

    return query


def cypher_Reference(nctid, study):
    identification_module = study.get('protocolSection', dict()).get('identificationModule', dict())
    ref_module = study.get('protocolSection', dict()).get('referencesModule', dict())
    
    #nctid = identification_module.get('nctId', '')
    refs = ref_module.get('references', list())

    if refs == list():
        return None

    for ref in refs:
        query = f"""
            MATCH (x:ClinicalTrial) WHERE x.NCTId = "{nctid}"
            MERGE (y:Reference {{
                Citation: "{ _clean(ref.get('citation',''))}",
                ReferencePMID: "{ _clean(ref.get('pmid',''))}",
                ReferenceType: "{ _clean(ref.get('type',''))}"
             }}) 
            MERGE (x)<-[:is_about]-(y)
        """
        yield query


def cypher_Intervention(nctid, study):
   
    intervention_module = study.get('protocolSection', dict()).get('armsInterventionsModule', dict())
    interventions = intervention_module.get('interventions', list())

    if interventions == list():
        return None

    for intervention in interventions: 

        intervention_name = _clean(intervention.get('name','')) 
        intervention_type = _clean(intervention.get('type',''))
        intervention_desc = _clean(intervention.get('description',''))
 
        query = f"""
            MATCH (x:ClinicalTrial) WHERE x.NCTId = "{nctid}"
            MERGE (y:Intervention {{
                InterventionName: "{intervention_name}",
                InterventionType: "{intervention_type}",
                InterventionDescription: "{intervention_desc}"
            }})
            MERGE (x)-[:has_intervention]->(y)
        """

        yield query
 

        # 
        # Ignore the `Drug`` at this time. 
        # The "Drug" nodes will be created by "RxNormID" and create the relationship from Intervention to Drug in the later process(init_clinical_trial_step_4.py).

        # see init_clinical_trial_step_3.py, the drug properties are in table "clinical_trial_intervention_drug" (Also 'drug_property')
        # See init_clinical_trial_step_4.py

        '''
        if intervention_type == 'DRUG':
            for rxnorm_query in rxnorm_map(intervention_name): 
                yield rxnorm_query 
        '''


def rxnorm_map(intervention):

    def cypher_Drug(rxdata,intervention_name,wspacy=False):
        rxnormid = rxdata['RxNormID']

        # Create or merge Drug node with RxNormID
        query = f'''
                MERGE (x:Drug {{RxNormID: {rxnormid} }}) 
                WITH x MATCH (y:Intervention {{InterventionName: "{intervention_name}" }}) 
                MERGE (y)-[:has_rxnorm_mapping {{WITH_SPACY: {wspacy} }}]->(x)
                '''

        yield query

        # Set additional properties on the Drug node
        for k, v in rxdata.items():
            key = k.replace(' ','')
            
            if isinstance(v, list):
                v = json.dumps(v)

            query = (f'MATCH (y:Drug {{RxNormID: {rxnormid} }}) SET y.{key} = {v}')
            yield query



    def nlp_to_drug(doc, matches, drug_name):
        for match_id, start, end in matches:
            span = doc[start:end].text

            # Retrieve RxNorm data for the drug name
            #rxdata = get_rxnorm_data(span.replace(' ','+'))

            ### For Clinial Trial init only ###
            # Use simple version for Clinical Trial init, it only contains 'RxNormID'
            rxdata = get_rxnorm_data_simple(drug_name)
            ### For Clinial Trial init only ###

            if rxdata:
                # Create connections in the database using RxNorm data
                for query in cypher_Drug(rxdata,drug_name,wspacy=True): 
                    yield query
            else:
                print(f'\t\tMap to RxNorm failed for intervention name: {drug_name}')
 

    def drug_normalize(drug_name):
        # Remove non-ASCII characters
        new_val = drug_name.encode("ascii", "ignore")
        # Decode the bytes to string
        updated_str = new_val.decode()
        # Replace non-word characters with spaces
        updated_str = re.sub('\W+',' ', updated_str)
        return updated_str
    

    drug = drug_normalize(intervention)
    drug_name = drug.replace(' ','+')

    # Retrieve RxNorm data for the drug name
    #rxdata = get_rxnorm_data(drug_name)

    ### For Clinial Trial init only ###
    # Use simple version for Clinical Trial init, it only contains 'RxNormID'
    rxdata = get_rxnorm_data_simple(drug_name)
    ### For Clinial Trial init only ###


    if rxdata:
        print(f'\t\tDrug: {drug_name}, rxdata.RxNormID = {rxdata["RxNormID"]}') ########
        # Create connections in the database using RxNorm data
        for query in cypher_Drug(rxdata, drug): 
            yield query
    else:
        # If RxNorm data not found, use SpaCy NLP to detect drug names and map to RxNorm
        doc = nlp(drug)
        matches = matcher(doc)
        for query in nlp_to_drug(doc, matches, drug): 
            yield query


### For Clinial Trial init only ###
# Simple version, only get the RxNormID
def get_rxnorm_data_simple(drug_name):
        
        # Initialize retry counter
        retries = 0
        rxnormid = None
        max_retries=10

        rxdata = dict()
        while retries < max_retries:
            try:
                # Form RxNav API request to get RxNormID based on drug name
                rq = f'https://rxnav.nlm.nih.gov/REST/rxcui.json?name={drug_name}&search=2'
                response = requests.get(rq)
                response.raise_for_status()  # Raise an exception for HTTP errors (4xx and 5xx)
                                
                # Extract RxNormID from the response 
                try:
                   obj = response.json()
                   rxnormid = obj['idGroup']['rxnormId'][0]
                   rxdata['RxNormID'] = rxnormid
                   return rxdata
                
                except KeyError as e:
                    print(f"KeyError: {e} - The required key does not exist in the JSON structure.")
                    print(f'\n{obj}\n')
                    rxnormid = None  # or some default value or behavior
                except IndexError:
                    print("IndexError: The 'rxnormId' list is empty or does not have an element at index 0.")
                    print(f'\n{obj}\n')
                    rxnormid = None  # or handle this case appropriately
                except (TypeError, AttributeError):
                    print("The JSON structure is not as expected or 'response' might not be JSON.")
                    rxnormid = None  # or handle this case appropriately
                
                break  # Exit the loop if successful
            except requests.exceptions.Timeout:
                retries += 1
                time.sleep(0.1)
            except requests.exceptions.RequestException as e:
                break  # Exit the loop for non-retryable errors

        return None
        
        ### For Clinial Trial init only ###
        # Ignore the step: Form RxNav API request to get all properties of the drug using RxNormID

        # See init_3_clinical_trial_step_3.py
         


# This is original
def get_rxnorm_data(drug_name):
        
        # Initialize retry counter
        retries = 0
        rxnormid = None
        max_retries=10

        rxdata = dict()
        while retries < max_retries:
            try:
                # Form RxNav API request to get RxNormID based on drug name
                rq = f'https://rxnav.nlm.nih.gov/REST/rxcui.json?name={drug_name}&search=2'
                response = requests.get(rq)
                response.raise_for_status()  # Raise an exception for HTTP errors (4xx and 5xx)
                                
                # Extract RxNormID from the response 
                try:
                   obj = response.json()
                   rxnormid = obj['idGroup']['rxnormId'][0]
                   rxdata['RxNormID'] = rxnormid

                except KeyError as e:
                    print(f"KeyError: {e} - The required key does not exist in the JSON structure.")
                    print(f'\n{obj}\n')
                    rxnormid = None  # or some default value or behavior
                except IndexError:
                    print("IndexError: The 'rxnormId' list is empty or does not have an element at index 0.")
                    print(f'\n{obj}\n')
                    rxnormid = None  # or handle this case appropriately
                except (TypeError, AttributeError):
                    print("The JSON structure is not as expected or 'response' might not be JSON.")
                    rxnormid = None  # or handle this case appropriately
                
                break  # Exit the loop if successful
            except requests.exceptions.Timeout:
                retries += 1
                time.sleep(0.25)
            except requests.exceptions.RequestException as e:
                break  # Exit the loop for non-retryable errors

        if not rxnormid:
            return None
        
        # re-init
        retries = 0
        max_retries=10
        while retries < max_retries:
            try:        

                # Form RxNav API request to get all properties of the drug using RxNormID
                rq2 = f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxnormid}/allProperties.json?prop=codes+attributes+names+sources'
                response = requests.get(rq2)
                results = response.json()['propConceptGroup']['propConcept']

                # Extract and organize properties of the drug
                for r in results:
                    propName = r['propName']
                    if propName in rxdata:
                        rxdata[propName].append(r['propValue'])
                    else:
                        rxdata[propName] = [r['propValue']]
                return rxdata
            
            except requests.exceptions.Timeout:
                retries += 1
                time.sleep(0.25)
            except requests.exceptions.RequestException as e:
                break  # Exit the loop for non-retryable errors

 
#
#
#
# Depracted: see init_ClinicalTrail_all.py
#
#
#

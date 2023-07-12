# Define fields (field names as used by clinical trials api) in clinical trial node type (hub node) 
ClinicalTrial = ['Acronym','BriefSummary','BriefTitle','CompletionDate','CompletionDateType','LastKnownStatus',
    'LastUpdatePostDate','LastUpdatePostDateType','LastUpdateSubmitDate','NCTId','NCTIdAlias','OfficialTitle',
    'OverallStatus','Phase','PrimaryCompletionDate','PrimaryCompletionDateType','ResultsFirstPostDate',
    'ResultsFirstPostDateType','ResultsFirstPostedQCCommentsDate','ResultsFirstPostedQCCommentsDateType',
    'StartDate','StartDateType','StudyFirstPostDate','StudyFirstPostDateType','StudyType']

# define fields for additional node types (attach to hub node)
IndividualPatientData = ['AvailIPDComment','AvailIPDId','AvailIPDType','AvailIPDURL','IPDSharing',
    'IPDSharingAccessCriteria','IPDSharingDescription','IPDSharingInfoType','IPDSharingTimeFrame','IPDSharingURL']

Sponsor = ['LeadSponsorName','LeadSponsorClass']

Collaborator = ['CollaboratorName','CollaboratorClass']

Condition = ['Condition']

StudyDesign = ['DesignAllocation','DesignInterventionModel','DesignInterventionModelDescription','DesignMasking',
    'DesignMaskingDescription','DesignObservationalModel','DesignPrimaryPurpose','DesignTimePerspective',
    'DetailedDescription','PrimaryOutcomeDescription','PrimaryOutcomeMeasure','PrimaryOutcomeTimeFrame',
    'SamplingMethod']

Participant = ['EligibilityCriteria']

ExpandedAccess = ['ExpAccTypeIndividual','ExpAccTypeIntermediate','ExpAccTypeTreatment','ExpandedAccessNCTId',
    'ExpandedAccessStatusForNCTId','HasExpandedAccess']

'''Intervention = ['InterventionBrowseLeafId','InterventionBrowseLeafName','InterventionBrowseLeafRelevance',
    'InterventionDescription','InterventionMeshId','InterventionMeshTerm','InterventionName','InterventionOtherName',
    'InterventionType','IsFDARegulatedDevice','IsFDARegulatedDrug']'''

Intervention = ['InterventionName','InterventionType']

Location = ['LocationCity','LocationCountry','LocationFacility','LocationState',
    'LocationStatus','LocationZip']

PatientRegistry = ['PatientRegistry']

Reference = ['ReferenceCitation','ReferencePMID','ReferenceType']

ArrayValues = ['Phase']

# list of lists of fields for each additional node type
additional_class_fields = [IndividualPatientData, Sponsor, Collaborator, Condition, StudyDesign, Participant, 
    ExpandedAccess, Intervention, Location, PatientRegistry, Reference]

unique_identification = ['AvailIPDId','LeadSponsorName','CollaboratorName','Condition','DetailedDescription','EligibilityCriteria','HasExpandedAccess','InterventionName','LocationCity','PatientRegistry','ReferencePMID']

# name for each additional node type
additional_class_names = ['IndividualPatientData','Sponsor','Collaborator','Condition','StudyDesign',
    'Participant','ExpandedAccess','Intervention','Location','PatientRegistry','Reference']

# direction from clinical trial node to the additional node types. Order is the same as additional_class_names
data_direction = [['-','->'],['-','->'],['-','->'],['-','->'],['-','->'],['-','->'],['-','->'],['-','->'],['-','->'],['-','->'],['<-','-']]

# name for connection from hub node to additional node type
additional_class_connections = ['has_individual_patient_data','sponsored_by','collaborated_with','investigates_condition',
    'has_study_design','has_participant_info','expanded_access_info','has_intervention','in_locations',
    'patient_registry_info','is_about']

# variable name to use in cypher
additional_class_variable_names = ['ind','spo','col','con','stu','par','exp','int','loc','pat','ref']
#old
##################################################################
#new

node_names = ['ClinicalTrial', 'IndividualPatientData', 'Sponsor', 'Collaborator', 'Condition', 'StudyDesign', 'Participant', 'ExpandedAccess', 'Intervention', 'Location', 'PatientRegistry', 'Reference']

abbreviations = {
    'ClinicalTrial': 'ct',
    'IndividualPatientData': 'ind',
    'Sponsor': 'spo',
    'Collaborator': 'col',
    'Condition': 'con',
    'StudyDesign': 'stu',
    'Participant': 'par',
    'ExpandedAccess': 'exp',
    'Intervention': 'int',
    'Location': 'loc',
    'PatientRegistry': 'pat',
    'Reference': 'ref'
}

relationships = {
    'IndividualPatientData': 'has_individual_patient_data',
    'Sponsor': 'sponsored_by',
    'Collaborator': 'collaborated_with',
    'Condition': 'investigates_condition',
    'StudyDesign': 'has_study_design',
    'Participant': 'has_participant_info',
    'ExpandedAccess': 'expanded_access_info',
    'Intervention': 'has_intervention',
    'Location': 'in_locations',
    'PatientRegistry': 'patient_registry_info',
    'Reference': 'is_about'
}

rel_directions = {
    'IndividualPatientData': ['-','->'],
    'Sponsor': ['-','->'],
    'Collaborator': ['-','->'],
    'Condition': ['-','->'],
    'StudyDesign': ['-','->'],
    'Participant': ['-','->'],
    'ExpandedAccess': ['-','->'],
    'Intervention': ['-','->'],
    'Location': ['-','->'],
    'PatientRegistry': ['-','->'],
    'Reference': ['<-','-']

}

fields = {
    'ClinicalTrial': ['Acronym','BriefSummary','BriefTitle','CompletionDate','CompletionDateType','LastKnownStatus',
    'LastUpdatePostDate','LastUpdatePostDateType','LastUpdateSubmitDate','NCTId','NCTIdAlias','OfficialTitle',
    'OverallStatus','Phase','PrimaryCompletionDate','PrimaryCompletionDateType','ResultsFirstPostDate',
    'ResultsFirstPostDateType','ResultsFirstPostedQCCommentsDate','ResultsFirstPostedQCCommentsDateType',
    'StartDate','StartDateType','StudyFirstPostDate','StudyFirstPostDateType','StudyType'],

    'IndividualPatientData': ['AvailIPDComment','AvailIPDId','AvailIPDType','AvailIPDURL','IPDSharing',
    'IPDSharingAccessCriteria','IPDSharingDescription','IPDSharingInfoType','IPDSharingTimeFrame','IPDSharingURL'],
    
    'Sponsor': ['LeadSponsorName','LeadSponsorClass'],

    'Collaborator': ['CollaboratorName','CollaboratorClass'],

    'Condition': ['Condition'],

    'StudyDesign': ['DesignAllocation','DesignInterventionModel','DesignInterventionModelDescription','DesignMasking',
    'DesignMaskingDescription','DesignObservationalModel','DesignPrimaryPurpose','DesignTimePerspective',
    'DetailedDescription','PrimaryOutcomeDescription','PrimaryOutcomeMeasure','PrimaryOutcomeTimeFrame',
    'SamplingMethod'],

    'Participant': ['EligibilityCriteria'],

    'ExpandedAccess': ['ExpAccTypeIndividual','ExpAccTypeIntermediate','ExpAccTypeTreatment','ExpandedAccessNCTId',
    'ExpandedAccessStatusForNCTId','HasExpandedAccess'],

    'Intervention': ['InterventionName','InterventionType','InterventionDescription'],

    'Location': ['LocationCity','LocationCountry','LocationFacility','LocationState',
    'LocationStatus','LocationZip'],

    'PatientRegistry': ['PatientRegistry'],

    'Reference': ['ReferenceCitation','ReferencePMID','ReferenceType']

}

lists_of_nodes = {
'Collaborator': 'Collaborator',
'Condition': 'Condition',
'Intervention': 'Intervention',
'Location': 'Location',
'Reference': 'Reference',

'Unassigned': ['SecondaryIdInfo','ArmGroup','PrimaryOutcome','SecondaryOutcome','OtherOutcome','StdAge','OverallOfficial','IPDSharingInfoType','ConditionMesh','ConditionAncestor','ConditionBrowseLeaf','ConditionBrowseBranch','InterventionMesh','InterventionAncestor','InterventionBrowseLeaf','InterventionBrowseBranch']
}

fields_as_properties = {
'ClinicalTrial': ['Phase']
}

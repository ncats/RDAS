# Profile.title
DR  = 'Dr.'
MR  = 'Mr.'
MRS = 'Mrs.'
MS  = 'Ms.'

# Profile.qualification
RESEARCHER                      = "Researcher"
MICROBIOLOGIST                  = "Microbiologist"
PHARMACOLOGIST                  = "Pharmacologist"
IMMUNOLOGIST                    = "Immunologist"
SOCIAL_WORKER                   = "Social Worker"
COMMUNITY_HEALTH_WORKER         = 'Community Health Worker'
EPIDEMIOLOGIST                  = "Epidemiologist"
PUBLIC_HEALTH_SPECIALIST        = 'Public Health Specialist'
HEALTH_POLICY_SPECIALIST        = "Health Policy Specialist"
REGULATOR                       = "Regulator"
LAWYER                          = "Lawyer"
INFORMATICIST_DATA_SCIENTIST    = "Informaticist/Data Scientist"
MEDICAL_DOCTOR                  = 'Medical Doctor'
NURSE_PRACTITIONER              = 'Nurse Practitioner'
NURSE                           = 'Nurse'
OTHER_HEALTHCARE_PROFESSIONAL   = 'Other Healthcare Professional'
PHARMACIST                      = 'Pharmacist'
PHYSICIAN_ASSISTANT             = 'Physician Assistant'
PATIENT                         = 'Patient'
PARENT                          = 'Parent'
CAREGIVER                       = 'Caregiver'
COMMUNITY_MEMBER                = 'Community Member'
ADVOCATE                        = 'Advocate'
CURE_ADMIN                      = 'CURE Admin'

# Profile.status
ACTIVE = 'ACTIVE'
PAUSED = 'PAUSED'
BANNED = 'BANNED'

# Regimen.severity + Report.severity
ICU_CRITICAL_CARE   = 'ICU/Critical Care'
INPATIENT           = 'Inpatient'
OUTPATIENT          = 'Outpatient'

# Report.status, UserProposedArticle.status
APPROVED    = 'Approved'
DELETED     = 'Deleted'
REJECTED    = 'Rejected'
SAVED       = 'Saved'
SUBMITTED   = 'Submitted'
FLAGGED     = 'Flagged'

# Report.article_type
ORIGINAL    = 'Original'
REVIEW      = 'Review'

# Report.study_type
CASE_REPORT         = 'Case Report'
CASE_SERIES         = 'Case Series'
OBSERVATIONAL_STUDY = 'Observational Study'
CLINICAL_TRIAL      = 'Clinical Trial'

# Report.study_type, Patient.sex
OTHER = 'Other'

# Report.outcome
PATIENT_CONDITION_UNCHANGED = "Patient's condition was unchanged"
PATIENT_DETERIORATED        = 'Patient deteriorated'
PATIENT_DIED                = 'Patient died'
PATIENT_IMPROVED            = 'Patient improved'
PATIENT_WAS_CURED           = 'Patient was cured/recovered'
TREATMENT_TERMINATED        = 'Treatment was terminated due to adverse events'
UNKNOWN_OUTCOME             = 'Outcome is unknown/not yet determined'

# Report.outcome_computed
IMPROVED = 'Improved'
DETERIORATED = 'Deteriorated'
UNDETERMINED = 'Undetermined'
OUTCOME_CASES   = {
        PATIENT_WAS_CURED:              IMPROVED,
        PATIENT_IMPROVED:               IMPROVED,
        PATIENT_CONDITION_UNCHANGED:    UNDETERMINED,
        PATIENT_DETERIORATED:           DETERIORATED,
        PATIENT_DIED:                   DETERIORATED,
        TREATMENT_TERMINATED:           UNDETERMINED,
        UNKNOWN_OUTCOME:                UNDETERMINED,
        '':                             UNDETERMINED,
}

# Report.surgery, Neonate.diagnosed_with_disease
IDK_UNKNOWN = 'Unknown'
NO          = 'No'
YES         = 'Yes'
NOT_TESTED  = 'Not tested'

# Patient.age_group
Q6_C1  = '<1 year'
Q6_C2  = '1-5 years'
Q6_C3  = '6-10 years'
Q6_C4  = '11-15 years'
Q6_C5  = '16-20 years'
Q6_C6  = '21-30 years'
Q6_C7  = '31-40 years'
Q6_C8  = '41-50 years'
Q6_C9  = '51-60 years'
Q6_C10 = '61-70 years'
Q6_C11 = '71-80 years'
Q6_C12 = '81-89 years'
Q6_C13 = '90+ years'

#Report.when_outcome
WHILE = 'While the patient was still on treatment'
AT_COMPLETED = 'At the time the treatment was completed'
AFTER = 'After a period of follow-up'

# Patient.sex
FEMALE          = 'Female'
MALE            = 'Male'
NOT_SPECIFIED   = 'Not specified'
INTERSEX        = 'Intersex'

# Patient.ethnicity
HISPANIC        = 'Hispanic/Latino'
NON_HISPANIC    = 'Not Hispanic/Latino'
NA              = 'Not applicable'

# Django Admin pages/admin.py
ADMIN_ITEMS_PER_PAGE = 25

# Gestational Age Weeks
UNKNOWN = "Unknown"

# Pregnancy.treatment/delivery_gestational_age
GESTATIONAL_1_WEEK = "1 Week"
GESTATIONAL_2_WEEKS = "2 Weeks"
GESTATIONAL_3_WEEKS = "3 Weeks"
GESTATIONAL_4_WEEKS = "4 Weeks"
GESTATIONAL_5_WEEKS = "5 Weeks"
GESTATIONAL_6_WEEKS = "6 Weeks"
GESTATIONAL_7_WEEKS = "7 Weeks"
GESTATIONAL_8_WEEKS = "8 Weeks"
GESTATIONAL_9_WEEKS = "9 Weeks"
GESTATIONAL_10_WEEKS = "10 Weeks"
GESTATIONAL_11_WEEKS= "11 Weeks"
GESTATIONAL_12_WEEKS = "12 Weeks"
GESTATIONAL_13_WEEKS = "13 Weeks"
GESTATIONAL_14_WEEKS = "14 Weeks"
GESTATIONAL_15_WEEKS = "15 Weeks"
GESTATIONAL_16_WEEKS = "16 Weeks"
GESTATIONAL_17_WEEKS = "17 Weeks"
GESTATIONAL_18_WEEKS = "18 Weeks"
GESTATIONAL_19_WEEKS = "19 Weeks"
GESTATIONAL_20_WEEKS = "20 Weeks"
GESTATIONAL_21_WEEKS = "21 Weeks"
GESTATIONAL_22_WEEKS = "22 Weeks"
GESTATIONAL_23_WEEKS = "23 Weeks"
GESTATIONAL_24_WEEKS = "24 Weeks"
GESTATIONAL_25_WEEKS = "25 Weeks"
GESTATIONAL_26_WEEKS = "26 Weeks"
GESTATIONAL_27_WEEKS = "27 Weeks"
GESTATIONAL_28_WEEKS = "28 Weeks"
GESTATIONAL_29_WEEKS = "29 Weeks"
GESTATIONAL_30_WEEKS = "30 Weeks"
GESTATIONAL_31_WEEKS = "31 Weeks"
GESTATIONAL_32_WEEKS = "32 Weeks"
GESTATIONAL_33_WEEKS = "33 Weeks"
GESTATIONAL_34_WEEKS = "34 Weeks"
GESTATIONAL_35_WEEKS = "35 Weeks"
GESTATIONAL_36_WEEKS = "36 Weeks"
GESTATIONAL_37_WEEKS = "37 Weeks"
GESTATIONAL_38_WEEKS = "38 Weeks"
GESTATIONAL_39_WEEKS = "39 Weeks"
GESTATIONAL_40_WEEKS= "40 Weeks"
GESTATIONAL_41_WEEKS = "41 Weeks"
GESTATIONAL_42_WEEKS = "42 Weeks"

GESTATIONAL_AGE_WEEKS=(
    (UNKNOWN,UNKNOWN),
    (GESTATIONAL_1_WEEK,GESTATIONAL_1_WEEK),
    (GESTATIONAL_2_WEEKS,GESTATIONAL_2_WEEKS),
    (GESTATIONAL_3_WEEKS,GESTATIONAL_3_WEEKS),
    (GESTATIONAL_4_WEEKS,GESTATIONAL_4_WEEKS),
    (GESTATIONAL_5_WEEKS,GESTATIONAL_5_WEEKS),
    (GESTATIONAL_6_WEEKS,GESTATIONAL_6_WEEKS),
    (GESTATIONAL_7_WEEKS,GESTATIONAL_7_WEEKS),
    (GESTATIONAL_8_WEEKS,GESTATIONAL_8_WEEKS),
    (GESTATIONAL_9_WEEKS,GESTATIONAL_9_WEEKS),
    (GESTATIONAL_10_WEEKS,GESTATIONAL_10_WEEKS),
    (GESTATIONAL_11_WEEKS,GESTATIONAL_11_WEEKS),
    (GESTATIONAL_12_WEEKS,GESTATIONAL_12_WEEKS),
    (GESTATIONAL_13_WEEKS,GESTATIONAL_13_WEEKS),
    (GESTATIONAL_14_WEEKS,GESTATIONAL_14_WEEKS),
    (GESTATIONAL_15_WEEKS,GESTATIONAL_15_WEEKS),
    (GESTATIONAL_16_WEEKS,GESTATIONAL_16_WEEKS),
    (GESTATIONAL_17_WEEKS,GESTATIONAL_17_WEEKS),
    (GESTATIONAL_18_WEEKS,GESTATIONAL_18_WEEKS),
    (GESTATIONAL_19_WEEKS,GESTATIONAL_19_WEEKS),
    (GESTATIONAL_20_WEEKS,GESTATIONAL_20_WEEKS),
    (GESTATIONAL_21_WEEKS,GESTATIONAL_21_WEEKS),
    (GESTATIONAL_22_WEEKS,GESTATIONAL_22_WEEKS),
    (GESTATIONAL_23_WEEKS,GESTATIONAL_23_WEEKS),
    (GESTATIONAL_24_WEEKS,GESTATIONAL_24_WEEKS),
    (GESTATIONAL_25_WEEKS,GESTATIONAL_25_WEEKS),
    (GESTATIONAL_26_WEEKS,GESTATIONAL_26_WEEKS),
    (GESTATIONAL_27_WEEKS,GESTATIONAL_27_WEEKS),
    (GESTATIONAL_28_WEEKS,GESTATIONAL_28_WEEKS),
    (GESTATIONAL_29_WEEKS,GESTATIONAL_29_WEEKS),
    (GESTATIONAL_30_WEEKS,GESTATIONAL_30_WEEKS),
    (GESTATIONAL_31_WEEKS,GESTATIONAL_31_WEEKS),
    (GESTATIONAL_32_WEEKS,GESTATIONAL_32_WEEKS),
    (GESTATIONAL_33_WEEKS,GESTATIONAL_3_WEEKS),
    (GESTATIONAL_34_WEEKS,GESTATIONAL_34_WEEKS),
    (GESTATIONAL_35_WEEKS,GESTATIONAL_35_WEEKS),
    (GESTATIONAL_36_WEEKS,GESTATIONAL_36_WEEKS),
    (GESTATIONAL_37_WEEKS,GESTATIONAL_37_WEEKS),
    (GESTATIONAL_38_WEEKS,GESTATIONAL_38_WEEKS),
    (GESTATIONAL_39_WEEKS,GESTATIONAL_39_WEEKS),
    (GESTATIONAL_40_WEEKS,GESTATIONAL_40_WEEKS),
    (GESTATIONAL_41_WEEKS,GESTATIONAL_41_WEEKS),
    (GESTATIONAL_42_WEEKS,GESTATIONAL_42_WEEKS),
    (GESTATIONAL_42_WEEKS,GESTATIONAL_42_WEEKS),
)

#clinical trial study type
INTERVENTIONAL="Interventional"
OBESERVATIONAL="Observational"
PATIENT_REGISTRIES="Patient registries"
EXPANDED_ACCESS="Expanded access"

# Adverse Event Outcome
AE_DEATH = 'Death'
AE_LIFETHREATENING = 'Life-threatening'
AE_HOSPITALIZATION = 'Hospitalization (initial or prolonged)'
AE_PERMANENT_DAMAGE = 'Disability or Permanent Damage'
AE_CONGENITAL_ANOMALY = 'Congenital Anomaly/Birth Defects'
AE_OTHER_SERIOUS_EVENTS = 'Other Serious or Important Medical Events'
AE_REQUIRED_INTERVENTION = 'Required Intervention to Prevent Permanent Impairment/Damage'
AE_NON_SERIOUS_EVENT = 'Non-Serious Medical Event'

#Report Reminder
NO_REMINDER = 'No reminder'
ONE_WEEK = '1 week'
TWO_WEEKS = '2 weeks'
THREE_WEEKS = '3 weeks'
ONE_MONTH = '1 month'
THREE_MONTHS = '3 months'
SIX_MONTHS = '6 months'
ONE_YEAR = '1 year'

# AttachedImage image_name

#TODO NEED TO FIX WHEN FRONT END FIXES IT
ATTACHED_IMAGE_FILLER="attached-image-filler-content.jpg"
PROFILE_IMAGE_FILLER="attached-image-filler-profile.jpg"

# Article.publication_type
NEWS = "news"
JOURNAL = "journal"

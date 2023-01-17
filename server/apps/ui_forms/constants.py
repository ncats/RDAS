# Default page for Case Reports form
DEFAULT_PAGE = 'disease'

# Additional pages to be added in Case Reports form for each Drug
DRUG_PAGES = ('dosing-regimen','additional-details','treatment-setting')
PATIENT_DRUG_PAGES = ('dosing-regimen','treatment-setting')
TUBERCULOSIS_PAGES = ('organism', 'tb-clinical-presentation','clinical-presentation','resistance', 'tb-resistance')
OUTCOME_PAGES = ('outcome-determination', 'outcome-timing', 'relapse')
PREGNANCY_PAGES = ['neonates', 'gestational-age']
#MONKEY_POX_PAGES = ['monkeypox_symptoms', 'monkeypox_treatment_administration', 'monkeypox_hospitalization', 'monkeypox_contact', 'monkeypox_vaccine_status']


OUTCOME_TIMING_FOLLOW_UP = "report__outcome_followup"
OUTCOME_TIMING_ADDITIONAL_Q =  {
			"key": "report__outcome_followup",
			"controlType": "textbox",
			"width":100,
			"label": "",
			"placeholder": "How long was the follow-up?",
			"inputType": "text",
			"visibility": False
		}

PATIENT_MONKEY_POX_TREATMENT_ADDITIONAL_Q = {
                "key": "report__extra_fields__monkeypox_lesion_pain_time_to_improvement",
                "label": "How long did it take for your blisters or rash to go away?",
                "placeholder": "Select",
                "controlType": "dropdown",
                "options": [{
                    "value": "1 day"
                }, {
                    "value": "2 days"
                }, {
                    "value": "3 days"
                }, {
                    "value": "4 days"
                }, {
                    "value": "5 days"
                }, {
                    "value": "6 days"
                }, {
                    "value": "7 days"
                }, {
                    "value": "8 days"
                }, {
                    "value": "9 days"
                }, {
                    "value": "10 days"
                }, {
                    "value": "11 days"
                }, {
                    "value": "12 days"
                }, {
                    "value": "13 days"
                }, {
                    "value": "14 days"
                }, {
                    "value": "15 days"
                }]
            }, {
                "key": "report__extra_fields__monkeypox_lesion_time_to_resolve",
                "label": "How long did it take for your blisters or rash to go away?",
                "placeholder": "Select",
                "controlType": "dropdown",
                "options": [{
                    "value": "1 day"
                }, {
                    "value": "2 days"
                }, {
                    "value": "3 days"
                }, {
                    "value": "4 days"
                }, {
                    "value": "5 days"
                }, {
                    "value": "6 days"
                }, {
                    "value": "7 days"
                }, {
                    "value": "8 days"
                }, {
                    "value": "9 days"
                }, {
                    "value": "10 days"
                }, {
                    "value": "11 days"
                }, {
                    "value": "12 days"
                }, {
                    "value": "13 days"
                }, {
                    "value": "14 days"
                }, {
                    "value": "15 days"
                }]
            }
            
MONKEY_POX_TREATMENT_ADDITIONAL_Q = [{
                "key": "report__extra_fields__monkeypox_lesion_pain_time_to_improvement",
                "label": "How long did it take for the pain associated with the patient's skin lesions or vesicles to improve?",
                "placeholder": "Select",
                "controlType": "dropdown",
                "options": [{
                    "value": "1 day"
                }, {
                    "value": "2 days"
                }, {
                    "value": "3 days"
                }, {
                    "value": "4 days"
                }, {
                    "value": "5 days"
                }, {
                    "value": "6 days"
                }, {
                    "value": "7 days"
                }, {
                    "value": "8 days"
                }, {
                    "value": "9 days"
                }, {
                    "value": "10 days"
                }, {
                    "value": "11 days"
                }, {
                    "value": "12 days"
                }, {
                    "value": "13 days"
                }, {
                    "value": "14 days"
                }, {
                    "value": "15 days"
                }]
            }, {
                "key": "report__extra_fields__monkeypox_lesion_time_to_resolve",
                "label": "How many days after treatment administration was it before the skin lesions or vesicles began to resolve?",
                "placeholder": "Select",
                "controlType": "dropdown",
                "options": [{
                    "value": "1 day"
                }, {
                    "value": "2 days"
                }, {
                    "value": "3 days"
                }, {
                    "value": "4 days"
                }, {
                    "value": "5 days"
                }, {
                    "value": "6 days"
                }, {
                    "value": "7 days"
                }, {
                    "value": "8 days"
                }, {
                    "value": "9 days"
                }, {
                    "value": "10 days"
                }, {
                    "value": "11 days"
                }, {
                    "value": "12 days"
                }, {
                    "value": "13 days"
                }, {
                    "value": "14 days"
                }, {
                    "value": "15 days"
                }]
            }]

MONKEY_POX_LESION_ADDITIONAL_Q =             {
                "key": "report__extra_fields__monkeypox_lesion_pain",
                "label": "How severe was the pain with the patient's skin lesions or vesicles? from 0-10 (0=no pain, 10=worst pain ever experienced)",
                "placeholder": "Select",
                "controlType": "dropdown",
                "options": [{
                    "value": "0"
                }, {
                    "value": "1"
                }, {
                    "value": "2"
                }, {
                    "value": "3"
                }, {
                    "value": "4"
                }, {
                    "value": "5"
                }, {
                    "value": "6"
                }, {
                    "value": "7"
                }, {
                    "value": "8"
                }, {
                    "value": "9"
                }, {
                    "value": "10"
                }]
            }

PATIENT_MONKEY_POX_LESION_ADDITIONAL_Q = {
                "key": "report__extra_fields__monkeypox_lesion_pain",
                "label": "How painful were your blisters or rash from 0-10 (0=no pain, 10=worst pain ever experienced)?",
                "placeholder": "Select",
                "controlType": "dropdown",
                "options": [{
                    "value": "0"
                }, {
                    "value": "1"
                }, {
                    "value": "2"
                }, {
                    "value": "3"
                }, {
                    "value": "4"
                }, {
                    "value": "5"
                }, {
                    "value": "6"
                }, {
                    "value": "7"
                }, {
                    "value": "8"
                }, {
                    "value": "9"
                }, {
                    "value": "10"
                }]
            }
REQUIRED_PAGES = {"Disease":"disease", "Challenge": "why_new_way", "Drug(s)": "drugs", "Outcome": "outcome", "Adverse Events": "adverse_events", "Pregnancy":"report__patient__pregnant", "Reminder": "report__reminder"}

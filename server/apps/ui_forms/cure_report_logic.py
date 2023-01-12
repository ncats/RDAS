
from copy import deepcopy
from server.apps.core.models import Drug, Report,CureReport, AttachedImage
from urllib.parse import urlparse, parse_qs
from operator import attrgetter
from server.apps.ui_forms.constants import *
import json
from os.path import exists
from django.contrib.contenttypes.models import ContentType
from server.apps.api.v2.serializers.image import AttachedImageSerializer

def get_pageinfo(request, pk=None):
    current = request.data["currentPage"].split("__")[0]
    next= request.data["nextPage"].split("__")[0]
    page_obj = {
        "next-page":'',
        "menu":''
    }
    includeMenu = False

    report = CureReport.objects.get(id=pk)
    if report.report.disease.name == 'Monkeypox':
        page_data_obj,cure_menu_obj = get_json(report,'mp-report')
    else:
        page_data_obj,cure_menu_obj = get_json(report, 'case-report')

    if current:
        currentpage = current.split('&')[0]

    if next:
        nextpage = next.split('&')[0]
        nextpage_data = page_obj['next-page'] = page_data_obj[nextpage]

        if nextpage in DRUG_PAGES:
            drug_index = 0
            parsed_url = urlparse('?' + next)
            params = parse_qs(parsed_url.query)
            if params:
                drug_index = params.get('index')[0]
                drug_id = params.get('drug')[0]
        
            if currentpage in DRUG_PAGES:
                if currentpage == 'treatment-settings':
                    drug_index += 1

            changeKeyForAdditionalDrugPages(nextpage_data, drug_index, drug_id)
            
    current_page_required = page_data_obj[currentpage]['isRequired']
    if (currentpage and current_page_required == True) or currentpage=='other-conditions' or currentpage=='monkeypox_symptoms' :
        includeMenu = True


    if next == 'initial-overview':
        fillInitialReview(pk, page_obj['next-page'])

    if next == 'final-review':
        page_obj['next-page'] = fillAttributes(request,pk, page_obj['next-page'])

    changeMenu(cure_menu_obj,page_data_obj,pk,request)

    if includeMenu:
        page_obj['menu'] = cure_menu_obj["menu"]
    
    return page_obj

def get_json(report,type_):
    files = [ "form", "menu", ]
    return_obj = []
    if report.report.disease.name == 'Monkeypox':
        if report.author.profile.qualification in ['Patient', 'Parent', 'Caregiver']:
            page_data = open(f'server/apps/ui_forms/json_data/patient-case-report-form.json').read()
            page_data_obj = json.loads(page_data)
            return_obj.append(page_data_obj)
        else:
            page_data = open(f'server/apps/ui_forms/json_data/case-report-form.json').read()
            page_data_obj = json.loads(page_data)
            return_obj.append(page_data_obj)

    for file_ in files:
        if report.author.profile.qualification in ['Patient', 'Parent', 'Caregiver']:
            key = f"patient-{type_}-{file_}"
            print(key)
        else:
            key = f"{type_}-{file_}"
            print(key)

        json_file = f"server/apps/ui_forms/json_data/{key}.json"
        if exists(json_file):
            json_data = open(json_file).read()
            data = json.loads(json_data)
            return_obj.append(deepcopy(data))

    return return_obj

def changeMenu(cure_menu_obj,page_data_obj,pk, request=None):
    report = CureReport.objects.get(id=pk)
    if  report.report.disease.name == 'Tuberculosis':
        #set organism page false, send tb resistance, send tb clinical presentation page
        changeShowPage(cure_menu_obj,TUBERCULOSIS_PAGES)

    if report.report.disease.name == 'Monkeypox':

        has_hiv = [c for c in report.report.patient.comorbidity.all() if c.value == 'HIV']
        if has_hiv:
            print('herere')
            changeShowPage(cure_menu_obj, ['monkeypox_hiv'])

        if report.report.extra_fields.get('monkeypox_symptoms',None) and any(s['value'] == "Skin lesions, vesicles, pustules, or scabs on the limbs or trunk" for s in report.report.extra_fields.get('monkeypox_symptoms')):
            if request.user.profile.qualification in ['Patient', 'Parent', 'Caregiver']:
                page_data_obj['monkeypox_treatment_administration']['formControls'].append(PATIENT_MONKEY_POX_LESION_ADDITIONAL_Q)
            else:
                page_data_obj['monkeypox_treatment_administration']['formControls'].append(MONKEY_POX_LESION_ADDITIONAL_Q)

        if report.report.outcome in ['Patient was cured/recovered']:
            print('outtttcomeeoeme')
            changeShowPage(cure_menu_obj,['monkeypox_skin_vesicle'])
            if (report.report.extra_fields.get('monkeypox_symptoms',None) and any(s['value'] == "Skin lesions, vesicles, pustules, or scabs on the limbs or trunk" for s in report.report.extra_fields.get('monkeypox_symptoms'))) and report.report.outcome in ['Patient was cured/recovered','Patient Improved']:
                if request.user.profile.qualification in ['Patient', 'Parent', 'Caregiver']:
                    print('hererer')
                    page_data_obj['monkeypox_skin_vesicle']['formControls'].insert(0,PATIENT_MONKEY_POX_TREATMENT_ADDITIONAL_Q)
                else:
                    page_data_obj['monkeypox_skin_vesicle']['formControls'].insert(0,MONKEY_POX_TREATMENT_ADDITIONAL_Q)
    if report.report.outcome not in ["Outcome is unknown/not yet determined","Patient died"]:
        #do not send outcome determination outcome timing relapse
        if request.user.profile.qualification in ['Patient', 'Parent', 'Caregiver']:
            changeShowPage(cure_menu_obj,['relapse'])
        else:
            changeShowPage(cure_menu_obj, ['outcome-determination','outcome-timing','relapse'])

    if report.report.how_outcome:
        toggleAdditionalQuestionOutcomeTiming(page_data_obj['outcome-timing'])

    if report.report.adverse_events:
        changeShowPage(cure_menu_obj, ['adverse-events-outcome'])
        
    if report.report.patient.pregnant:
        #if patient dont show neonates
        if request.user.profile.qualification in ['Patient', 'Parent', 'Caregiver']:
            changeShowPage(cure_menu_obj, ['gestational-age'])
        else:
            changeShowPage(cure_menu_obj, PREGNANCY_PAGES)
            cure_menu_obj['menu'][get_page_index('neonates',cure_menu_obj)]['name'] = "neonates&disease={}".format(report.report.disease.id)
            addDiseasetoNeonatePage(report.report.disease , page_data_obj['neonates'])
        
    if report.report.drugs.all():
        #send dosing regiment additional details treatment setting
        #patient only shows two dosing regimen and treatment setting
        if request.user.profile.qualification in ['Patient', 'Parent', 'Caregiver']:
            changeShowPage(cure_menu_obj, PATIENT_DRUG_PAGES)
        else:
            changeShowPage(cure_menu_obj, DRUG_PAGES)
        start = get_page_index('dosing-regimen',cure_menu_obj)
        end = get_page_index('treatment-setting',cure_menu_obj)

        drug_count = report.report.drugs.all().count()
        #copy the required drug pages use deep copy so they take up different areas in memoery need to be independent of each other
        drug_pages_to_show = deepcopy(cure_menu_obj['menu'][start:end+1])
        drug_pages_to_return = list()
        #make a list so the pages need to show are not the same in memory
        #multiple the pages by how many drugs there are
        for i in range(drug_count):
            drug_pages_to_return.extend(deepcopy(drug_pages_to_show))
        counter = 0
        for index, page in enumerate(drug_pages_to_return):
            if index % 3 == 0:
                drug_name = report.report.drugs.all()[counter].name
                drug_index = [i for i, d in enumerate(report.report.drugs.all()) if d.name == drug_name]
                drug,created = Drug.objects.get_or_create(name=drug_name.capitalize())
                counter+=1
            #update the name of the page to include the drug index in the request
            page['name'] = "{}&index={}&drug={}".format(page['name'],drug_index[0],drug.id)
            page['title'] = page['title']+ " " +drug_name
            page['description'] = page['description'].replace("${drug}", drug_name)

        first_half = cure_menu_obj['menu'][:start]
        second_half = cure_menu_obj['menu'][end+1:]
        cure_menu_obj['menu'] = first_half + drug_pages_to_return + second_half
    
    
def changeShowPage(curemenu, fields):
    for page in curemenu['menu']:
        if page['name'] in fields:
            print(page)
            page['showPage'] = not page['showPage']
            print(page['showPage'])

def changeKeyForAdditionalDrugPages( next_page_data, index, drug_id):
    pagename = next_page_data['name']
    drug_name = Drug.objects.get(id=drug_id)
    next_page_data['description'] = next_page_data['description'].replace("${drug_name}", drug_name.name )
    next_page_data['name'] = "{}&index={}&drug={}".format(pagename, index, drug_id)
    for formdata in next_page_data['formControls']:
        params=formdata['key'].split('__')
        params.insert(2,index)
        formdata['key'] = '__'.join(params)

#to change the label for the diagnosed disease section for neonates ti inlcude the disease name
def addDiseasetoNeonatePage(disease, neonate_page):
    neonate_page['name']= "neonates&disease={}".format(disease.id)
    neonate_page['formControls'][0]['controls'][0]['label']=neonate_page['formControls'][0]['controls'][0]['label'].replace("${disease}",disease.name)

def toggleAdditionalQuestionOutcomeTiming(outcome_timing_page):
    outcome_timing_page['formControls'][0]['toggleFieldName'] = OUTCOME_TIMING_FOLLOW_UP
    outcome_timing_page['formControls'].append(OUTCOME_TIMING_ADDITIONAL_Q)

def fillInitialReview(report, page):
    report = CureReport.objects.get(id=report)
    if report.author.profile.qualification in ['Patient', 'Parent', 'Caregiver']:
        values = {
            "Disease": report.report.disease.name,
            "Diagnosis": ", ".join(report.report.how_diagnosis),
            "Drug(s)": ", ".join([d.name for d in report.report.drugs.all()]),
            "Outcome": report.report.outcome,
            "Adverse Events": report.report.adverse_events or "Not reported",
            "Pregnant": 'Yes' if report.report.patient.pregnant else "No",
            "Reminder": report.when_reminder
        }
    else:
        values = {
            "Disease": report.report.disease.name,
            "Diagnosis": ", ".join(report.report.how_diagnosis),
            "Challenge": ", ".join(report.report.why_new_way),
            "Drug(s)": ", ".join([d.name for d in report.report.drugs.all()]),
            "Outcome": report.report.outcome,
            "Adverse Events": report.report.adverse_events or "Not reported",
            "Pregnant": 'Yes' if report.report.patient.pregnant else "No",
            "Reminder": report.when_reminder
        }

    page['formControls'][0]['value'] = values


def fillAttributes(request,pk, final_review):
    report = CureReport.objects.get(id=pk)
    if report.report.disease.name == 'Monkeypox':
        final_review = open('server/apps/ui_forms/json_data/final-review-monkeypox.json').read()
        if request.user.profile.qualification in ['Patient', 'Parent', 'Caregiver']:
            final_review = open('server/apps/ui_forms/json_data/patient-final-review-monkeypox.json').read()
        final_review = json.loads(final_review)
    if report.report.disease.name == 'Tuberculosis':
        final_review = open('server/apps/ui_forms/json_data/final-review-tb.json').read()
        if request.user.profile.qualification in ['Patient', 'Parent', 'Caregiver']:
            final_review = open('server/apps/ui_forms/json_data/patient-final-review-tb.json').read()
        final_review = json.loads(final_review)

    for d in final_review['formControls']:
        if 'groups' in d:
            for i in d['groups']:
                #treatment-details and pregnancy have multiple values have to be handled differently
                if i['key'] == 'treatment-details':
                    #have to add one object per each drug regimen
                    for r in report.report.regimens.all():
                        value =  {"controlType": "json",
                            "key": "diagnosis",
                            "value": {
                                str(r.drug.name): f"{r.dose}, {r.frequency}, {r.route}, {r.duration}" if r.dose and r.frequency and r.duration else "",
                                "How Drug Used in New Way": " ".join(r.use_drug) if r.use_drug else "",
                                "Treatment Setting": f"{r.severity}" if r.severity else "",
                                "Additional Details": f"{r.severity_detail}" if r.severity_detail else ""
                            }
                        }
                        value['value'] = dict((k,v) for k,v in value['value'].items() if v)
                        i['controls'].append(value)
                    
                    #delete the default empty value from json
                    if len(i['controls']) > 1:
                        del i['controls'][0]
                elif i['key'] == 'pregnancy':
                    #have to fill in values normally
                    i['controls'][0]['value']= fill_fields(pk, i['controls'][0]['value'].items()) if fill_fields(pk, i['controls'][0]['value'].items()) else {}
                    if report.report.patient.pregnancy:
                        #have to add one object for each neonate
                        for n in report.report.patient.pregnancy.neonates.all():
                            neonate_value = {
                                "controlType": "json",
                                "key": "neonate",
                                "value": {
                                    f"Was the neonate/fetus diagnosed with {report.report.disease.name}?": f"{n.diagnosed_with_disease}" if n.diagnosed_with_disease else '',
                                    "Neonate/fetus anomalies/disabilities, if present": f"{n.abnormalities_or_defects}" if n.abnormalities_or_defects else '',
                                    "Neonate additional info": f"{n.other_outcome_details}" if n.other_outcome_details else ''
                                }
                            }
                            #neonate_value['value'] = dict((k,v) for k,v in neonate_value['value'].items() if v)
                            i['controls'].append(neonate_value)
                #this section different from the rest
                elif i['key'] == 'additional':

                    content_type = ContentType.objects.get_for_model(report.report)
                    images = AttachedImage.objects.filter(content_type_id=content_type.id, object_id=report.id)
                    if images:
                        serializer = AttachedImageSerializer(images, many=True)
                        i['controls'][0]['images'] = serializer.data
                    else:
                        i['controls'][0]['value'] = {}
                        i['controls'][0]['images'] = []

                    if report.report.additional_info:
                        i['controls'][0]['value'] = {
                            "Other": report.report.additional_info
                        }
                #fill in data normally
                else:
                    i['controls'][0]['value'] = fill_fields(pk, i['controls'][0]['value'].items())

            #remove completely empty sections
            new_groups = []
            for i in d['groups']:
                if i['controls'][0]['value'] or (i['key'] == 'additional' and i['controls'][0]['images']):
                    new_groups.append(i)
            d['groups'] = new_groups
            #d['groups'] = [i for i in d['groups'] if i['controls'][0]['value'] or (i['key'] == 'additional' and i['controls'][0]['images'])]
        

        else:
            if d['key'] == 'key_review':
                fillInitialReview(pk, final_review)
            if d['key'] == 'anonymous':
                d['options'].clear()
                anonymous_value = {
                    "label": "I do not want to appear publicity as the author of this case",
                    "value": f"{report.anonymous}"
                }
                d['options'].append(anonymous_value)

    return final_review

def getReportAttribute(report, attribute):
    #Report is report id
    #attribute is the object path
    report = CureReport.objects.get(id=report)
    attr = attrgetter(attribute)
    try:
        value = attr(report)
    except Exception as e :
        value = ''
        pass

    if attribute == 'report.patient.comorbidity':
        value = ", ".join(c.value for c in report.report.patient.comorbidity.all())
    if attribute == 'report.organisms':
        value = ", ".join(c.name for c in report.report.organisms.all())
    if attribute == 'report.previous_drugs':
        value = ", ".join([d.name for d in report.report.previous_drugs.all()])
    if attribute == 'report.resistant_drugs':
        value = ", ".join([d.name for d in report.report.resistant_drugs.all()])
    if attribute == 'report.how_outcome':
        value = ", ".join([h for h in report.report.how_outcome])
    if attribute == 'report.sample':
        value = ", ".join([s for s in report.report.sample])
    if attribute == 'report.patient.races':
        value = ", ".join([r.value for r in report.report.patient.race])
    if attribute == 'report.site_of_tuberculosis_infection':
        value = ", ".join([r for r in report.report.site_of_tuberculosis_infection])
    if attribute == 'report.extrapulmonary_site':
        value = ", ". join(e.value for e in report.report.extrapulmonary_site)   
    if 'monkeypox' in attribute:
        value = report.report.extra_fields.get(attribute,None)
        if attribute == 'monkeypox_symptoms':
            if report.report.extra_fields.get('monkeypox_symptoms',None):
                value = ", ".join([r.get('value') for r in report.report.extra_fields.get('monkeypox_symptoms')])
    return value

def fill_fields(pk,items):
    new_values =dict()
    for k,v in items:
        value = getReportAttribute(pk,v)
        if value:
            new_values[k] = value
    
    return new_values


def get_page_index(page,menu):
    for i,item in enumerate(menu['menu']):
        if item['name'] == page:
            return int(i)

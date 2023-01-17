from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST
from server.apps.core.models import Drug,Report
from server.apps.ui_forms.cure_report_logic import *
from .constants import DEFAULT_PAGE, DRUG_PAGES, OUTCOME_TIMING_FOLLOW_UP, OUTCOME_TIMING_ADDITIONAL_Q
from copy import deepcopy

import json


@api_view(['GET'])
def get_pageinfo(request):
    # TODO: should check request.user??

    if request.method == 'GET':
        page = request.GET.get("page")
        menu = request.GET.get("menu")

    if request.method == 'POST':
        page = request.POST.get("page")
        menu = request.POST.get("menu")
    return_obj = {}
    data, menu_data = form_json_data(request, "case-report")
    if not page or page == "default":
        page = DEFAULT_PAGE
    return_obj["next-page"] = data[page]
    if page in DRUG_PAGES:
        index = request.GET.get("index")
        drug = request.GET.get("drug")
        changeKeyForAdditionalDrugPages(return_obj["next-page"], index, drug)

    report = request.GET.get("reportId")
    if report:
        changeMenu(menu_data,data,report,request)
    if page == 'neonates':
        neonate_page = return_obj["next-page"]
        report = request.GET.get("reportId")
        report_ = Report.objects.get(id=int(report))
        disease = report_.disease
        addDiseasetoNeonatePage(disease, neonate_page)
        
    if page == 'initial-overview':
        report = request.GET.get("reportId")
        fillInitialReview(report, return_obj['next-page'])

    if page == 'final-review':
        report = request.GET.get('reportId')
        return_obj['next-page'] = fillAttributes(request,report,return_obj['next-page'])

    if menu is not None:
        return_obj["menu"] = menu_data['menu']

    return Response(
        {"data": return_obj},
        status=HTTP_200_OK,
    )


def form_json_data(request, type_):
    """ Load json data from const files """
    files = [ "form", "menu", ]
    return_obj = []
    for file_ in files:
        if request.user.profile.qualification == 'Patient':
            key = f"patient-{type_}-{file_}"
        else:
            key = f"{type_}-{file_}"
        data = request.session.get(key)
        if not data:
            try:
                json_file = f"server/apps/ui_forms/json_data/{key}.json"
                json_data = open(json_file).read()
                data = json.loads(json_data)
                request.session[key] = data
            except Exception as e:
                return_obj.append("")

        return_obj.append(deepcopy(data))
    return return_obj

def addDiseasetoNeonatePage(disease, neonate_page):
    neonate_page['name']= "neonates&disease={}".format(disease.id)
    neonate_page['formControls'][0]['controls'][0]['label']=neonate_page['formControls'][0]['controls'][0]['label'].replace("${disease}",disease.name)
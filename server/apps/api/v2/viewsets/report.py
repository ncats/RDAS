from django.contrib.auth.models import User
from django.db.models import Count, Q, ExpressionWrapper, BooleanField
from django_filters import FilterSet
from django_filters.filters import CharFilter
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.viewsets import ModelViewSet
from rest_framework.response import Response
from rest_framework.decorators import action

from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.serializers.report import WritableCureReportSerializer, CureReportSerializer
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer
from rest_framework.permissions import SAFE_METHODS
from server.apps.api.v2.views import not_allowed_to_change_status

from server.apps.api.v2.constants import ALLOWED_SORT, LIMIT_NEWSFEEDS
from server.apps.api.models import LogRequest
from server.apps.core.constants import APPROVED, SAVED, SUBMITTED, OTHER, PATIENT_DIED, OUTCOME_CASES, MALE, FEMALE, NOT_SPECIFIED, PATIENT, PARENT, CAREGIVER
from server.apps.core.models import CureReport, Report, Regimen
from server.apps.ui_forms.cure_report_logic import get_pageinfo


class ReportOrderFilter(DjangoFilterBackend):
    def filter_queryset(self, request, queryset, view):
        queryset = super().filter_queryset(request, queryset, view)

        if request.query_params.get("origin", "") == "home":
            if request.user.profile.favorited_diseases.all():
                queryset = queryset.filter(report__disease__in=request.user.profile.favorited_diseases.all())

            queryset = queryset.order_by('-updated', '-created')
            return queryset

        sort = request.query_params.get("sort", None)
        sort = sort if sort else "latest"
        sort = sort if sort in ALLOWED_SORT else "latest"

        if sort == "latest":
            queryset = queryset.order_by('-updated', '-created')

        if sort == "most-viewed":
            path = "/reports/"
            length = len(path)
            lr = LogRequest.objects.filter(path__contains=path).values('path').annotate(count=Count('path')).order_by('-count')[:LIMIT_NEWSFEEDS]
            data = {}
            for item in lr:
                ndx = item["path"].index(path)
                object_id = item["path"][ ndx + length :-1]
                if object_id:
                    data[int(object_id)] = item["count"]
            queryset = sorted(queryset, key=lambda x: data[x.report_id] if x.report_id in data else 0, reverse=True)

        return queryset


class ReportFilter(FilterSet):
    disease = CharFilter(method='by_disease')
    drugs = CharFilter(method='by_drugs')
    search_drugs = CharFilter(method='by_drugs')
    report = CharFilter(method='by_report')
    therapy = CharFilter(method='by_therapy')
    diagnosis = CharFilter(method='by_diagnosis')
    outcome = CharFilter(method='by_outcome')
    outcome_computed = CharFilter(method='by_outcome_computed')
    adverse = CharFilter(method='by_adverse')
    sex = CharFilter(method='by_sex')
    age = CharFilter(method='by_age')
    country_contracted = CharFilter(method='by_country_contracted')
    country_treated = CharFilter(method='by_country_treated')
    year = CharFilter(method='by_year')
    morbidities = CharFilter(method='by_morbidities')
    organism = CharFilter(method='by_organism')
    resistance = CharFilter(method='by_resistance')
    origin = CharFilter(method='by_origin')

    class Meta:
        model = CureReport
        fields = '__all__'

    def by_origin(self, queryset, name, value):
        if isinstance(self.request.user, User):
            favorited_disease_ids = self.request.user.profile.favorited_diseases.all().values_list("id", flat=True)
            favorited_drug_ids = self.request.user.profile.favorited_drugs.all().values_list("id", flat=True)
            report_ids = Regimen.objects.filter(drug_id__in=favorited_drug_ids).values_list("report_id", flat=True)
            return queryset.annotate(
                    favorited=ExpressionWrapper(
                        Q(report__disease_id__in=favorited_disease_ids) | Q(report__in=report_ids),
                        output_field=BooleanField()
                    )
                    ).order_by("-favorited")
        return queryset

    def by_disease(self, queryset, name, value):
        return queryset.filter(report__disease_id=value)

    def by_drugs(self, queryset, name, value):
        drug_ids = value.split(',')
        return queryset.filter(report__drugs__in=drug_ids)

    def by_report(self, queryset, name, value):
        values = value.split(',')
        options = [0, 0, 0,]
        for item in values:
            if item == "Published Report":
                options[0] = 1
            elif item == "Clinician-Submitted Report":
                options[1] = 2
            elif item == "Patient-Submitted Report":
                options[2] = 4

        patient_qualifications = [PATIENT, CAREGIVER, PARENT]
        objects = queryset
        summed = sum(options)
        if summed == 1:
            objects = objects.filter(report__article__published=True)
        elif summed == 2:
            objects = objects.exclude(author__profile__qualification__in=patient_qualifications)
        elif summed == 3:
            objects = objects.filter(~Q(author__profile__qualification__in=patient_qualifications) | Q(report__article__published=True))
        elif summed == 4:
            objects = objects.filter(author__profile__qualification__in=patient_qualifications)
        elif summed == 5:
            objects = objects.filter(Q(author__profile__qualifications__in=patient_qualifications) | Q(report__article__published=True))

        return objects

    def by_therapy(self, queryset, name, value):
        values = value.lower().split(',')
        if "monotherapy" in values and "combination therapy" in values:
            return queryset

        if "monotherapy" in values:
            report_ids = Regimen.objects.values('report_id').annotate(count=Count('report_id')).filter(count=1).values_list('report_id', flat=True)

        if "combination therapy" in values:
            report_ids = Regimen.objects.values('report_id').annotate(count=Count('report_id')).filter(count__gte=2).values_list('report_id', flat=True)

        return queryset.filter(report_id__in=report_ids)

    def by_diagnosis(self, queryset, name, value):
        allowed_values = ["clinical assessment", "imaging", "pathology",
                "culture", "serology", "pcr", "smear", "other" ]
        values = value.lower().split(',')

        matches = filter(lambda x: x in allowed_values, values)
        # TODO: second or condition can happen when several allowed values are sent
        #   for ex. diagnosis=pcr,pcr,pcr...
        if not any(matches) or len(list(filter(lambda x: x, matches))) == len(allowed_values):
            return queryset

        return queryset.filter(report__how_diagnosis__contains=values)

    def by_outcome(self, queryset, name, value):
        values = value.split(',')
        return queryset.filter(report__outcome__in=values)

    def by_outcome_computed(self, queryset, name, value):
        computed_values = value.split(',')
        values = []
        for cvalue in computed_values:
           values += [i for i in OUTCOME_CASES if OUTCOME_CASES[i] == cvalue]

        if values and len(set(values)) != len(OUTCOME_CASES.keys()):
            return queryset.filter(report__outcome__in=values)

        return queryset

    def by_adverse(self, queryset, name, value):
        value = value.lower()
        if value not in ["yes", "no"]:
            return queryset

        condition = False
        if value == "yes":
            condition = True
        return queryset.filter(report__have_adverse_events=condition)

    def by_sex(self, queryset, name, value):
        value = value.lower()
        if value == "male":
            values = [ MALE ]
        elif value == "female":
            values = [ FEMALE ]
        elif value == "not specified":
            values = [ OTHER, NOT_SPECIFIED ]
        else:
            return queryset
        return queryset.filter(report__patient__sex__in=values)

    def by_age(self, queryset, name, value):
        value = value.lower()
        if value == "child":
            return queryset.filter(report__patient__age__lte=17)
        elif value == "adult":
            return queryset.filter(report__patient__age__gte=18, report__patient__age__lte=64)
        elif value == "older":
            return queryset.filter(report__patient__age__gte=65)
        else:
            return queryset

    def by_country_contracted(self, queryset, name, value):
        return queryset.filter(report__country_contracted=value)

    def by_country_treated(self, queryset, name, value):
        return queryset.filter(report__country_treated=value)

    def by_year(self, queryset, name, value):
        values = value.split(',')
        return queryset.filter(report__began_treatment_year__in=values)

    def by_morbidities(self, queryset, name, value):
        return queryset.filter(report__patient__comorbidity__icontains=value.lower())

    def by_organism(self, queryset, name, value):
        values = value.lower().split(',')
        return queryset.filter(report__organisms__in=values)

    def by_resistance(self, queryset, name, value):
        values = value.split(',')
        return queryset.filter(report__resistant_drugs__in=values)


class ReportViewSet(ModelViewSet):
    pagination_class = LimitOffsetPagination
    permission_classes = [BaseCUREIDAccessPermission,]
    queryset = CureReport.objects.filter(status=APPROVED)\
              .select_related('report')
    renderer_classes = [CustomRenderer, BrowsableAPIRenderer]
    serializer_class = WritableCureReportSerializer
    filter_class = ReportFilter
    filter_backends = (ReportOrderFilter,)

    def get_serializer_class(self):
        if self.request.method in SAFE_METHODS:
            return CureReportSerializer
        return WritableCureReportSerializer

    def get_queryset(self):
        if self.request.method not in SAFE_METHODS and isinstance(self.request.user, User):
            return CureReport.objects.filter(author=self.request.user)
        return self.queryset

    def retrieve(self, request, pk=None):
        viewer = request.user
        report = CureReport.objects.filter(pk=pk).select_related("report").first()
        if report:
            if report.status != APPROVED and viewer != report.author:
                return Response({
                    "detail":'Only author can view a report that is not Approved.'
                })

            if report and report.id:
                return Response(CureReportSerializer(report, context={'request': request}).data)
            else:
                self.queryset = CureReport.objects.none()
                return super(ReportViewSet, self).retrieve(request, pk)
        else:
            return Response({
                "detail":"Report does not exist"
            })

    def create(self, request):
        serializer = WritableCureReportSerializer(data=request.data["form"], context={'request': request})
        serializer.is_valid()
        new_report = serializer.save()
        pk = serializer.data.get('id')

        # try:
        page_info = get_pageinfo(request, pk)
        # except Exception as e:
        #     return Response(str(e))

        if serializer.is_valid():
            return Response({
                "data":serializer.data,
                "navigation": page_info
            },status=200)
        else:
            return Response({
                "detail": str(serializer.errors)}
                ,status=500
            )

    def partial_update(self, request, pk=None):
        currentPage = request.data["currentPage"].split("__")[0]
        nextPage = request.data["nextPage"].split("__")[0]

        report = CureReport.objects.filter(id=pk)[0]

        if request.user != report.author:
            return Response({
                "detail": "You do not have permission to edit this discussion."
            }, status=200)

        if not_allowed_to_change_status(request.user, report.status, request.data.get('status', None)):
            return Response({
                "detail": "You do not have permission to edit status."
            }, status=200)

        if 'anonymous' in request.data["form"]:
            if request.data['form']['anonymous']:
                request.data['form']['anonymous'] = True
            else:
                request.data['form']['anonymous'] = False

        if not_allowed_to_change_status(request.user, report.status, request.data.get('status', None)):
            return Response({
                "detail": "You do not have permission to edit status."
            }, status=200)

        serializer = WritableCureReportSerializer(report, data=request.data["form"], context={'request': request}, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()

        try:
            page_info = get_pageinfo(request, pk)
        except Exception as e:
            return Response(str(e))
        if serializer.is_valid():
            return Response({
                "data": serializer.data,
                "navigation": page_info
            },status=200)
        else:
            return Response({
                "detail": str(serializer.errors)
            },status=500)

  ### Needed this to allow for patch updates for only updating one field at a time instead of sending the whole report everytime
    def update(self,request,pk=None):
        return Response({
            "data": ""
        })

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        if request.user.id != instance.author.id:
            return Response({
                "detail": "You do not have permission to delete this."
            }, status=200)
        id = instance.id
        self.perform_destroy(instance)
        return Response({
            "detail": f"Report {id} deleted",
            "data": {"id": id}
        },status=200)

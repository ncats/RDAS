from django.db.models import Count
from django.utils.timezone import now
from django_filters import FilterSet
from django_filters.filters import CharFilter
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.viewsets import ModelViewSet

from server.apps.api.models import LogRequest
from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer
from server.apps.api.v2.serializers.newsfeed import NewsfeedWSSerializer
from server.apps.core.constants import OUTCOME_CASES, PATIENT_DIED, MALE, FEMALE, NOT_SPECIFIED
from server.apps.core.models import Newsfeed, Regimen, CureReport, Discussion, ClinicalTrial, Article

import datetime


allowed_models = ["cases", "discussions", "clinical-trials", "articles", "events"]
allowed_sort = ["latest", "most-viewed"]
LIMIT_NEWSFEEDS = 100


class NewsfeedOrderFilter(DjangoFilterBackend):
    def filter_queryset(self, request, queryset, view):
        queryset = super().filter_queryset(request, queryset, view)

        sort = request.query_params.get("sort", None)
        sort = sort if sort else "latest"
        sort = sort if sort in allowed_sort else "latest"

        if sort == "latest":
            queryset = sorted(queryset, key=lambda x: (x.content_object.updated, x.content_object.created), reverse=True)

        if sort == "most-viewed":
            model = request.query_params.get("model", None)
            if model and model in allowed_models:
                model = "reports" if model == "cases" else model
                path = f"/{model}/"
                length = len(path)
                lr = LogRequest.objects.filter(path__contains=path).values('path').annotate(count=Count('path')).order_by('-count')[:LIMIT_NEWSFEEDS]
                data = {}
                for item in lr:
                    ndx = item["path"].index(path)
                    object_id = item["path"][ ndx + length :-1]
                    data[int(object_id)] = item["count"]
                queryset = sorted(queryset, key=lambda x: data[x.object_id] if x.object_id in data else 0, reverse=True)

        return queryset


class NewsfeedFilter(FilterSet):
    model = CharFilter(method='by_model')
    drugs = CharFilter(method='by_drugs')
    report_type = CharFilter(method='by_report_type')
    therapy_type = CharFilter(method='by_therapy_type')
    diagnosis = CharFilter(method='by_diagnosis')
    outcome = CharFilter(method='by_outcome')
    adverse_events = CharFilter(method='by_adverse_events')
    sex = CharFilter(method='by_sex')
    age = CharFilter(method='by_age')
    country_contracted = CharFilter(method='by_country_contracted')
    country_treated = CharFilter(method='by_country_treated')
    year = CharFilter(method='by_year')
    comorbidity = CharFilter(method='by_comorbidity')
    organism = CharFilter(method='by_organism')
    resistance = CharFilter(method='by_resistance')
    date = CharFilter(method='by_date')
    author = CharFilter(method='by_author')
    status = CharFilter(method='by_status')
    phase = CharFilter(method='by_phase')
    country = CharFilter(method='by_country')
    study_type = CharFilter(method='by_study_type')
    sponsor_type = CharFilter(method='by_sponsor_type')
    sponsor = CharFilter(method='by_sponsor')

    class Meta:
        model = Newsfeed
        fields = "__all__"

    def by_model(self, queryset, name, value):
        if value not in allowed_models:
            return Newsfeed.objects.none()
        value = "curereport" if value == "cases" else value
        value = value[:-1] if value.endswith("s") else value

        query_set = queryset.filter(content_type__model=value)
        return query_set

    def by_drugs(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        drug_ids = value.split(',')
        report_ids = CureReport.objects.filter(report__drugs__in=drug_ids).values('id', flat=True)
        return queryset.filter(object_id__in=report_ids)

    def by_report_type(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        values = value.split(',')
        report_ids = CureReport.objects.filter(report__report_type__in=values).values('id', flat=True)
        return queryset.filter(object_id__in=report_ids)

    def by_therapy_type(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        values = value.split(',')
        if "monotherapy" in values and "combination" in values:
            return queryset

        if "monotherapy" in values:
            report_ids = Regimen.objects.values('report_id').annotate(count=Count('report_id')).filter(count=1).values('report_id', flat=True)

        if "combination" in values:
            report_ids = Regimen.objects.values('report_id').annotate(count=Count('report_id')).filter(count__gte=2).values('report_id', flat=True)

        curereport_ids = CureReport.objects.filter(report_id__in=report_ids)
        return queryset.filter(object_id__in=curereport_ids)

    def by_diagnosis(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        allowed_values = ["Clinical Assessment", "Imaging", "Pathology", "Culture",
                "Serology", "PCR", "Smear", "Other" ]
        values = value.split(',')

        # TODO: or values contains all allowed_values
        if not filter(lambda x: x in allowed_values, values):
            return queryset

        report_ids = CureReport.objects.filter(report__how_diagnosis__contains=values)
        return queryset.filter(object_id__in=report_ids)

    def by_outcome(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        allowed_values = ["Improved", "Undetermined", "Died", "Other"]
        values = value.split(',')

        # TODO: or values contains all allowed_values
        if not filter(lambda x: x in allowed_values, values):
            return queryset

        outcome_values = []
        for key in OUTCOME_CASES:
            if OUTCOME_CASES[key] in values:
                outcome_values.append(key)
        if "Died" in values:
            outcome_values.append(PATIENT_DIED)

        report_ids = CureReport.objects.filter(report__outcome__in=outcome_values)
        return queryset.filter(object_id__in=report_ids)

    def by_adverse_events(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        if value == "yes":
            report_ids = CureReport.objects.filter(report__have_adverse_events=True)
        elif value == "no":
            report_ids = CureReport.objects.filter(report__have_adverse_events=False)
        else:
            return queryset
        return queryset.filter(object_id__in=report_ids)

    def by_sex(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        if value == "Male":
            report_ids = CureReport.objects.filter(report__patient__sex=MALE)
        elif value == "Female":
            report_ids = CureReport.objects.filter(report__patient__sex=FEMALE)
        elif value == "Not specified":
            report_ids = CureReport.objects.filter(report__patient__sex__in=[OTHER, NOT_SPECIFIED])
        else:
            return queryset
        return queryset.filter(object_id__in=report_ids)

    def by_age(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        # TODO: our age constants don't end at 17
        if value == "Child (birth-17)":
            report_ids = CureReport.objects.filter(report__patient__age__lte=17)
        elif value == "Adult (18-64)":
            report_ids = CureReport.objects.filter(report__patient__age__gte=18, report__patient__age__lte=64)
        elif value == "Older Adult (65+)":
            report_ids = CureReport.objects.filter(report__patient__age__gte=65)
        else:
            return queryset

        return queryset.filter(object_id__in=report_ids)

    def by_country_contracted(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        values = value.split(',')
        report_ids = CureReport.objects.filter(report__country_contracted__in=values)
        return queryset.filter(object_id__in=report_ids)

    def by_country_treated(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        values = value.split(',')
        report_ids = CureReport.objects.filter(report__country_treated__in=values)
        return queryset.filter(object_id__in=report_ids)

    def by_year(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model not in ["cases", "clinical-trials"]:
            return queryset

        if model == "cases":
            report_ids = CureReport.objects.filter(report__began_treatment_year=value)
            return queryset.filter(object_id__in=report_ids)

        if model == "clinical-trials":
            values = value.split(',')
            cts = ClinicalTrial.objects.all()
            if values[0]:
                cts = cts.filter(start_year__gte=values[0])
            if values[1]:
                cts = cts.filter(start_year__lte=values[1])
            cts = cts.values('id', flat=True)
            return queryset.filter(object_id__in=cts)

    def by_comorbidity(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        report_ids = CureReport.objects.filter(report__patient__comorbidity__icontains=value.lower()).values('id', flat=True)
        return queryset.filter(object_id__in=report_ids)

    def by_organism(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        values = value.split(',')
        report_ids = CureReport.objects.filter(report__organisms__in=values).values('id', flat=True)
        return queryset.filter(object_id__in=report_ids)

    def by_resistance(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "cases":
            return queryset

        values = value.split(',')
        report_ids = CureReport.objects.filter(report__resistant_drugs__in=values).values('id', flat=True)
        return queryset.filter(object_id__in=report_ids)

    def by_date(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model not in ["discussions", "articles"]:
            return queryset

        if value == "Last 30 days":
            start_date = now() - datetime.timedelta(30)
            days = 30
        elif value == "Last 60 days":
            days = 60
        elif value == "Last 90 days":
            days = 90
        elif value == "Last 6 months":
            days = 120
        else:
            return queryset

        start_date = now() - datetime.timedelta(days)
        if model == "discussions":
            ids = Discussion.objects.filter(created__gte=start_date).values('id', flat=True)
        elif model == "articles":
            ids = Article.objects.filter(created__gte=start_date).values('id', flat=True)
        return queryset.filter(object_id__in=ids)

    def by_author(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model not in ["discussions", "article"]:
            return queryset

        values = value.split(' ')
        if model == "discussions":
            ids = Discussion.objects.filter(author__first_name=values[0], author__last_name=values[1]).values('id', flat=True)
        elif model == "articles":
            ids = Article.objects.filter(author__first_name=values[0], author__last_name=values[1]).values('id', flat=True)
        return queryset.filter(object_id__in=ids)

    def by_status(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "clinical-trials":
            return queryset

        # TODO: allowed_values??
        values = value.split(',')
        ct_ids = ClinicalTrial.objects.filter(status__in=values).values('id', flat=True)
        return queryset.filter(object_id__in=ct_ids)

    def by_phase(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "clinical-trials":
            return queryset

        values = value.split(',')
        ct_ids = ClinicalTrial.objects.filter(phase__in=values).values('id', flat=True)
        return queryset.filter(object_id__in=ct_ids)

    def by_country(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "clinical-trials":
            return queryset

        values = value.split(',')
        ct_ids = ClinicalTrial.objects.filter(country__in=values).values('id', flat=True)
        return queryset.filter(object_id__in=ct_ids)

    def by_study_type(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "clinical-trials":
            return queryset

        values = value.split(',')
        ct_ids = ClinicalTrial.objects.filter(study_type__in=values).values('id', flat=True)
        return queryset.filter(object_id__in=ct_ids)

    def by_sponsor_type(self, queryset, name, value):
        return queryset

    def by_sponsor(self, queryset, name, value):
        model = self.request.query_params.get("model", None)
        if not model and model != "clinical-trials":
            return queryset

        values = value.split(',')
        ct_ids = ClinicalTrial.objects.filter(sponsor__in=values).values('id', flat=True)
        return queryset.filter(object_id__in=ct_ids)



class NewsfeedViewSet(ModelViewSet):
    pagination_class = LimitOffsetPagination
    permission_classes = [BaseCUREIDAccessPermission,]
    queryset = Newsfeed.objects.all().prefetch_related('content_object', "content_type")
    renderer_classes = [CustomRenderer, BrowsableAPIRenderer]
    filter_class = NewsfeedFilter
    filter_backends = (NewsfeedOrderFilter,)
    serializer_class = NewsfeedWSSerializer

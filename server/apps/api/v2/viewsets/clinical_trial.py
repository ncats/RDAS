from django.contrib.auth.models import User
from django.db.models import Q, Count, BooleanField, Case, When, Value
from django_filters import FilterSet, BaseInFilter
from django_filters.filters import CharFilter
from django_filters.rest_framework import DjangoFilterBackend
from functools import reduce
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import SAFE_METHODS
from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet
from rest_framework.status import HTTP_200_OK
from django.shortcuts import get_object_or_404

from server.apps.api.v2.constants import ALLOWED_SORT, LIMIT_NEWSFEEDS
from server.apps.api.models import LogRequest
from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.serializers.clinical_trial import WritableClinicalTrialSerializer, ClinicalTrialSerializer, ClinicalTrialDataVisiualizationSerializer
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer
from server.apps.api.v2.views import not_allowed_to_change_status
from server.apps.core.constants import APPROVED
from server.apps.core.models import ClinicalTrial

import operator


class ClinicalTrialOrderFilter(DjangoFilterBackend):
    def filter_queryset(self, request, queryset, view):
        queryset = super().filter_queryset(request, queryset, view)

        if request.query_params.get("origin", "") == "home":
            return queryset

        sort = request.query_params.get("sort", None)
        sort = sort if sort else "latest"
        sort = sort if sort in ALLOWED_SORT else "latest"

        if sort == "latest":
            queryset = sorted(queryset, key=lambda x: (x.updated, x.created), reverse=True)

        if sort == "most-viewed":
            path = "/clinical-trials/"
            length = len(path)
            lr = LogRequest.objects.filter(path__contains=path).values('path').annotate(count=Count('path')).order_by('-count')[:LIMIT_NEWSFEEDS]
            data = {}
            for item in lr:
                ndx = item["path"].index(path)
                object_id = item["path"][ ndx + length :-1]
                if object_id:
                    data[int(object_id)] = item["count"]
            queryset = sorted(queryset, key=lambda x: data[x.id] if x.id in data else 0, reverse=True)

        return queryset


class CharInFilter(BaseInFilter, CharFilter):
    pass


class ClinicalTrialFilter(FilterSet):
    status = CharFilter(method='by_status')
    phase = CharFilter(method='by_phase')
    year_from = CharFilter(method='by_year_from')
    year_to = CharFilter(method='by_year_to')
    country = CharFilter(method='by_country')
    study_type = CharFilter(method='by_study_type')
    sponsor_type = CharFilter(method='by_sponsor_type')
    sponsor = CharFilter(method='by_sponsor')
    drugs = CharInFilter(lookup_expr='icontains')
    origin = CharFilter(method='by_origin')

    class Meta:
        model = ClinicalTrial
        fields = '__all__'

    def by_origin(self, queryset, name, value):
        if isinstance(self.request.user, User):
            favorited_disease_ids = self.request.user.profile.favorited_diseases.all().values_list("id", flat=True)
            return queryset.annotate(
                    favorited=Case(
                        When(disease_id__in=favorited_disease_ids, then=Value(True)),
                        default=Value(False),
                        output_field=BooleanField()
                    )
                ).order_by("-favorited", "-created")
        return queryset

    def by_status(self, queryset, name, value):
        values = value.split(',')
        # We have a valid value "Active, not recruiting", which breaks the split
        if 'Active' in values and ' not recruiting' in values:
            values.remove('Active')
            values.remove(' not recruiting')
            values.append('Active, not recruiting')
        return queryset.filter(ct_status__in=values)

    def by_year_from(self, queryset, name, value):
        return queryset.filter(start_year__gte=value)

    def by_year_to(self, queryset, name, value):
        return queryset.filter(start_year__lte=value)

    def by_phase(self, queryset, name, value):
        values = value.split(',')
        return queryset.filter(phase__in=values)

    def by_country(self, queryset, name, value):
        values = value.split(',')
        return queryset.filter(country__in=values)

    def by_study_type(self, queryset, name, value):
        values = value.split(',')
        return queryset.filter(study_type__in=values)

    def by_sponsor_type(self, queryset, name, value):
        return queryset

    def by_sponsor(self, queryset, name, value):
        values = value.split(',')
        return queryset.filter(reduce(operator.or_, (Q(sponsor__contains=x) for x in values)))


class ClinicalTrialViewSet(ModelViewSet):
  pagination_class = LimitOffsetPagination
  permission_classes = [BaseCUREIDAccessPermission,]
  queryset = ClinicalTrial.objects.filter(status=APPROVED)
  serializer_class = ClinicalTrialSerializer
  renderer_classes = [CustomRenderer, BrowsableAPIRenderer]
  filter_class = ClinicalTrialFilter
  filter_backends = (ClinicalTrialOrderFilter,)

  def get_queryset(self):
      disease = self.request.query_params.get('disease', None)
      ct_id = self.request.query_params.get('nctId', None)
    
      if disease is not None:
          self.queryset = self.queryset.filter(disease_id=int(disease))

      if ct_id:
          self.queryset = self.queryset.filter(clinical_trials_gov_id=ct_id)
          # TODO: no check for disease above
          if not self.queryset:
            res = ValidationError({"data":[], "detail": "No clinical trial with that ct gov id"})
            res.status_code=200
            raise res
      print(self.queryset)
    #   # TODO: remove in PROD
    #   return self.queryset.exclude(Q(sponsor="") | Q(drugs=[]))
      return self.queryset

  def get_serializer_class(self):
      minimal = self.request.query_params.get('minimal', None)
      if self.request.method in SAFE_METHODS:
          return ClinicalTrialSerializer
      if self.request.method in SAFE_METHODS and minimal is not None:
          return ClinicalTrialDataVisiualizationSerializer
      return WritableClinicalTrialSerializer

  def create(self, request):
    try:
        serializer = WritableClinicalTrialSerializer(data=request.data, context={'request': request})
        serializer.is_valid(raise_exception=True)
        serializer.save()
        if serializer.is_valid():
            return Response({
                "data":serializer.data,
            },status=200)
        else:
            return Response({
                "detail":str(serializer.errors)
            },status=400)
    except Exception as e:
        return Response({
            "detail":str(e)
        },status=500)

  # retrieve doesn't work without it but works in other viewsets like article for example
  def retrieve(self, request, pk=None):
      queryset = self.queryset
      ct = get_object_or_404(queryset, pk=pk)
      serializer = self.serializer_class(ct)
      return Response(serializer.data)

  def partial_update(self, request, pk=None):
      return self.update(request, pk)

  def update(self, request, pk=None):
    try:
        ct = ClinicalTrial.objects.filter(pk=pk)[0]
        if request.user != ct.author:
            return Response({
                "detail": "You do not have permission to edit this clinical trial."
            }, status=200)

        if not_allowed_to_change_status(request.user, ct.status, request.data.get('status', None)):
            return Response({
                "detail": "You do not have permission to edit status."
            }, status=200)

        serializer = WritableClinicalTrialSerializer(ct, data=request.data, context={'request': request}, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        if serializer.is_valid():
            return Response({
                "data":serializer.data,
            },status=200)
        else:
            return Response(
                {"detail":str(serializer.errors)},
                status=400
            )
    except Exception as e:
        return Response({
            "detail":str(e)
        },status=500)

  def destroy(self, request, *args, **kwargs):
    instance = self.get_object()
    if request.user != instance.author:
        return Response({
            "detail": "You do not have permission to delete this clinical trial."
        }, status=200)
    instance.deleted = True
    instance.save()
    return Response({
        "detail": "Clinical Trial deleted.",
        "data": {"id": instance.id}
    }, status=200)

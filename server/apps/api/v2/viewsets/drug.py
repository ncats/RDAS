from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.viewsets import ModelViewSet
from rest_framework.permissions import SAFE_METHODS
from django.db.models import (
    Count,
    F,
    Q,
    Value,
    Case,
    When,
    BooleanField
)
from django.db.models.functions import StrIndex, Lower
from django_filters import FilterSet
from django_filters.filters import CharFilter

from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.serializers.drug import DrugSerializer, WritableDrugSerializer
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer

from server.apps.core.constants import APPROVED
from server.apps.core.models import Drug, Report, CureReport, Disease, Regimen


class DrugFilter(FilterSet):
    disease = CharFilter(method='disease_drugs')

    class Meta:
        model = Drug
        fields = ["id", "name"]

    def disease_drugs(self, queryset, name, value):
        reports = CureReport.objects.filter(status=APPROVED, report__disease_id=value).values('report_id')
        fda_approved_drugs = Disease.objects.filter(pk=value).values_list('fda_approved_drugs', flat=True)
        queryset = (
            Regimen.objects.filter(report_id__in=reports)
                .select_related('drug')
                .values('drug_id','drug__name')
                .annotate(total=Count('drug_id'))
                .annotate(id=F('drug_id'))
                .annotate(name=F('drug__name'))
                .annotate(
                    fda_approved=Case(
                        When(drug_id__in=fda_approved_drugs, then=Value(True)),
                        default=Value(False),
                        output_field=BooleanField()
                    )
                )
                .order_by('-total', 'drug__name')
        )
        return queryset


class DrugViewSet(ModelViewSet):
  pagination_class = LimitOffsetPagination
  permission_classes = [BaseCUREIDAccessPermission,]
  queryset = Drug.objects.all()
  renderer_classes = [CustomRenderer, BrowsableAPIRenderer]
  filter_class = DrugFilter

  def get_serializer_class(self):
      if self.request.method in SAFE_METHODS:
          return DrugSerializer
      else:
          return WritableDrugSerializer

  def get_queryset(self):
      typeahead = self.request.query_params.get("typeahead", None)
      if "pk" in self.request.parser_context["kwargs"]:
          results = self.queryset.filter(
              id=self.request.parser_context["kwargs"]["pk"]
          )
      elif typeahead:
          typeahead = typeahead.lower()
          results = (
              self.queryset.filter(
                  Q(name__istartswith=typeahead) | Q(name__icontains=typeahead)
              )
              .distinct()
              .annotate(index=StrIndex(Lower("name"), Value(typeahead)))
              .order_by("index", "name")
          )
      else:
          results = self.queryset.order_by("name")
      return results

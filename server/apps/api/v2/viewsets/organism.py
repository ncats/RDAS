from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.viewsets import ModelViewSet
from rest_framework.permissions import SAFE_METHODS
from django.db.models.functions import StrIndex, Lower
from django.db.models import (
    Q,
    Value
)

from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.serializers.organism import OrganismSerializer
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer

from server.apps.core.models import Organism


class OrganismViewSet(ModelViewSet):
  pagination_class = LimitOffsetPagination
  permission_classes = [BaseCUREIDAccessPermission,]
  queryset = Organism.objects.all()
  renderer_classes = [CustomRenderer, BrowsableAPIRenderer] 
  serializer_class = OrganismSerializer

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

from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.viewsets import ModelViewSet
from rest_framework.permissions import SAFE_METHODS
from django.contrib.auth.models import User
from django.db.models.functions import StrIndex, Lower
from django.db.models import (
    Q,
    F,
    Value,
    ExpressionWrapper,
    BooleanField,
)

from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.serializers.disease import DiseaseSerializer, MinimalDiseaseSerializer
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer

from server.apps.core.models import Disease


class DiseaseViewSet(ModelViewSet):
    pagination_class = LimitOffsetPagination
    permission_classes = [BaseCUREIDAccessPermission,]
    queryset = Disease.objects.all()
    #queryset = DiseaseDocument.search().all()
    renderer_classes = [CustomRenderer, BrowsableAPIRenderer]

    def get_serializer_class(self):
        type_ = self.request.query_params.get('minimal', None)
        if self.request.method in SAFE_METHODS and type_ is not None:
            return MinimalDiseaseSerializer
        return DiseaseSerializer

    def get_queryset(self):
        typeahead = self.request.query_params.get("typeahead", None)
        origin = self.request.query_params.get("origin", None)

        if origin == "home":
            favorited_ids = []
            if isinstance(self.request.user, User):
                favorited_ids = self.request.user.profile.favorited_diseases.all().values_list("id", flat=True)
            return self.queryset.annotate(
                favorited=ExpressionWrapper(
                    Q(id__in=favorited_ids),
                    output_field=BooleanField()
                )
            ).order_by("-favorited", "name")

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

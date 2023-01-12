from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.viewsets import ModelViewSet
from rest_framework.permissions import SAFE_METHODS

from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.serializers.photo_credit import PhotoCreditSerializer
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer
from server.apps.core.models import PhotoCredit


class PhotoCreditViewSet(ModelViewSet):
  pagination_class = LimitOffsetPagination
  permission_classes = [BaseCUREIDAccessPermission,]
  queryset = PhotoCredit.objects.all()
  renderer_classes = [CustomRenderer, BrowsableAPIRenderer]
  serializer_class = PhotoCreditSerializer

      
  def get_queryset(self):
      return self.queryset
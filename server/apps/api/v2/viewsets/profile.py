from server.apps.api.v2.serializers.profile import (
    EditableProfileSerializer,
    ProfileSerializer
)
from rest_framework.permissions import SAFE_METHODS
from rest_framework.renderers import BrowsableAPIRenderer

from server.apps.core.models import Profile
from rest_framework.viewsets import ModelViewSet

from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer
from rest_framework.response import Response

class ProfileViewSet(ModelViewSet):
    pagination_class = LimitOffsetPagination
    permission_classes = [BaseCUREIDAccessPermission,]
    renderer_classes = [CustomRenderer, BrowsableAPIRenderer]

    def get_queryset(self):
        if not self.request.user.is_authenticated:
            # Profile data is not accessible for unauthenticated users
            return Profile.objects.none()
        else:
            # Need .filter here (vs .get) for pagination to work
            return Profile.objects.filter(user=self.request.user)

    def get_serializer_class(self):
        if self.request.method in SAFE_METHODS:
            return ProfileSerializer
        else:
            return EditableProfileSerializer

    def retrieve(self, request, pk=None):
        # No idea why, but self.get_queryset is not called here
        #   and self.queryset is None. So'm calling it explicitly
        self.queryset = self.get_queryset()
        try:
            if self.queryset.filter(pk=pk).count() > 0:
                profile = self.queryset.filter(pk=pk)[0]
            else:
                profile = None
            serializer = ProfileSerializer(profile)
            return Response({
                "data":serializer.data,
            })
        except Exception as e:
            return Response({
                "detail": str(e)
            })

    def __partial_update(self, request, pk=None):
        # TODO: check later whether the below code is necessary
        self.queryset = self.get_queryset()
        profile = self.queryset.filter(pk=pk)[0]
        if "username" in request.data:
            return Response({
                "message":"You cannot change username."
            })
        serializer = EditableProfileSerializer(profile, data=request.data, context={'request': request}, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        if serializer.is_valid():
            return Response({
                "data":serializer.data,
            })
        else:
            return Response(
               serializer.errors
            )

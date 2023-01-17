from django.apps import apps
from rest_framework import serializers
from server.apps.core.constants import ATTACHED_IMAGE_FILLER, PROFILE_IMAGE_FILLER
from server.apps.core.models import AttachedImage

class AttachedImageSerializer(serializers.ModelSerializer):
    url = serializers.SerializerMethodField()

    class Meta:
        model = AttachedImage
        fields = ('id', 'url', 'caption')

    def get_url(self, obj):
        if obj.url in [ATTACHED_IMAGE_FILLER, PROFILE_IMAGE_FILLER]:
            return f"assets/{obj.url}"
        return f"{obj.url}"


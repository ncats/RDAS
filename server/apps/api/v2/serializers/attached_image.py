from django.conf import settings
from rest_framework import serializers

from server.apps.core.models import AttachedImage


class AttachedImageSerializer(serializers.ModelSerializer):
    url = serializers.SerializerMethodField()

    class Meta:
        model = AttachedImage
        fields = [ 'id', 'url', 'caption']

    def get_url(self, instance):
        path = settings.ATTACHED_IMAGE_S3_CONTENT_BUCKET if instance.url == instance.real_name else "assets/"
        return f"{path}{instance.url}"
